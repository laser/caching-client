extern crate chrono;
#[macro_use]
extern crate failure;
extern crate reqwest;
extern crate rocksdb;
extern crate serde;
extern crate serde_cbor;
#[macro_use]
extern crate serde_derive;
#[macro_use]
pub extern crate slog;

use chrono::{offset::Utc, DateTime};
use failure as f;
use failure::ResultExt;
use reqwest::Request;
use std::io::{BufReader, Cursor, Read};
use std::time::Duration;

pub struct CachingClient {
    rocksdb: rocksdb::DB,
    http_client: reqwest::Client,
    cache_duration: Option<chrono::Duration>,
    log: slog::Logger,
}

// TODO
// * logging
// * threads

#[derive(Serialize, Deserialize)]
struct CachedValue {
    expires: Option<DateTime<Utc>>,
    value: Vec<u8>,
}

type HttpStatus = u16;

impl CachingClient {
    pub fn new(
        db_path: &str,
        cache_duration: Option<Duration>,
        logger: Option<slog::Logger>,
    ) -> Result<Self, f::Error> {
        let rocksdb = rocksdb::DB::open_default(db_path).context("opening rocksdb")?;
        let cache_duration: Option<chrono::Duration> = match cache_duration {
            Some(duration) => Some(chrono::Duration::from_std(duration)?),
            None => None,
        };
        let log = logger.unwrap_or_else(|| slog::Logger::root(slog::Discard, o!()));

        Ok(CachingClient {
            rocksdb,
            http_client: reqwest::Client::new(),
            cache_duration,
            log,
        })
    }

    fn exec(&self, request: Request) -> Result<(HttpStatus, Vec<u8>), f::Error> {
        let mut response = self.http_client.execute(request)?;
        let mut bytes = Vec::with_capacity(4096);
        response.read_to_end(&mut bytes)?;
        bytes.shrink_to_fit();

        Ok((response.status().as_u16(), bytes))
    }

    fn store(&self, k: &[u8], value: Vec<u8>) -> Result<Vec<u8>, f::Error> {
        let expires = self.cache_duration.map(|duration| Utc::now() + duration);
        let cached_val = CachedValue { expires, value };

        self.rocksdb.put(k, &serde_cbor::to_vec(&cached_val)?)?;

        Ok(cached_val.value)
    }

    pub fn send(&self, request: Request) -> Result<BufReader<Cursor<Vec<u8>>>, f::Error> {
        let uri = request.url().as_str().to_owned();
        let k = uri.as_bytes();
        match self.rocksdb.get(k)? {
            Some(bytes) => {
                let bytes: &[u8] = &bytes;
                let cached: CachedValue = serde_cbor::from_reader(bytes)?;

                match cached.expires {
                    Some(expires) if expires > Utc::now() => {
                        trace!(self.log, "reading from cache"; "uri" => &uri);
                        Ok(BufReader::new(Cursor::new(cached.value.to_vec())))
                    }
                    _ => {
                        trace!(self.log, "expired cache entry, refetching"; "uri" => &uri);
                        let (status, bytes) = self.exec(request)?;
                        let retriable_error = status >= 500;
                        if !retriable_error {
                            let bytes = self.store(k, bytes)?;
                            Ok(BufReader::new(Cursor::new(bytes)))
                        } else {
                            Err(format_err!("http status {}", status))
                        }
                    }
                }
            }
            None => {
                trace!(self.log, "no entry found"; "uri" => &uri);
                let (status, bytes) = self.exec(request)?;
                let retriable_error = status >= 500;
                if !retriable_error {
                    let bytes = self.store(k, bytes)?;
                    Ok(BufReader::new(Cursor::new(bytes)))
                } else {
                    Err(format_err!("http status {}", status))
                }
            }
        }
    }
}
