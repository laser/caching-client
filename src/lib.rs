extern crate chrono;
extern crate failure;
extern crate reqwest;
extern crate rocksdb;
extern crate serde;
extern crate serde_cbor;
#[macro_use]
extern crate serde_derive;

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
}

// TODO
// * logging
// * threads

#[derive(Serialize, Deserialize)]
struct CachedValue {
    expires: Option<DateTime<Utc>>,
    value: Vec<u8>,
}

impl CachingClient {
    pub fn new(db_path: &str, cache_duration: Option<Duration>) -> Result<Self, f::Error> {
        let rocksdb = rocksdb::DB::open_default(db_path).context("opening rocksdb")?;
        let cache_duration: Option<chrono::Duration> = match cache_duration {
            Some(duration) => Some(chrono::Duration::from_std(duration)?),
            None => None,
        };

        Ok(CachingClient {
            rocksdb,
            http_client: reqwest::Client::new(),
            cache_duration,
        })
    }

    fn exec(&self, request: Request) -> Result<Vec<u8>, f::Error> {
        let mut response = self.http_client.execute(request)?;
        let mut bytes = Vec::with_capacity(4096);
        response.read_to_end(&mut bytes)?;
        bytes.shrink_to_fit();

        Ok(bytes)
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
                        Ok(BufReader::new(Cursor::new(cached.value.to_vec())))
                    }
                    _ => {
                        let bytes = self.exec(request)?;
                        let bytes = self.store(k, bytes)?;

                        Ok(BufReader::new(Cursor::new(bytes)))
                    }
                }
            }
            None => {
                let bytes = self.exec(request)?;
                let bytes = self.store(k, bytes)?;

                Ok(BufReader::new(Cursor::new(bytes)))
            }
        }
    }
}

impl AsRef<reqwest::Client> for CachingClient {
    fn as_ref(&self) -> &reqwest::Client {
        &self.http_client
    }
}
