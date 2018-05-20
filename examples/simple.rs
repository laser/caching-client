extern crate caching_client;
extern crate failure;
extern crate reqwest;

use std::{thread, time::Duration};

pub fn main() -> Result<(), failure::Error> {
    let client =
        caching_client::CachingClient::new("simple.rocks", Some(Duration::from_secs(1)), None)?;
    let req = reqwest::Request::new(reqwest::Method::Get, "https://www.google.com".parse()?);
    let _ = client.send(req)?;

    let req2 = reqwest::Request::new(reqwest::Method::Get, "https://www.google.com".parse()?);
    let _ = client.send(req2)?;

    thread::sleep(Duration::from_millis(1000));

    let req3 = reqwest::Request::new(reqwest::Method::Get, "https://www.google.com".parse()?);
    let _ = client.send(req3)?;

    Ok(())
}
