//! Initilize S3 handler to manipulate objects and buckets
//! ```
//! let config = s3handler::CredentialConfig{
//!     host: "s3.us-east-1.amazonaws.com".to_string(),
//!     access_key: "akey".to_string(),
//!     secret_key: "skey".to_string(),
//!     user: None,
//!     region: None, // default is us-east-1
//!     s3_type: None, // default will try to config as AWS S3 handler
//! };
//! let handler = s3handler::Handler::init_from_config(&config);
//! let _ = handler.ls();
//! ```
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
extern crate colored;

use std::fs::{metadata, write, File};
use std::io::prelude::*;
use std::path::Path;
use std::str::FromStr;

use chrono::prelude::*;
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::Regex;
use reqwest::{header, Client, Response};
use serde_json;

mod aws;

static S3_FORMAT: &'static str =
    r#"[sS]3://(?P<bucket>[A-Za-z0-9\-\._]+)(?P<object>[A-Za-z0-9\-\._/]*)"#;
static RESPONSE_FORMAT: &'static str = r#""Contents":\["([A-Za-z0-9\-\._]+?)"(.*?)\]"#;

/// # The struct for credential config for each S3 cluster
/// - host is a parameter for the server you want to link
///     - it can be s3.us-east-1.amazonaws.com or a ip, ex 10.1.1.100, for a ceph node
/// - user name is not required, because it only show in the prompt of shell
/// - access_key and secret_key are keys to connect to the cluster providing S3
/// - region is a paramter for the S3 cluster location
///     - if region is not specified, it will take default value us-east-1
/// - s3 type is a shortcut to set up auth type, format, url style for aws or ceph
///     - if s3_type is not specified, it will take aws as default value, aws
#[derive(Debug, Clone, Deserialize)]
pub struct CredentialConfig {
    pub host: String,
    pub user: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    pub region: Option<String>,
    pub s3_type: Option<String>,
}

/// # The signature type of Authentication
/// AWS2, AWS4 represent for AWS signature v2 and AWS signature v4
/// The v2 and v4 signature are both supported by CEPH.
/// Generally, AWS support v4 signature, and only limited support v2 signature.
/// following AWS region support v2 signature before June 24, 2019
/// - US East (N. Virginia) Region
/// - US West (N. California) Region
/// - US West (Oregon) Region
/// - EU (Ireland) Region
/// - Asia Pacific (Tokyo) Region
/// - Asia Pacific (Singapore) Region
/// - Asia Pacific (Sydney) Region
/// - South America (So Paulo) Region
pub enum AuthType {
    AWS4,
    AWS2,
}

/// # The response format
/// AWS only support XML format (default)
/// CEPH support JSON and XML
pub enum Format {
    JSON,
    XML,
}

/// # The request URL style
///
/// CEPH support JSON and XML
pub enum UrlStyle {
    PATH,
    HOST,
}

/// # The struct for generate the request
/// - host is a parameter for the server you want to link
///     - it can be s3.us-east-1.amazonaws.com or a ip, ex 10.1.1.100, for a ceph node
/// - access_key and secret_key are keys to connect to the cluster providing S3
/// - auth_type specify the signature version of S3
/// - format specify the s3 response from server
/// - url_style specify the s3 request url style
/// - region is a paramter for the S3 cluster location
///     - if region is not specified, it will take default value us-east-1
/// It can be init from the config structure, for example:
/// ```
/// let config = s3handler::CredentialConfig{
///     host: "s3.us-east-1.amazonaws.com".to_string(),
///     access_key: "akey".to_string(),
///     secret_key: "skey".to_string(),
///     user: None,
///     region: None, // default is us-east-1
///     s3_type: None, // default will try to config as AWS S3 handler
/// };
/// let handler = s3handler::Handler::init_from_config(&config);
/// ```
pub struct Handler<'a> {
    pub host: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub auth_type: AuthType,
    pub format: Format,
    pub url_style: UrlStyle,
    pub region: Option<String>,
}

trait ResponseHandler {
    fn handle_response(&mut self) -> (Vec<u8>, reqwest::header::HeaderMap);
}

impl ResponseHandler for Response {
    fn handle_response(&mut self) -> (Vec<u8>, reqwest::header::HeaderMap) {
        let mut body = Vec::new();
        let _ = self.read_to_end(&mut body);
        if self.status().is_success() || self.status().is_redirection() {
            info!("Status: {}", self.status());
            info!("Headers:\n{:?}", self.headers());
            info!(
                "Body:\n{}\n\n",
                std::str::from_utf8(&body).expect("Body can not decode as UTF8")
            );
        } else {
            error!("Status: {}", self.status());
            error!("Headers:\n{:?}", self.headers());
            error!(
                "Body:\n{}\n\n",
                std::str::from_utf8(&body).expect("Body can not decode as UTF8")
            );
        }
        (body, self.headers().clone())
    }
}

impl<'a> Handler<'a> {
    fn aws_v2_request(
        &self,
        method: &str,
        uri: &str,
        qs: &Vec<(&str, &str)>,
        insert_headers: &Vec<(&str, &str)>,
        payload: &Vec<u8>,
    ) -> Result<(Vec<u8>, reqwest::header::HeaderMap), &'static str> {
        let utc: DateTime<Utc> = Utc::now();
        let mut headers = header::HeaderMap::new();
        let time_str = utc.to_rfc2822();
        headers.insert("date", time_str.clone().parse().unwrap());

        // NOTE: ceph has bug using x-amz-date
        let mut signed_headers = vec![("Date", time_str.as_str())];
        let insert_headers_name: Vec<String> = insert_headers
            .into_iter()
            .map(|x| x.0.to_string())
            .collect();

        // Support AWS delete marker feature
        if insert_headers_name.contains(&"delete-marker".to_string()) {
            for h in insert_headers {
                if h.0 == "delete-marker" {
                    headers.insert("x-amz-delete-marker", h.1.parse().unwrap());
                    signed_headers.push(("x-amz-delete-marker", h.1));
                }
            }
        }

        // Support BIGTERA secure delete feature
        if insert_headers_name.contains(&"secure-delete".to_string()) {
            for h in insert_headers {
                if h.0 == "secure-delete" {
                    headers.insert("x-amz-secure-delete", h.1.parse().unwrap());
                    signed_headers.push(("x-amz-secure-delete", h.1));
                }
            }
        }

        let mut query_strings = vec![];
        match self.format {
            Format::JSON => query_strings.push(("format", "json")),
            _ => {}
        }

        query_strings.extend(qs.iter().cloned());

        let mut query = String::from_str("http://").unwrap();
        query.push_str(self.host);
        query.push_str(uri);
        query.push('?');
        query.push_str(&aws::canonical_query_string(&mut query_strings));
        let signature = aws::aws_s3_v2_sign(
            self.secret_key,
            &aws::aws_s3_v2_get_string_to_signed(method, uri, &mut signed_headers, payload),
        );
        let mut authorize_string = String::from_str("AWS ").unwrap();
        authorize_string.push_str(self.access_key);
        authorize_string.push(':');
        authorize_string.push_str(&signature);
        headers.insert(header::AUTHORIZATION, authorize_string.parse().unwrap());

        // get a client builder
        let client = Client::builder().default_headers(headers).build().unwrap();

        let action;
        match method {
            "GET" => {
                action = client.get(query.as_str());
            }
            "PUT" => {
                action = client.put(query.as_str());
            }
            "DELETE" => {
                action = client.delete(query.as_str());
            }
            "POST" => {
                action = client.post(query.as_str());
            }
            _ => {
                error!("unspport HTTP verb");
                action = client.get(query.as_str());
            }
        }
        match action.body((*payload).clone()).send() {
            Ok(mut res) => Ok(res.handle_response()),
            Err(_) => Err("Reqwest Error"),
        }
    }

    // region, endpoint parameters are used for HTTP redirect
    fn _aws_v4_request(
        &self,
        method: &str,
        virtural_host: Option<String>,
        uri: &str,
        qs: &Vec<(&str, &str)>,
        insert_headers: &Vec<(&str, &str)>,
        payload: Vec<u8>,
        region: Option<String>,
        endpoint: Option<String>,
    ) -> Result<(Vec<u8>, reqwest::header::HeaderMap), &'static str> {
        let utc: DateTime<Utc> = Utc::now();
        let mut headers = header::HeaderMap::new();
        let time_str = utc.format("%Y%m%dT%H%M%SZ").to_string();
        headers.insert("x-amz-date", time_str.clone().parse().unwrap());

        let payload_hash = aws::hash_payload(&payload);
        headers.insert("x-amz-content-sha256", payload_hash.parse().unwrap());

        // follow the endpoint from http redirect first
        let hostname = match endpoint {
            Some(ep) => ep,
            None => match virtural_host {
                Some(vs) => {
                    let mut host = vs;
                    host.push_str(".");
                    host.push_str(self.host);
                    host
                }
                None => self.host.to_string(),
            },
        };

        let insert_headers_name: Vec<String> = insert_headers
            .into_iter()
            .map(|x| x.0.to_string())
            .collect();

        let mut signed_headers = vec![
            ("X-AMZ-Date", time_str.as_str()),
            ("Host", hostname.as_str()),
        ];

        // Support AWS delete marker feature
        if insert_headers_name.contains(&"delete-marker".to_string()) {
            for h in insert_headers {
                if h.0 == "delete-marker" {
                    headers.insert("x-amz-delete-marker", h.1.parse().unwrap());
                    signed_headers.push(("x-amz-delete-marker", h.1));
                }
            }
        }

        // Support BIGTERA secure delete feature
        if insert_headers_name.contains(&"secure-delete".to_string()) {
            for h in insert_headers {
                if h.0 == "secure-delete" {
                    headers.insert("x-amz-secure-delete", h.1.parse().unwrap());
                    signed_headers.push(("x-amz-secure-delete", h.1));
                }
            }
        }

        let mut query_strings = vec![];
        match self.format {
            Format::JSON => query_strings.push(("format", "json")),
            _ => {}
        }
        query_strings.extend(qs.iter().cloned());

        let mut query = String::from_str("http://").unwrap();
        query.push_str(hostname.as_str());
        query.push_str(uri);
        query.push('?');
        query.push_str(&aws::canonical_query_string(&mut query_strings));
        let signature = aws::aws_v4_sign(
            self.secret_key,
            aws::aws_v4_get_string_to_signed(
                method,
                uri,
                &mut query_strings,
                &mut signed_headers,
                &payload,
                utc.format("%Y%m%dT%H%M%SZ").to_string(),
                region.clone(),
                false,
            )
            .as_str(),
            utc.format("%Y%m%d").to_string(),
            region.clone(),
            false,
        );
        let mut authorize_string = String::from_str("AWS4-HMAC-SHA256 Credential=").unwrap();
        authorize_string.push_str(self.access_key);
        authorize_string.push('/');
        authorize_string.push_str(&format!(
            "{}/{}/s3/aws4_request, SignedHeaders={}, Signature={}",
            utc.format("%Y%m%d").to_string(),
            region.clone().unwrap_or(String::from("us-east-1")),
            aws::signed_headers(&mut signed_headers),
            signature
        ));
        headers.insert(header::AUTHORIZATION, authorize_string.parse().unwrap());

        // get a client builder
        let client = Client::builder().default_headers(headers).build().unwrap();

        let action;
        match method {
            "GET" => {
                action = client.get(query.as_str());
            }
            "PUT" => {
                action = client.put(query.as_str());
            }
            "DELETE" => {
                action = client.delete(query.as_str());
            }
            "POST" => {
                action = client.post(query.as_str());
            }
            _ => {
                error!("unspport HTTP verb");
                action = client.get(query.as_str());
            }
        }
        match action.body(payload.clone()).send() {
            Ok(mut res) => {
                match res.status().is_redirection() {
                    true => {
                        let body = res.handle_response().0;
                        let result = std::str::from_utf8(&body).unwrap_or("");
                        let mut endpoint = "".to_string();
                        match self.format {
                            Format::JSON => {
                                // Not implement, AWS response is XML, maybe ceph need this
                            }
                            Format::XML => {
                                let mut reader = Reader::from_str(&result);
                                let mut in_tag = false;
                                let mut buf = Vec::new();

                                loop {
                                    match reader.read_event(&mut buf) {
                                        Ok(Event::Start(ref e)) => {
                                            if e.name() == b"Endpoint" {
                                                in_tag = true;
                                            }
                                        }
                                        Ok(Event::End(ref e)) => {
                                            if e.name() == b"Endpoint" {
                                                in_tag = false;
                                            }
                                        }
                                        Ok(Event::Text(e)) => {
                                            if in_tag {
                                                endpoint = e.unescape_and_decode(&reader).unwrap();
                                            }
                                        }
                                        Ok(Event::Eof) => break,
                                        Err(e) => panic!(
                                            "Error at position {}: {:?}",
                                            reader.buffer_position(),
                                            e
                                        ),
                                        _ => (),
                                    }
                                    buf.clear();
                                }
                            }
                        }
                        self._aws_v4_request(
                            method,
                            None,
                            uri,
                            qs,
                            insert_headers,
                            payload,
                            Some(
                                res.headers()["x-amz-bucket-region"]
                                    .to_str()
                                    .unwrap_or("")
                                    .to_string(),
                            ),
                            Some(endpoint),
                        )
                    }
                    false => Ok(res.handle_response()),
                }
            }
            Err(_) => Err("Reqwest Error"),
        }
    }

    fn aws_v4_request(
        &self,
        method: &str,
        virtural_host: Option<String>,
        uri: &str,
        qs: &Vec<(&str, &str)>,
        headers: &Vec<(&str, &str)>,
        payload: Vec<u8>,
    ) -> Result<(Vec<u8>, reqwest::header::HeaderMap), &'static str> {
        self._aws_v4_request(
            method,
            virtural_host,
            uri,
            qs,
            headers,
            payload,
            self.region.clone(),
            None,
        )
    }

    /// List all objects in a bucket
    pub fn la(&self) -> Result<(), &'static str> {
        let re = Regex::new(RESPONSE_FORMAT).unwrap();
        let mut res = match self.auth_type {
            AuthType::AWS4 => std::str::from_utf8(
                &self
                    .aws_v4_request("GET", None, "/", &Vec::new(), &Vec::new(), Vec::new())?
                    .0,
            )
            .unwrap_or("")
            .to_string(),
            AuthType::AWS2 => std::str::from_utf8(
                &self
                    .aws_v2_request("GET", "/", &Vec::new(), &Vec::new(), &Vec::new())?
                    .0,
            )
            .unwrap_or("")
            .to_string(),
        };
        let mut buckets = Vec::new();
        match self.format {
            Format::JSON => {
                let result: serde_json::Value;
                result = serde_json::from_str(&res).unwrap();
                for bucket_list in result[1].as_array() {
                    for bucket in bucket_list {
                        buckets.push(bucket["Name"].as_str().unwrap().to_string());
                    }
                }
            }
            Format::XML => {
                let mut reader = Reader::from_str(&res);
                let mut in_name_tag = false;
                let mut buf = Vec::new();

                loop {
                    match reader.read_event(&mut buf) {
                        Ok(Event::Start(ref e)) => {
                            if e.name() == b"Name" {
                                in_name_tag = true;
                            }
                        }
                        Ok(Event::End(ref e)) => {
                            if e.name() == b"Name" {
                                in_name_tag = false;
                            }
                        }
                        Ok(Event::Text(e)) => {
                            if in_name_tag {
                                buckets.push(e.unescape_and_decode(&reader).unwrap());
                            }
                        }
                        Ok(Event::Eof) => break,
                        Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                        _ => (),
                    }
                    buf.clear();
                }
            }
        }
        for bucket in buckets {
            let bucket_prefix = format!("s3://{}", bucket.as_str());
            match self.auth_type {
                AuthType::AWS4 => {
                    res = match self.url_style {
                        UrlStyle::PATH => std::str::from_utf8(
                            &self
                                .aws_v4_request(
                                    "GET",
                                    None,
                                    &format!("/{}", bucket.as_str()),
                                    &Vec::new(),
                                    &Vec::new(),
                                    Vec::new(),
                                )?
                                .0,
                        )
                        .unwrap_or("")
                        .to_string(),
                        UrlStyle::HOST => std::str::from_utf8(
                            &self
                                .aws_v4_request(
                                    "GET",
                                    Some(bucket),
                                    "/",
                                    &Vec::new(),
                                    &Vec::new(),
                                    Vec::new(),
                                )?
                                .0,
                        )
                        .unwrap_or("")
                        .to_string(),
                    };
                    match self.format {
                        Format::JSON => {
                            for cap in re.captures_iter(&res) {
                                println!("{}/{}", bucket_prefix, &cap[1]);
                            }
                        }
                        Format::XML => {
                            let mut reader = Reader::from_str(&res);
                            let mut in_key_tag = false;
                            let mut buf = Vec::new();

                            loop {
                                match reader.read_event(&mut buf) {
                                    Ok(Event::Start(ref e)) => {
                                        if e.name() == b"Key" {
                                            in_key_tag = true;
                                        }
                                    }
                                    Ok(Event::End(ref e)) => {
                                        if e.name() == b"Key" {
                                            in_key_tag = false;
                                        }
                                    }
                                    Ok(Event::Text(e)) => {
                                        if in_key_tag {
                                            println!(
                                                "{}/{}",
                                                bucket_prefix,
                                                e.unescape_and_decode(&reader).unwrap()
                                            );
                                        }
                                    }
                                    Ok(Event::Eof) => break,
                                    Err(e) => panic!(
                                        "Error at position {}: {:?}",
                                        reader.buffer_position(),
                                        e
                                    ),
                                    _ => (),
                                }
                                buf.clear();
                            }
                        }
                    }
                }
                AuthType::AWS2 => {
                    res = std::str::from_utf8(
                        &self
                            .aws_v2_request(
                                "GET",
                                &format!("/{}", bucket.as_str()),
                                &Vec::new(),
                                &Vec::new(),
                                &Vec::new(),
                            )?
                            .0,
                    )
                    .unwrap_or("")
                    .to_string();
                    match self.format {
                        Format::JSON => {
                            for cap in re.captures_iter(&res) {
                                println!("{}/{}", bucket_prefix, &cap[1]);
                            }
                        }
                        Format::XML => {
                            let mut reader = Reader::from_str(&res);
                            let mut in_key_tag = false;
                            let mut buf = Vec::new();

                            loop {
                                match reader.read_event(&mut buf) {
                                    Ok(Event::Start(ref e)) => {
                                        if e.name() == b"Key" {
                                            in_key_tag = true;
                                        }
                                    }
                                    Ok(Event::End(ref e)) => {
                                        if e.name() == b"Key" {
                                            in_key_tag = false;
                                        }
                                    }
                                    Ok(Event::Text(e)) => {
                                        if in_key_tag {
                                            println!(
                                                "{}/{}",
                                                bucket_prefix,
                                                e.unescape_and_decode(&reader).unwrap()
                                            );
                                        }
                                    }
                                    Ok(Event::Eof) => break,
                                    Err(e) => panic!(
                                        "Error at position {}: {:?}",
                                        reader.buffer_position(),
                                        e
                                    ),
                                    _ => (),
                                }
                                buf.clear();
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// List all bucket of an account
    pub fn ls(&self, bucket: Option<&str>) -> Result<(), &'static str> {
        let res: String;
        match bucket {
            Some(b) => {
                let uri: String;
                let mut re = Regex::new(S3_FORMAT).unwrap();
                let mut vitural_host = None;
                if b.starts_with("s3://") || b.starts_with("S3://") {
                    let caps = re.captures(b).expect("S3 object format error.");
                    match self.url_style {
                        UrlStyle::PATH => {
                            uri = format!("/{}", &caps["bucket"]);
                        }
                        UrlStyle::HOST => {
                            vitural_host = Some(format!("{}", &caps["bucket"]));
                            uri = "/".to_string();
                        }
                    }
                } else {
                    uri = format!("/{}", b);
                }
                match self.auth_type {
                    AuthType::AWS4 => {
                        res = std::str::from_utf8(
                            &self
                                .aws_v4_request(
                                    "GET",
                                    vitural_host.clone(),
                                    &uri,
                                    &Vec::new(),
                                    &Vec::new(),
                                    Vec::new(),
                                )?
                                .0,
                        )
                        .unwrap_or("")
                        .to_string();
                    }
                    AuthType::AWS2 => {
                        res = std::str::from_utf8(
                            &self
                                .aws_v2_request("GET", &uri, &Vec::new(), &Vec::new(), &Vec::new())?
                                .0,
                        )
                        .unwrap_or("")
                        .to_string();
                    }
                }
                match self.format {
                    Format::JSON => {
                        re = Regex::new(RESPONSE_FORMAT).unwrap();
                        for cap in re.captures_iter(&res) {
                            println!("s3:/{}/{}", uri, &cap[1]);
                        }
                    }
                    Format::XML => {
                        let mut reader = Reader::from_str(&res);
                        let mut in_key_tag = false;
                        let mut buf = Vec::new();

                        loop {
                            match reader.read_event(&mut buf) {
                                Ok(Event::Start(ref e)) => {
                                    if e.name() == b"Key" {
                                        in_key_tag = true
                                    }
                                }
                                Ok(Event::End(ref e)) => {
                                    if e.name() == b"Key" {
                                        in_key_tag = false
                                    }
                                }
                                Ok(Event::Text(e)) => {
                                    if in_key_tag {
                                        println!(
                                            "s3://{}/{} ",
                                            vitural_host.clone().unwrap_or(uri.to_string()),
                                            e.unescape_and_decode(&reader).unwrap()
                                        )
                                    }
                                }
                                Ok(Event::Eof) => break,
                                Err(e) => panic!(
                                    "Error at position {}: {:?}",
                                    reader.buffer_position(),
                                    e
                                ),
                                _ => (),
                            }
                            buf.clear();
                        }
                    }
                }
            }
            None => {
                match self.auth_type {
                    AuthType::AWS4 => {
                        res = std::str::from_utf8(
                            &self
                                .aws_v4_request(
                                    "GET",
                                    None,
                                    "/",
                                    &Vec::new(),
                                    &Vec::new(),
                                    Vec::new(),
                                )?
                                .0,
                        )
                        .unwrap_or("")
                        .to_string();
                    }
                    AuthType::AWS2 => {
                        res = std::str::from_utf8(
                            &self
                                .aws_v2_request("GET", "/", &Vec::new(), &Vec::new(), &Vec::new())?
                                .0,
                        )
                        .unwrap_or("")
                        .to_string();
                    }
                }
                match self.format {
                    Format::JSON => {
                        let result: serde_json::Value = serde_json::from_str(&res).unwrap();
                        for bucket_list in result[1].as_array() {
                            for bucket in bucket_list {
                                println!("s3://{} ", bucket["Name"].as_str().unwrap());
                            }
                        }
                    }
                    Format::XML => {
                        let mut reader = Reader::from_str(&res);
                        let mut in_name_tag = false;
                        let mut buf = Vec::new();

                        loop {
                            match reader.read_event(&mut buf) {
                                Ok(Event::Start(ref e)) => {
                                    if e.name() == b"Name" {
                                        in_name_tag = true
                                    }
                                }
                                Ok(Event::End(ref e)) => {
                                    if e.name() == b"Name" {
                                        in_name_tag = false
                                    }
                                }
                                Ok(Event::Text(e)) => {
                                    if in_name_tag {
                                        println!(
                                            "s3://{} ",
                                            e.unescape_and_decode(&reader).unwrap()
                                        )
                                    }
                                }
                                Ok(Event::Eof) => break,
                                Err(e) => panic!(
                                    "Error at position {}: {:?}",
                                    reader.buffer_position(),
                                    e
                                ),
                                _ => (),
                            }
                            buf.clear();
                        }
                    }
                }
            }
        };
        Ok(())
    }

    /// Upload a file to a S3 bucket
    pub fn put(&self, file: &str, dest: &str) -> Result<(), &'static str> {
        if file == "" || dest == "" {
            return Err("please specify the file and the destiney");
        }
        let re = Regex::new(S3_FORMAT).unwrap();
        let caps = match re.captures(dest) {
            Some(c) => c,
            None => return Err("S3 object format error."),
        };
        let mut content: Vec<u8>;

        let uri = if &caps["object"] == "" || &caps["object"] == "/" {
            let file_name = Path::new(file).file_name().unwrap().to_string_lossy();
            format!("/{}/{}", &caps["bucket"], file_name)
        } else {
            format!("/{}{}", &caps["bucket"], &caps["object"])
        };

        if !Path::new(file).exists() && file == "test" {
            // TODO: add time info in the test file
            content = vec![83, 51, 82, 83, 32, 116, 101, 115, 116, 10]; // S3RS test/n
            let _ = match self.auth_type {
                AuthType::AWS4 => {
                    self.aws_v4_request("PUT", None, &uri, &Vec::new(), &Vec::new(), content)
                }
                AuthType::AWS2 => {
                    self.aws_v2_request("PUT", &uri, &Vec::new(), &Vec::new(), &content)
                }
            };
        } else {
            let file_size = match metadata(Path::new(file)) {
                Ok(m) => m.len(),
                Err(e) => {
                    error!("file meta error: {}", e);
                    0
                }
            };

            debug!("upload file size: {}", file_size);

            if file_size > 5242880 {
                let res = match self.auth_type {
                    AuthType::AWS4 => std::str::from_utf8(
                        &self
                            .aws_v4_request(
                                "POST",
                                None,
                                &uri,
                                &vec![("uploads", "")],
                                &Vec::new(),
                                Vec::new(),
                            )?
                            .0,
                    )
                    .unwrap_or("")
                    .to_string(),
                    AuthType::AWS2 => std::str::from_utf8(
                        &self
                            .aws_v2_request(
                                "POST",
                                &uri,
                                &vec![("uploads", "")],
                                &Vec::new(),
                                &Vec::new(),
                            )?
                            .0,
                    )
                    .unwrap_or("")
                    .to_string(),
                };
                let mut upload_id = "".to_string();
                match self.format {
                    Format::JSON => {
                        error!("No JSON Multipart Implement");
                    }
                    Format::XML => {
                        let mut reader = Reader::from_str(&res);
                        let mut in_tag = false;
                        let mut buf = Vec::new();

                        loop {
                            match reader.read_event(&mut buf) {
                                Ok(Event::Start(ref e)) => {
                                    if e.name() == b"UploadId" {
                                        in_tag = true;
                                    }
                                }
                                Ok(Event::End(ref e)) => {
                                    if e.name() == b"UploadId" {
                                        in_tag = false;
                                    }
                                }
                                Ok(Event::Text(e)) => {
                                    if in_tag {
                                        upload_id = e.unescape_and_decode(&reader).unwrap();
                                    }
                                }
                                Ok(Event::Eof) => break,
                                Err(e) => panic!(
                                    "Error at position {}: {:?}",
                                    reader.buffer_position(),
                                    e
                                ),
                                _ => (),
                            }
                            buf.clear();
                        }
                    }
                }

                info!("upload id: {}", upload_id);

                let mut etags = Vec::new();
                let mut part = 0u64;
                let mut fin = match File::open(file) {
                    Ok(f) => f,
                    Err(_) => return Err("input file open error"),
                };
                loop {
                    let mut buffer = [0; 5242880];
                    match fin.read_exact(&mut buffer) {
                        Ok(_) => {}
                        Err(e) => {
                            error!("partial read file error: {}", e);
                        }
                    };

                    part += 1;

                    trace!("part {}, size: {}", part, buffer.to_vec().len());

                    let headers = match self.auth_type {
                        AuthType::AWS4 => {
                            self.aws_v4_request(
                                "PUT",
                                None,
                                &uri,
                                &vec![
                                    ("uploadId", upload_id.as_str()),
                                    ("partNumber", part.to_string().as_str()),
                                ],
                                &Vec::new(),
                                buffer.to_vec(),
                            )?
                            .1
                        }
                        AuthType::AWS2 => {
                            self.aws_v2_request(
                                "PUT",
                                &uri,
                                &vec![
                                    ("uploadId", upload_id.as_str()),
                                    ("partNumber", part.to_string().as_str()),
                                ],
                                &Vec::new(),
                                &buffer.to_vec(),
                            )?
                            .1
                        }
                    };
                    let etag = headers[reqwest::header::ETAG]
                        .to_str()
                        .expect("unexpected etag from server");
                    etags.push((part.clone(), etag.to_string()));
                    info!("part: {} uploaded, etag: {}", part, etag);

                    if part * 5242880 >= file_size {
                        let mut content = format!("<CompleteMultipartUpload>");
                        for etag in etags {
                            content.push_str(&format!(
                                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                                etag.0, etag.1
                            ));
                        }
                        content.push_str(&format!("</CompleteMultipartUpload>"));
                        let _ = match self.auth_type {
                            AuthType::AWS4 => self.aws_v4_request(
                                "POST",
                                None,
                                &uri,
                                &vec![("uploadId", upload_id.as_str())],
                                &Vec::new(),
                                content.into_bytes(),
                            ),
                            AuthType::AWS2 => self.aws_v2_request(
                                "POST",
                                &uri,
                                &vec![("uploadId", upload_id.as_str())],
                                &Vec::new(),
                                &content.into_bytes(),
                            ),
                        };
                        info!("complete multipart");
                        break;
                    }
                }
            } else {
                content = Vec::new();
                let mut fin = match File::open(file) {
                    Ok(f) => f,
                    Err(_) => return Err("input file open error"),
                };
                let _ = fin.read_to_end(&mut content);
                let _ = match self.auth_type {
                    AuthType::AWS4 => {
                        self.aws_v4_request("PUT", None, &uri, &Vec::new(), &Vec::new(), content)
                    }
                    AuthType::AWS2 => {
                        self.aws_v2_request("PUT", &uri, &Vec::new(), &Vec::new(), &content)
                    }
                };
            };
        }
        Ok(())
    }

    /// Download an object from S3 service
    pub fn get(&self, src: &str, file: Option<&str>) -> Result<(), &'static str> {
        if src == "" {
            return Err("Please specify the object");
        }
        let re = Regex::new(S3_FORMAT).unwrap();
        let mut virtural_host = None;
        let uri: String;
        let caps = match re.captures(src) {
            Some(c) => c,
            None => return Err("S3 object format error."),
        };

        let fout = match file {
            Some(fname) => fname,
            None => Path::new(src)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap_or("s3download"),
        };

        if &caps["object"] == "" {
            return Err("Please specific the object");
        }

        match self.auth_type {
            AuthType::AWS4 => {
                match self.url_style {
                    UrlStyle::PATH => {
                        uri = format!("/{}{}", &caps["bucket"], &caps["object"]);
                    }
                    UrlStyle::HOST => {
                        virtural_host = Some(format!("{}", &caps["bucket"]));
                        uri = format!("{}", &caps["object"]);
                    }
                }
                match write(
                    fout,
                    self.aws_v4_request(
                        "GET",
                        virtural_host,
                        &uri,
                        &Vec::new(),
                        &Vec::new(),
                        Vec::new(),
                    )?
                    .0,
                ) {
                    Ok(_) => return Ok(()),
                    Err(_) => return Err("write file error"), //XXX
                }
            }
            AuthType::AWS2 => {
                match write(
                    fout,
                    self.aws_v2_request(
                        "GET",
                        &format!("/{}{}", &caps["bucket"], &caps["object"]),
                        &Vec::new(),
                        &Vec::new(),
                        &Vec::new(),
                    )?
                    .0,
                ) {
                    Ok(_) => return Ok(()),
                    Err(_) => return Err("write file error"), //XXX
                }
            }
        }
    }

    /// Show an object's content, this method is use for quick check a small object on the fly
    pub fn cat(&self, src: &str) -> Result<(), &'static str> {
        if src == "" {
            return Err("please specific the object");
        }
        let re = Regex::new(S3_FORMAT).unwrap();
        let mut virtural_host = None;
        let uri: String;
        let caps = match re.captures(src) {
            Some(c) => c,
            None => return Err("S3 object format error."),
        };

        if &caps["object"] == "" {
            return Err("Please specific the object");
        }

        match self.auth_type {
            AuthType::AWS4 => {
                match self.url_style {
                    UrlStyle::PATH => {
                        uri = format!("/{}{}", &caps["bucket"], &caps["object"]);
                    }
                    UrlStyle::HOST => {
                        virtural_host = Some(format!("{}", &caps["bucket"]));
                        uri = format!("{}", &caps["object"]);
                    }
                }
                match self.aws_v4_request(
                    "GET",
                    virtural_host,
                    &uri,
                    &Vec::new(),
                    &Vec::new(),
                    Vec::new(),
                ) {
                    Ok(r) => {
                        println!("{}", std::str::from_utf8(&r.0).unwrap_or(""));
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                }
            }
            AuthType::AWS2 => {
                match self.aws_v2_request(
                    "GET",
                    &format!("/{}{}", &caps["bucket"], &caps["object"]),
                    &Vec::new(),
                    &Vec::new(),
                    &Vec::new(),
                ) {
                    Ok(r) => {
                        println!("{}", std::str::from_utf8(&r.0).unwrap_or(""));
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }

    /// Delete with header flags for some deletion features
    /// - AWS - delete-marker
    /// - Bigtera - secure-delete
    pub fn del_with_flag(
        &self,
        src: &str,
        headers: &Vec<(&str, &str)>,
    ) -> Result<(), &'static str> {
        debug!("headers: {:?}", headers);
        if src == "" {
            return Err("please specific the object");
        }
        let re = Regex::new(S3_FORMAT).unwrap();
        let mut virtural_host = None;
        let uri: String;
        let caps = match re.captures(src) {
            Some(c) => c,
            None => return Err("S3 object format error."),
        };

        if &caps["object"] == "" {
            return Err("Please specific the object");
        }
        match self.url_style {
            UrlStyle::PATH => {
                uri = format!("/{}{}", &caps["bucket"], &caps["object"]);
            }
            UrlStyle::HOST => {
                virtural_host = Some(format!("{}", &caps["bucket"]));
                uri = format!("{}", &caps["object"]);
            }
        }

        let _ = match self.auth_type {
            AuthType::AWS4 => self.aws_v4_request(
                "DELETE",
                virtural_host,
                &uri,
                &Vec::new(),
                headers,
                Vec::new(),
            ),
            AuthType::AWS2 => self.aws_v2_request(
                "GET",
                &format!("/{}{}", &caps["bucket"], &caps["object"]),
                &Vec::new(),
                headers,
                &Vec::new(),
            ),
        };
        Ok(())
    }

    /// Delete an object
    pub fn del(&self, src: &str) -> Result<(), &'static str> {
        self.del_with_flag(src, &Vec::new())
    }

    /// Make a new bucket
    pub fn mb(&self, bucket: &str) -> Result<(), &'static str> {
        if bucket == "" {
            return Err("please specific the bucket name");
        }
        let mut uri = String::from_str("/").unwrap();
        uri.push_str(bucket);
        let _ = match self.auth_type {
            AuthType::AWS4 => {
                self.aws_v4_request("PUT", None, &uri, &Vec::new(), &Vec::new(), Vec::new())
            }
            AuthType::AWS2 => {
                self.aws_v2_request("PUT", &uri, &Vec::new(), &Vec::new(), &Vec::new())
            }
        };
        Ok(())
    }

    /// Remove a bucket
    pub fn rb(&self, bucket: &str) -> Result<(), &'static str> {
        if bucket == "" {
            return Err("please specific the bucket name");
        }
        let mut uri = String::from_str("/").unwrap();
        uri.push_str(bucket);
        let _ = match self.auth_type {
            AuthType::AWS4 => {
                self.aws_v4_request("DELETE", None, &uri, &Vec::new(), &Vec::new(), Vec::new())
            }
            AuthType::AWS2 => {
                self.aws_v2_request("DELETE", &uri, &Vec::new(), &Vec::new(), &Vec::new())
            }
        };
        Ok(())
    }

    /// list all tags of an object
    pub fn list_tag(&self, target: &str) -> Result<(), &'static str> {
        let res: String;
        debug!("target: {:?}", target);
        if target == "" {
            return Err("please specific the object");
        }
        let re = Regex::new(S3_FORMAT).unwrap();
        let mut virtural_host = None;
        let uri: String;
        let caps = match re.captures(target) {
            Some(c) => c,
            None => return Err("S3 object format error."),
        };

        if &caps["object"] == "" {
            return Err("Please specific the object");
        }

        match self.url_style {
            UrlStyle::PATH => {
                uri = format!("/{}{}", &caps["bucket"], &caps["object"]);
            }
            UrlStyle::HOST => {
                virtural_host = Some(format!("{}", &caps["bucket"]));
                uri = format!("{}", &caps["object"]);
            }
        }

        let query_string = vec![("tagging", "")];
        res = match self.auth_type {
            AuthType::AWS4 => std::str::from_utf8(
                &self
                    .aws_v4_request(
                        "GET",
                        virtural_host,
                        &uri,
                        &query_string,
                        &Vec::new(),
                        Vec::new(),
                    )?
                    .0,
            )
            .unwrap_or("")
            .to_string(),
            AuthType::AWS2 => std::str::from_utf8(
                &self
                    .aws_v2_request(
                        "GET",
                        &format!("/{}{}", &caps["bucket"], &caps["object"]),
                        &query_string,
                        &Vec::new(),
                        &Vec::new(),
                    )?
                    .0,
            )
            .unwrap_or("")
            .to_string(),
        };
        // TODO:
        // parse tagging output when CEPH tagging json format respose bug fixed
        println!("{}", res);
        Ok(())
    }

    /// Put a tag on an object
    pub fn add_tag(&self, target: &str, tags: &Vec<(&str, &str)>) -> Result<(), &'static str> {
        debug!("target: {:?}", target);
        debug!("tags: {:?}", tags);
        if target == "" {
            return Err("please specific the object");
        }
        let re = Regex::new(S3_FORMAT).unwrap();
        let mut virtural_host = None;
        let uri: String;
        let caps = match re.captures(target) {
            Some(c) => c,
            None => return Err("S3 object format error."),
        };

        if &caps["object"] == "" {
            return Err("Please specific the object");
        }
        match self.url_style {
            UrlStyle::PATH => {
                uri = format!("/{}{}", &caps["bucket"], &caps["object"]);
            }
            UrlStyle::HOST => {
                virtural_host = Some(format!("{}", &caps["bucket"]));
                uri = format!("{}", &caps["object"]);
            }
        }
        let mut content = format!("<Tagging><TagSet>");
        for tag in tags {
            content.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                tag.0, tag.1
            ));
        }
        content.push_str(&format!("</TagSet></Tagging>"));
        debug!("payload: {:?}", content);

        let query_string = vec![("tagging", "")];
        let _ = match self.auth_type {
            AuthType::AWS4 => self.aws_v4_request(
                "PUT",
                virtural_host,
                &uri,
                &query_string,
                &Vec::new(),
                content.into_bytes(),
            ),
            AuthType::AWS2 => self.aws_v2_request(
                "PUT",
                &format!("/{}{}", &caps["bucket"], &caps["object"]),
                &query_string,
                &Vec::new(),
                &content.into_bytes(),
            ),
        };
        Ok(())
    }

    /// Remove aa tag from an object
    pub fn del_tag(&self, target: &str) -> Result<(), &'static str> {
        debug!("target: {:?}", target);
        if target == "" {
            return Err("please specific the object");
        }
        let re = Regex::new(S3_FORMAT).unwrap();
        let mut virtural_host = None;
        let uri: String;
        let caps = match re.captures(target) {
            Some(c) => c,
            None => return Err("S3 object format error."),
        };

        if &caps["object"] == "" {
            return Err("Please specific the object");
        }

        match self.url_style {
            UrlStyle::PATH => {
                uri = format!("/{}{}", &caps["bucket"], &caps["object"]);
            }
            UrlStyle::HOST => {
                virtural_host = Some(format!("{}", &caps["bucket"]));
                uri = format!("{}", &caps["object"]);
            }
        }

        let query_string = vec![("tagging", "")];
        let _ = match self.auth_type {
            AuthType::AWS4 => self.aws_v4_request(
                "DELETE",
                virtural_host,
                &uri,
                &query_string,
                &Vec::new(),
                Vec::new(),
            ),
            AuthType::AWS2 => self.aws_v2_request(
                "DELETE",
                &format!("/{}{}", &caps["bucket"], &caps["object"]),
                &query_string,
                &Vec::new(),
                &Vec::new(),
            ),
        };
        Ok(())
    }

    /// Show the usage of a bucket (CEPH only)
    pub fn usage(&self, target: &str, options: &Vec<(&str, &str)>) -> Result<(), &'static str> {
        let re = Regex::new(S3_FORMAT).unwrap();
        let uri = format!("/admin/bucket");
        let caps = match re.captures(target) {
            Some(c) => c,
            None => return Err("S3 format error."),
        };
        let bucket = caps["bucket"].to_string();
        let mut query_strings = options.clone();
        query_strings.push(("bucket", &bucket));
        let result = match self.auth_type {
            AuthType::AWS4 => {
                self.aws_v4_request("GET", None, &uri, &query_strings, &Vec::new(), Vec::new())?
            }
            AuthType::AWS2 => {
                self.aws_v2_request("GET", &uri, &query_strings, &Vec::new(), &Vec::new())?
            }
        };
        match self.format {
            Format::JSON => {
                let json: serde_json::Value;
                json = serde_json::from_str(std::str::from_utf8(&result.0).unwrap_or("")).unwrap();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json["usage"]).unwrap_or("".to_string())
                );
            }
            Format::XML => {
                // TODO:
                // Ceph Ops api may not support xml
            }
        };
        Ok(())
    }

    /// Do a GET request for the specific URL
    /// This method is easily to show the configure of S3 not implemented
    pub fn url_command(&self, url: &str) -> Result<(), &'static str> {
        let mut uri = String::new();
        let mut raw_qs = String::new();
        let mut query_strings = Vec::new();
        match url.find('?') {
            Some(idx) => {
                uri.push_str(&url[..idx]);
                raw_qs.push_str(&String::from_str(&url[idx + 1..]).unwrap());
                for q_pair in raw_qs.split('&') {
                    match q_pair.find('=') {
                        Some(_) => query_strings.push((
                            q_pair.split('=').nth(0).unwrap(),
                            q_pair.split('=').nth(1).unwrap(),
                        )),
                        None => query_strings.push((&q_pair, "")),
                    }
                }
            }
            None => {
                uri.push_str(&url);
            }
        }

        let result = match self.auth_type {
            AuthType::AWS4 => {
                self.aws_v4_request("GET", None, &uri, &query_strings, &Vec::new(), Vec::new())?
            }
            AuthType::AWS2 => {
                self.aws_v2_request("GET", &uri, &query_strings, &Vec::new(), &Vec::new())?
            }
        };
        println!("{}", std::str::from_utf8(&result.0).unwrap_or(""));
        Ok(())
    }
    /// Change S3 type to aws/ceph
    pub fn change_s3_type(&mut self, command: &str) {
        println!("set up s3 type as {}", command);
        if command.ends_with("aws") {
            self.auth_type = AuthType::AWS4;
            self.format = Format::XML;
            self.url_style = UrlStyle::HOST;
            println!("using aws verion 4 signature, xml format, and host style url");
        } else if command.ends_with("ceph") {
            self.auth_type = AuthType::AWS4;
            self.format = Format::JSON;
            self.url_style = UrlStyle::PATH;
            println!("using aws verion 4 signature, json format, and path style url");
        } else {
            println!("usage: s3_type [aws/ceph]");
        }
    }

    /// Change signature version to aws2/aws4
    /// CEPH support aws2 and aws4
    /// following AWS region support v2 signature before June 24, 2019
    /// - US East (N. Virginia) Region
    /// - US West (N. California) Region
    /// - US West (Oregon) Region
    /// - EU (Ireland) Region
    /// - Asia Pacific (Tokyo) Region
    /// - Asia Pacific (Singapore) Region
    /// - Asia Pacific (Sydney) Region
    /// - South America (So Paulo) Region
    pub fn change_auth_type(&mut self, command: &str) {
        if command.ends_with("aws2") {
            self.auth_type = AuthType::AWS2;
            println!("using aws version 2 signature");
        } else if command.ends_with("aws4") || command.ends_with("aws") {
            self.auth_type = AuthType::AWS4;
            println!("using aws verion 4 signature");
        } else {
            println!("usage: auth_type [aws4/aws2]");
        }
    }

    /// Change response format to xml/json
    /// CEPH support json and xml
    /// AWS only support xml
    pub fn change_format_type(&mut self, command: &str) {
        if command.ends_with("xml") {
            self.format = Format::XML;
            println!("using xml format");
        } else if command.ends_with("json") {
            self.format = Format::JSON;
            println!("using json format");
        } else {
            println!("usage: format_type [xml/json]");
        }
    }

    /// Change request url style
    pub fn change_url_style(&mut self, command: &str) {
        if command.ends_with("path") {
            self.url_style = UrlStyle::PATH;
            println!("using path style url");
        } else if command.ends_with("host") {
            self.url_style = UrlStyle::HOST;
            println!("using host style url");
        } else {
            println!("usage: url_style [path/host]");
        }
    }

    /// Initailize the `Handler` from `CredentialConfig`
    /// ```
    /// let config = s3handler::CredentialConfig{
    ///     host: "s3.us-east-1.amazonaws.com".to_string(),
    ///     access_key: "akey".to_string(),
    ///     secret_key: "skey".to_string(),
    ///     user: None,
    ///     region: None, // default is us-east-1
    ///     s3_type: None, // default will try to config as AWS S3 handler
    /// };
    /// let handler = s3handler::Handler::init_from_config(&config);
    /// ```
    pub fn init_from_config(credential: &'a CredentialConfig) -> Self {
        debug!("host: {}", credential.host);
        debug!("access key: {}", credential.access_key);
        debug!("secret key: {}", credential.secret_key);
        match credential
            .clone()
            .s3_type
            .unwrap_or("".to_string())
            .as_str()
        {
            "aws" => Handler {
                host: &credential.host,
                access_key: &credential.access_key,
                secret_key: &credential.secret_key,
                auth_type: AuthType::AWS4,
                format: Format::XML,
                url_style: UrlStyle::HOST,
                region: credential.region.clone(),
            },
            "ceph" => Handler {
                host: &credential.host,
                access_key: &credential.access_key,
                secret_key: &credential.secret_key,
                auth_type: AuthType::AWS4,
                format: Format::JSON,
                url_style: UrlStyle::PATH,
                region: credential.region.clone(),
            },
            _ => Handler {
                host: &credential.host,
                access_key: &credential.access_key,
                secret_key: &credential.secret_key,
                auth_type: AuthType::AWS4,
                format: Format::XML,
                url_style: UrlStyle::PATH,
                region: credential.region.clone(),
            },
        }
    }
}
