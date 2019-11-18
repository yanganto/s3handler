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
//! let _ = handler.la();
//! ```
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
extern crate colored;
extern crate url;

use std::convert::From;
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
use url::Url;
use async_std::task;


mod aws;

static RESPONSE_CONTENT_FORMAT: &'static str =
    r#""Contents":\["([^"]+?)","([^"]+?)","\\"([^"]+?)\\"",([^"]+?),"([^"]+?)"(.*?)\]"#;
static RESPONSE_MARKER_FORMAT: &'static str = r#""NextMarker":"([^"]+?)","#;

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

/// # Flexible S3 format parser
/// - bucket - the objeck belonge to which
/// - key - the object key
/// - mtime - the last modified time
/// - etag - the etag calculated by server (MD5 in general)
/// - storage_class - the storage class of this object
/// ```
/// use s3handler::{S3Object, S3Convert};
///
/// let s3_object = S3Object::from("s3://bucket/objeckt_key".to_string());
/// assert_eq!(s3_object.bucket, Some("bucket".to_string()));
/// assert_eq!(s3_object.key, Some("/objeckt_key".to_string()));
/// assert_eq!("s3://bucket/objeckt_key".to_string(), String::from(s3_object));
///
/// let s3_object: S3Object = S3Object::from("S3://bucket/objeckt_key".to_string());
/// assert_eq!("s3://bucket/objeckt_key".to_string(), String::from(s3_object));
///
/// let s3_object: S3Object = S3Object::from("/bucket/objeckt_key".to_string());
/// assert_eq!("s3://bucket/objeckt_key".to_string(), String::from(s3_object));
///
/// let s3_object: S3Object = S3Object::from("bucket/objeckt_key".to_string());
/// assert_eq!("s3://bucket/objeckt_key".to_string(), String::from(s3_object));
/// ```
#[derive(Debug, Clone)]
pub struct S3Object {
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub mtime: Option<String>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
}

impl From<String> for S3Object {
    fn from(s3_path: String) -> Self {
        if s3_path.starts_with("s3://") || s3_path.starts_with("S3://") {
            let url_parser = Url::parse(&s3_path).unwrap();
            let bucket = match url_parser.host_str() {
                Some(h) if h != "" => Some(h.to_string()),
                _ => None,
            };
            match url_parser.path() {
                "/" => S3Object {
                    bucket: bucket,
                    key: None,
                    mtime: None,
                    etag: None,
                    storage_class: None,
                },
                _ => S3Object {
                    bucket: bucket,
                    key: Some(url_parser.path().to_string()),
                    mtime: None,
                    etag: None,
                    storage_class: None,
                },
            }
        } else {
            S3Convert::new_from_uri(s3_path)
        }
    }
}

impl From<S3Object> for String {
    fn from(s3_object: S3Object) -> Self {
        match s3_object.bucket {
            Some(b) => match s3_object.key {
                Some(k) => format!("s3://{}{}", b, k),
                None => format!("s3://{}", b),
            },
            None => format!("s3://"),
        }
    }
}

pub trait S3Convert {
    fn virtural_host_style_links(&self, host: String) -> (String, String);
    fn path_style_links(&self, host: String) -> (String, String);
    fn new_from_uri(path: String) -> Self;
    fn new(
        bucket: Option<String>,
        key: Option<String>,
        mtime: Option<String>,
        etag: Option<String>,
        storage_class: Option<String>,
    ) -> Self;
}

impl S3Convert for S3Object {
    fn virtural_host_style_links(&self, host: String) -> (String, String) {
        match self.bucket.clone() {
            Some(b) => (
                format!("{}.{}", b, host),
                self.key.clone().unwrap_or("/".to_string()),
            ),
            None => (host, "/".to_string()),
        }
    }
    fn path_style_links(&self, host: String) -> (String, String) {
        match self.bucket.clone() {
            Some(b) => (
                host,
                format!("/{}{}", b, self.key.clone().unwrap_or("/".to_string())),
            ),
            None => (host, "/".to_string()),
        }
    }
    fn new_from_uri(uri: String) -> S3Object {
        let re = Regex::new(r#"/?(?P<bucket>[A-Za-z0-9\-\._]+)(?P<object>[A-Za-z0-9\-\._/]*)\s*"#)
            .unwrap();
        let caps = re.captures(&uri).expect("S3 object uri format error.");
        if &caps["object"] == "" || &caps["object"] == "/" {
            S3Object {
                bucket: Some(caps["bucket"].to_string()),
                key: None,
                mtime: None,
                etag: None,
                storage_class: None,
            }
        } else {
            S3Object {
                bucket: Some(caps["bucket"].to_string()),
                key: Some(caps["object"].to_string()),
                mtime: None,
                etag: None,
                storage_class: None,
            }
        }
    }
    fn new(
        bucket: Option<String>,
        object: Option<String>,
        mtime: Option<String>,
        etag: Option<String>,
        storage_class: Option<String>,
    ) -> S3Object {
        let key = match object {
            None => None,
            Some(b) => {
                if b.starts_with("/") {
                    Some(b)
                } else {
                    Some(format!("/{}", b))
                }
            }
        };

        S3Object {
            bucket: bucket,
            key: key,
            mtime: mtime,
            etag: etag,
            storage_class: storage_class,
        }
    }
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
        s3_object: &S3Object,
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
        let links;
        match self.url_style {
            UrlStyle::HOST => {
                links = s3_object.virtural_host_style_links(self.host.to_string());
                query.push_str(&links.0);
                query.push_str(&links.1);
            }
            UrlStyle::PATH => {
                links = s3_object.path_style_links(self.host.to_string());
                query.push_str(self.host);
                query.push_str(&links.1);
            }
        }
        query.push('?');
        query.push_str(&aws::canonical_query_string(&mut query_strings));
        let signature = aws::aws_s3_v2_sign(
            self.secret_key,
            &aws::aws_s3_v2_get_string_to_signed(method, &links.1, &mut signed_headers, payload),
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

    async fn aws_v2_multipart_future(
        &self,
        s3_object: &S3Object,
        upload_id: String,
        part_number: String,
        payload: &Vec<u8>,
    ) -> Result<(String, String), &'static str> {
        let headers = self.aws_v2_request(
            "PUT",
            s3_object,
            &vec![("uploadId", upload_id.as_str()), ("partNumber", part_number.as_str()), ],
            &Vec::new(),
            payload
        ).expect("mulitpart request error").1;
        Ok((part_number.clone(), headers[reqwest::header::ETAG].to_str().expect("unexpected etag from server").to_string()))
    }

    // region, endpoint parameters are used for HTTP redirect
    fn _aws_v4_request(
        &self,
        method: &str,
        s3_object: &S3Object,
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

        let links = match self.url_style {
            UrlStyle::HOST => s3_object.virtural_host_style_links(self.host.to_string()),
            UrlStyle::PATH => s3_object.path_style_links(self.host.to_string()),
        };
        // follow the endpoint from http redirect first
        let hostname = match endpoint {
            Some(ep) => ep,
            None => links.0,
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

        let mut query = String::from_str("http://").unwrap(); // TODO SSL as config
        query.push_str(hostname.as_str());
        query.push_str(links.1.as_str());
        query.push('?');
        query.push_str(&aws::canonical_query_string(&mut query_strings));
        let signature = aws::aws_v4_sign(
            self.secret_key,
            aws::aws_v4_get_string_to_signed(
                method,
                links.1.as_str(),
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
                                unimplemented!();
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
                            s3_object,
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
        s3_object: &S3Object,
        qs: &Vec<(&str, &str)>,
        headers: &Vec<(&str, &str)>,
        payload: Vec<u8>,
    ) -> Result<(Vec<u8>, reqwest::header::HeaderMap), &'static str> {
        self._aws_v4_request(
            method,
            s3_object,
            qs,
            headers,
            payload,
            self.region.clone(),
            None,
        )
    }

    async fn aws_v4_multipart_future(
        &self,
        s3_object: &S3Object,
        upload_id: String,
        part_number: String,
        payload: Vec<u8>,
    ) -> Result<(String, String), &'static str> {
        let headers = self._aws_v4_request(
            "PUT",
            s3_object,
            &vec![("uploadId", upload_id.as_str()), ("partNumber", part_number.as_str()), ],
            &Vec::new(),
            payload,
            self.region.clone(),
            None,
        ).expect("mulitpart request error").1;
        Ok((part_number.clone(), headers[reqwest::header::ETAG].to_str().expect("unexpected etag from server").to_string()))
    }

    fn next_marker_xml_parser(&self, res: &str) -> Option<String> {
        let mut reader = Reader::from_str(res);
        let mut in_tag = false;
        let mut buf = Vec::new();
        let mut output = "".to_string();
        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => match e.name() {
                    b"NextMarker" => in_tag = true,
                    _ => {}
                },
                Ok(Event::End(ref e)) => match e.name() {
                    _ => {}
                },
                Ok(Event::Text(e)) => {
                    if in_tag {
                        output = e.unescape_and_decode(&reader).unwrap();
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                _ => (),
            }
            buf.clear();
        }
        if output.is_empty() {
            None
        } else {
            Some(output)
        }
    }

    fn object_list_xml_parser(&self, res: &str) -> Result<Vec<S3Object>, &'static str> {
        let mut output = Vec::new();
        let mut reader = Reader::from_str(res);
        let mut in_name_tag = false;
        let mut in_key_tag = false;
        let mut in_mtime_tag = false;
        let mut in_etag_tag = false;
        let mut in_storage_class_tag = false;
        let mut bucket = String::new();
        let mut key = String::new();
        let mut mtime = String::new();
        let mut etag = String::new();
        let mut storage_class = String::new();
        let mut buf = Vec::new();
        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => match e.name() {
                    b"Name" => in_name_tag = true,
                    b"Key" => in_key_tag = true,
                    b"LastModified" => in_mtime_tag = true,
                    b"ETag" => in_etag_tag = true,
                    b"StorageClass" => in_storage_class_tag = true,
                    _ => {}
                },
                Ok(Event::End(ref e)) => match e.name() {
                    b"Name" => {
                        output.push(S3Convert::new(Some(bucket.clone()), None, None, None, None))
                    }
                    b"Contents" => output.push(S3Convert::new(
                        Some(bucket.clone()),
                        Some(key.clone()),
                        Some(mtime.clone()),
                        Some(etag[1..etag.len() - 1].to_string()),
                        Some(storage_class.clone()),
                    )),
                    _ => {}
                },
                Ok(Event::Text(e)) => {
                    if in_key_tag {
                        key = e.unescape_and_decode(&reader).unwrap();
                        in_key_tag = false;
                    }
                    if in_mtime_tag {
                        mtime = e.unescape_and_decode(&reader).unwrap();
                        in_mtime_tag = false;
                    }
                    if in_etag_tag {
                        etag = e.unescape_and_decode(&reader).unwrap();
                        in_etag_tag = false;
                    }
                    if in_storage_class_tag {
                        storage_class = e.unescape_and_decode(&reader).unwrap();
                        in_storage_class_tag = false;
                    }
                    if in_name_tag {
                        bucket = e.unescape_and_decode(&reader).unwrap();
                        in_name_tag = false;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                _ => (),
            }
            buf.clear();
        }
        Ok(output)
    }

    /// List all objects in a bucket
    pub fn la(&self) -> Result<Vec<S3Object>, &'static str> {
        let mut output = Vec::new();
        let content_re = Regex::new(RESPONSE_CONTENT_FORMAT).unwrap();
        let next_marker_re = Regex::new(RESPONSE_MARKER_FORMAT).unwrap();
        let s3_object = S3Object::from("s3://".to_string());
        let mut res = match self.auth_type {
            AuthType::AWS4 => std::str::from_utf8(
                &self
                    .aws_v4_request("GET", &s3_object, &Vec::new(), &Vec::new(), Vec::new())?
                    .0,
            )
            .unwrap_or("")
            .to_string(),
            AuthType::AWS2 => std::str::from_utf8(
                &self
                    .aws_v2_request("GET", &s3_object, &Vec::new(), &Vec::new(), &Vec::new())?
                    .0,
            )
            .unwrap_or("")
            .to_string(),
        };
        let mut buckets = Vec::new();
        match self.format {
            Format::JSON => {
                let result: serde_json::Value = serde_json::from_str(&res).unwrap();
                result[1].as_array().map(|bucket_list| {
                    buckets.extend(
                        bucket_list
                            .iter()
                            .map(|b| b["Name"].as_str().unwrap().to_string()),
                    )
                });
            }
            Format::XML => {
                buckets.extend(
                    self.object_list_xml_parser(&res)?
                        .iter()
                        .map(|o| o.bucket.clone().unwrap()),
                );
            }
        }
        for bucket in buckets {
            let s3_object = S3Object::from(format!("s3://{}", bucket));
            let mut next_marker = Some("".to_string());
            while next_marker.is_some() {
                match self.auth_type {
                    AuthType::AWS4 => {
                        res = std::str::from_utf8(
                            &self
                                .aws_v4_request(
                                    "GET",
                                    &s3_object,
                                    &vec![("marker", &next_marker.clone().unwrap())],
                                    &Vec::new(),
                                    Vec::new(),
                                )?
                                .0,
                        )
                        .unwrap_or("")
                        .to_string();

                        match self.format {
                            Format::JSON => {
                                next_marker = match next_marker_re.captures_iter(&res).nth(0) {
                                    Some(c) => Some(c[1].to_string()),
                                    None => None,
                                };
                                output.extend(content_re.captures_iter(&res).map(|cap| {
                                    S3Convert::new(
                                        Some(bucket.clone()),
                                        Some(cap[1].to_string()),
                                        Some(cap[2].to_string()),
                                        Some(cap[3].to_string()),
                                        Some(cap[5].to_string()),
                                    )
                                }));
                            }
                            Format::XML => {
                                next_marker = self.next_marker_xml_parser(&res);
                                output.extend(self.object_list_xml_parser(&res)?);
                            }
                        }
                    }
                    AuthType::AWS2 => {
                        res = std::str::from_utf8(
                            &self
                                .aws_v2_request(
                                    "GET",
                                    &s3_object,
                                    &vec![("marker", &next_marker.clone().unwrap())],
                                    &Vec::new(),
                                    &Vec::new(),
                                )?
                                .0,
                        )
                        .unwrap_or("")
                        .to_string();
                        match self.format {
                            Format::JSON => {
                                next_marker = match next_marker_re.captures_iter(&res).nth(0) {
                                    Some(c) => Some(c[1].to_string()),
                                    None => None,
                                };
                                output.extend(content_re.captures_iter(&res).map(|cap| {
                                    S3Convert::new(
                                        Some(bucket.clone()),
                                        Some(cap[1].to_string()),
                                        Some(cap[2].to_string()),
                                        Some(cap[3].to_string()),
                                        Some(cap[5].to_string()),
                                    )
                                }));
                            }
                            Format::XML => {
                                next_marker = self.next_marker_xml_parser(&res);
                                output.extend(self.object_list_xml_parser(&res)?);
                            }
                        }
                    }
                }
            }
        }
        Ok(output)
    }

    /// List all bucket of an account
    /// List all object of an bucket
    pub fn ls(&self, prefix: Option<&str>) -> Result<Vec<S3Object>, &'static str> {
        let mut output = Vec::new();
        let mut res: String;
        let s3_object = S3Object::from(prefix.unwrap_or("s3://").to_string());
        let s3_bucket = S3Object::new(s3_object.bucket, None, None, None, None);
        match s3_bucket.bucket.clone() {
            Some(b) => {
                let re = Regex::new(RESPONSE_CONTENT_FORMAT).unwrap();
                let next_marker_re = Regex::new(RESPONSE_MARKER_FORMAT).unwrap();
                let mut next_marker = Some("".to_string());
                while next_marker.is_some() {
                    match self.auth_type {
                        AuthType::AWS4 => {
                            res = std::str::from_utf8(
                                &self
                                    .aws_v4_request(
                                        "GET",
                                        &s3_bucket,
                                        &vec![
                                            (
                                                "prefix",
                                                &s3_object.key.clone().unwrap_or("/".to_string())
                                                    [1..],
                                            ),
                                            ("marker", &next_marker.clone().unwrap()),
                                        ],
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
                                    .aws_v2_request(
                                        "GET",
                                        &s3_bucket,
                                        &vec![
                                            (
                                                "prefix",
                                                &s3_object.key.clone().unwrap_or("/".to_string())
                                                    [1..],
                                            ),
                                            ("marker", &next_marker.clone().unwrap()),
                                        ],
                                        &Vec::new(),
                                        &Vec::new(),
                                    )?
                                    .0,
                            )
                            .unwrap_or("")
                            .to_string();
                        }
                    }
                    match self.format {
                        Format::JSON => {
                            next_marker = match next_marker_re.captures_iter(&res).nth(0) {
                                Some(c) => Some(c[1].to_string()),
                                None => None,
                            };
                            output.extend(re.captures_iter(&res).map(|cap| {
                                S3Convert::new(
                                    Some(b.to_string()),
                                    Some(cap[1].to_string()),
                                    Some(cap[2].to_string()),
                                    Some(cap[3].to_string()),
                                    Some(cap[5].to_string()),
                                )
                            }));
                        }
                        Format::XML => {
                            next_marker = self.next_marker_xml_parser(&res);
                            output.extend(self.object_list_xml_parser(&res)?);
                        }
                    }
                }
            }
            None => {
                let s3_object = S3Object::from("s3://".to_string());
                match self.auth_type {
                    AuthType::AWS4 => {
                        res = std::str::from_utf8(
                            &self
                                .aws_v4_request(
                                    "GET",
                                    &s3_object,
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
                                .aws_v2_request(
                                    "GET",
                                    &s3_object,
                                    &Vec::new(),
                                    &Vec::new(),
                                    &Vec::new(),
                                )?
                                .0,
                        )
                        .unwrap_or("")
                        .to_string();
                    }
                }
                match self.format {
                    Format::JSON => {
                        let result: serde_json::Value = serde_json::from_str(&res).unwrap();
                        result[1].as_array().map(|bucket_list| {
                            output.extend(bucket_list.iter().map(|b| {
                                S3Convert::new(
                                    Some(b["Name"].as_str().unwrap().to_string()),
                                    None,
                                    None,
                                    None,
                                    None,
                                )
                            }))
                        });
                    }
                    Format::XML => {
                        output.extend(self.object_list_xml_parser(&res)?);
                    }
                }
            }
        };
        Ok(output)
    }

    /// Upload a file to a S3 bucket
    pub fn put(&self, file: &str, dest: &str) -> Result<(), &'static str> {
        // TODO: handle XCOPY
        if file == "" || dest == "" {
            return Err("please specify the file and the destiney");
        }

        let mut s3_object = S3Object::from(dest.to_string());

        let mut content: Vec<u8>;

        if s3_object.key.is_none() {
            let file_name = Path::new(file).file_name().unwrap().to_string_lossy();
            s3_object.key = Some(format!("/{}", file_name));
        }

        if !Path::new(file).exists() && file == "test" {
            // TODO: add time info in the test file
            content = vec![83, 51, 82, 83, 32, 116, 101, 115, 116, 10]; // S3RS test/n
            let _ = match self.auth_type {
                AuthType::AWS4 => {
                    self.aws_v4_request("PUT", &s3_object, &Vec::new(), &Vec::new(), content)
                }
                AuthType::AWS2 => {
                    self.aws_v2_request("PUT", &s3_object, &Vec::new(), &Vec::new(), &content)
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
                let total_part_number = file_size / 5242880 + 1;
                debug!("upload file in {} parts", total_part_number);
                let res = match self.auth_type {
                    AuthType::AWS4 => std::str::from_utf8(
                        &self
                            .aws_v4_request(
                                "POST",
                                &s3_object,
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
                                &s3_object,
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
                        let re = Regex::new(r#""UploadId":"(?P<upload_id>[^"]+)""#).unwrap();
                        let caps = re.captures(&res).expect("Upload ID missing");
                        upload_id = caps["upload_id"].to_string();
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

                let mut part = 0u64;
                let mut fin = match File::open(file) {
                    Ok(f) => f,
                    Err(_) => return Err("input file open error"),
                };
                let mut jobs = Vec::new();
                loop {
                    part += 1;

                    let mut buffer = [0; 5242880];
                    let mut tail_buffer = Vec::new();
                    if part == total_part_number {
                        match fin.read_to_end(&mut tail_buffer) {
                            Ok(_) => {}
                            Err(e) => {
                                error!("read last part of file error: {}", e);
                            }
                        };
                    } else {
                        match fin.read_exact(&mut buffer) {
                            Ok(_) => {}
                            Err(e) => {
                                error!("partial read file error: {}", e);
                            }
                        };
                    }

                    let payload = if part == total_part_number {
                        tail_buffer
                    } else {
                        buffer.to_vec()
                    };
                    jobs.push((part, payload));
                    if part * 5242880 >= file_size {
                        break;
                    }
                }
                // TODO: wait for std::task::Executor ready
                let mut content = format!("<CompleteMultipartUpload>");
                match self.auth_type {
                    AuthType::AWS4 => {
                        for job in jobs.iter().map(|t| self.aws_v4_multipart_future(
                            &s3_object,
                            upload_id.clone(),
                            t.0.to_string(),
                            t.1.clone(),
                        )){
                            let r = task::block_on(job).unwrap();
                            info!("part: {} uploaded, etag: {}", r.0, r.1);
                            content.push_str(&format!(
                                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                                r.0, r.1
                            ));
                        }
                    },
                    AuthType::AWS2 => {
                        for job in jobs.iter().map(|t| self.aws_v2_multipart_future(
                            &s3_object,
                            upload_id.clone(),
                            t.0.to_string(),
                            &t.1,
                        )){
                            let r = task::block_on(job).unwrap();
                            info!("part: {} uploaded, etag: {}", r.0, r.1);
                            content.push_str(&format!(
                                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                                r.0, r.1
                            ));
                        }
                    }
                };
                content.push_str(&format!("</CompleteMultipartUpload>"));
                let _ = match self.auth_type {
                    AuthType::AWS4 => self.aws_v4_request(
                        "POST",
                        &s3_object,
                        &vec![("uploadId", upload_id.as_str())],
                        &Vec::new(),
                        content.into_bytes(),
                    ),
                    AuthType::AWS2 => self.aws_v2_request(
                        "POST",
                        &s3_object,
                        &vec![("uploadId", upload_id.as_str())],
                        &Vec::new(),
                        &content.into_bytes(),
                    ),
                };
                info!("complete multipart");
            } else {
                content = Vec::new();
                let mut fin = match File::open(file) {
                    Ok(f) => f,
                    Err(_) => return Err("input file open error"),
                };
                let _ = fin.read_to_end(&mut content);
                let _ = match self.auth_type {
                    AuthType::AWS4 => {
                        self.aws_v4_request("PUT", &s3_object, &Vec::new(), &Vec::new(), content)
                    }
                    AuthType::AWS2 => {
                        self.aws_v2_request("PUT", &s3_object, &Vec::new(), &Vec::new(), &content)
                    }
                };
            };
        }
        Ok(())
    }

    /// Download an object from S3 service
    pub fn get(&self, src: &str, file: Option<&str>) -> Result<(), &'static str> {
        let s3_object = S3Object::from(src.to_string());
        if s3_object.key.is_none() {
            return Err("Please specific the object");
        }

        let fout = match file {
            Some(fname) => fname,
            None => Path::new(src)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap_or("s3download"),
        };

        match self.auth_type {
            AuthType::AWS4 => {
                match write(
                    fout,
                    self.aws_v4_request("GET", &s3_object, &Vec::new(), &Vec::new(), Vec::new())?
                        .0,
                ) {
                    Ok(_) => return Ok(()),
                    Err(_) => return Err("write file error"), //XXX
                }
            }
            AuthType::AWS2 => {
                match write(
                    fout,
                    self.aws_v2_request("GET", &s3_object, &Vec::new(), &Vec::new(), &Vec::new())?
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
        let s3_object = S3Object::from(src.to_string());
        if s3_object.key.is_none() {
            return Err("Please specific the object");
        }

        match self.auth_type {
            AuthType::AWS4 => {
                match self.aws_v4_request("GET", &s3_object, &Vec::new(), &Vec::new(), Vec::new()) {
                    Ok(r) => {
                        println!("{}", std::str::from_utf8(&r.0).unwrap_or(""));
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                }
            }
            AuthType::AWS2 => {
                match self.aws_v2_request("GET", &s3_object, &Vec::new(), &Vec::new(), &Vec::new())
                {
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
        let s3_object = S3Object::from(src.to_string());
        if s3_object.key.is_none() {
            return Err("Please specific the object");
        }
        let _ = match self.auth_type {
            AuthType::AWS4 => {
                self.aws_v4_request("DELETE", &s3_object, &Vec::new(), headers, Vec::new())
            }
            AuthType::AWS2 => {
                self.aws_v2_request("GET", &s3_object, &Vec::new(), headers, &Vec::new())
            }
        };
        Ok(())
    }

    /// Delete an object
    pub fn del(&self, src: &str) -> Result<(), &'static str> {
        self.del_with_flag(src, &Vec::new())
    }

    /// Make a new bucket
    pub fn mb(&self, bucket: &str) -> Result<(), &'static str> {
        let s3_object = S3Object::from(bucket.to_string());
        if s3_object.bucket.is_none() {
            return Err("please specific the bucket name");
        }
        let _ = match self.auth_type {
            AuthType::AWS4 => {
                self.aws_v4_request("PUT", &s3_object, &Vec::new(), &Vec::new(), Vec::new())
            }
            AuthType::AWS2 => {
                self.aws_v2_request("PUT", &s3_object, &Vec::new(), &Vec::new(), &Vec::new())
            }
        };
        Ok(())
    }

    /// Remove a bucket
    pub fn rb(&self, bucket: &str) -> Result<(), &'static str> {
        let s3_object = S3Object::from(bucket.to_string());
        if s3_object.bucket.is_none() {
            return Err("please specific the bucket name");
        }
        let _ = match self.auth_type {
            AuthType::AWS4 => {
                self.aws_v4_request("DELETE", &s3_object, &Vec::new(), &Vec::new(), Vec::new())
            }
            AuthType::AWS2 => {
                self.aws_v2_request("DELETE", &s3_object, &Vec::new(), &Vec::new(), &Vec::new())
            }
        };
        Ok(())
    }

    /// list all tags of an object
    pub fn list_tag(&self, target: &str) -> Result<(), &'static str> {
        let res: String;
        debug!("target: {:?}", target);
        let s3_object = S3Object::from(target.to_string());
        if s3_object.key.is_none() {
            return Err("Please specific the object");
        }
        let query_string = vec![("tagging", "")];
        res = match self.auth_type {
            AuthType::AWS4 => std::str::from_utf8(
                &self
                    .aws_v4_request("GET", &s3_object, &query_string, &Vec::new(), Vec::new())?
                    .0,
            )
            .unwrap_or("")
            .to_string(),
            AuthType::AWS2 => std::str::from_utf8(
                &self
                    .aws_v2_request("GET", &s3_object, &query_string, &Vec::new(), &Vec::new())?
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
        let s3_object = S3Object::from(target.to_string());
        if s3_object.key.is_none() {
            return Err("Please specific the object");
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
                &s3_object,
                &query_string,
                &Vec::new(),
                content.into_bytes(),
            ),
            AuthType::AWS2 => self.aws_v2_request(
                "PUT",
                &s3_object,
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
        let s3_object = S3Object::from(target.to_string());
        if s3_object.key.is_none() {
            return Err("Please specific the object");
        }
        let query_string = vec![("tagging", "")];
        let _ = match self.auth_type {
            AuthType::AWS4 => {
                self.aws_v4_request("DELETE", &s3_object, &query_string, &Vec::new(), Vec::new())
            }
            AuthType::AWS2 => self.aws_v2_request(
                "DELETE",
                &s3_object,
                &query_string,
                &Vec::new(),
                &Vec::new(),
            ),
        };
        Ok(())
    }

    /// Show the usage of a bucket (CEPH only)
    pub fn usage(&self, target: &str, options: &Vec<(&str, &str)>) -> Result<(), &'static str> {
        let s3_admin_bucket_object = S3Convert::new_from_uri("/admin/buckets".to_string());
        let s3_object = S3Object::from(target.to_string());
        let mut query_strings = options.clone();
        if s3_object.bucket.is_none() {
            return Err("S3 format error.");
        };
        let bucket = s3_object.bucket.unwrap();
        query_strings.push(("bucket", &bucket));
        let result = match self.auth_type {
            AuthType::AWS4 => self.aws_v4_request(
                "GET",
                &s3_admin_bucket_object,
                &query_strings,
                &Vec::new(),
                Vec::new(),
            )?,
            AuthType::AWS2 => self.aws_v2_request(
                "GET",
                &s3_admin_bucket_object,
                &query_strings,
                &Vec::new(),
                &Vec::new(),
            )?,
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
        let s3_object;
        let mut raw_qs = String::new();
        let mut query_strings = Vec::new();
        match url.find('?') {
            Some(idx) => {
                s3_object = S3Object::from(url[..idx].to_string());
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
                s3_object = S3Object::from(url.to_string());
            }
        }

        let result = match self.auth_type {
            AuthType::AWS4 => {
                self.aws_v4_request("GET", &s3_object, &query_strings, &Vec::new(), Vec::new())?
            }
            AuthType::AWS2 => {
                self.aws_v2_request("GET", &s3_object, &query_strings, &Vec::new(), &Vec::new())?
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
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_s3object_for_dummy_folder() {
        let s3_object = S3Object::from("s3://bucket/dummy_folder/".to_string());
        assert_eq!(s3_object.bucket, Some("bucket".to_string()));
        assert_eq!(s3_object.key, Some("/dummy_folder/".to_string()));
        assert_eq!(
            "s3://bucket/dummy_folder/".to_string(),
            String::from(s3_object)
        );
    }
    #[test]
    fn test_s3object_for_bucket() {
        let s3_object = S3Object::from("s3://bucket".to_string());
        assert_eq!(s3_object.bucket, Some("bucket".to_string()));
        assert_eq!(s3_object.key, None);
        assert_eq!("s3://bucket".to_string(), String::from(s3_object));
    }
    #[test]
    fn test_s3object_for_dummy_folder_from_uri() {
        let s3_object: S3Object = S3Convert::new_from_uri("/bucket/dummy_folder/".to_string());
        assert_eq!(
            "s3://bucket/dummy_folder/".to_string(),
            String::from(s3_object)
        );
    }
    #[test]
    fn test_s3object_for_root() {
        let s3_object = S3Object::from("s3://".to_string());
        assert_eq!(s3_object.bucket, None);
        assert_eq!(s3_object.key, None);
    }
    #[test]
    fn test_s3object_for_bucket_from_uri() {
        let s3_object: S3Object = S3Convert::new_from_uri("/bucket".to_string());
        assert_eq!("s3://bucket".to_string(), String::from(s3_object));
    }
    #[test]
    fn test_s3object_for_slash_end_bucket_from_uri() {
        let s3_object: S3Object = S3Convert::new_from_uri("/bucket/".to_string());
        assert_eq!("s3://bucket".to_string(), String::from(s3_object));
    }
    #[test]
    fn test_s3object_for_bucket_from_bucket_name() {
        let s3_object: S3Object = S3Convert::new_from_uri("bucket".to_string());
        assert_eq!("s3://bucket".to_string(), String::from(s3_object));
    }
}
