//! Initilize S3 handler to manipulate objects and buckets
//! ```
//! let config = s3handler::CredentialConfig{
//!     host: "s3.us-east-1.amazonaws.com".to_string(),
//!     access_key: "akey".to_string(),
//!     secret_key: "skey".to_string(),
//!     user: None,
//!     region: None, // default is us-east-1
//!     s3_type: None, // default will try to config as AWS S3 handler
//!     secure: None, // dafault is false, because the integrity protect by HMAC
//! };
//! let mut handler = s3handler::Handler::from(&config);
//! let _ = handler.la();
//! ```

use std::cmp;
use std::convert::From;
use std::fs::{metadata, write, File};
use std::io::prelude::*;
use std::path::Path;
use std::str::FromStr;

use crate::error::Error;
pub use crate::utils::UrlStyle;
use aws::{AWS2Client, AWS4Client};
use download_pool::{DownloadRequestPool, MultiDownloadParameters};
use upload_pool::{MultiUploadParameters, UploadRequestPool};

use crate::utils::{
    s3object_list_xml_parser, upload_id_xml_parser, S3Convert, S3Object, DEFAULT_REGION,
};
use log::{debug, error, info};
use mime_guess::from_path;
use quick_xml::{events::Event, Reader};
use regex::Regex;
use reqwest::{blocking::Response, StatusCode};
use serde_derive::Deserialize;

pub mod aws;
mod download_pool;
mod upload_pool;

static RESPONSE_CONTENT_FORMAT: &str =
    r#""Contents":\["([^"]+?)","([^"]+?)","\\"([^"]+?)\\"",([^"]+?),"([^"]+?)"(.*?)\]"#;
static RESPONSE_MARKER_FORMAT: &str = r#""NextMarker":"([^"]+?)","#;
static DEFAULT_PREPART_SIZE: u64 = 5242880;

/// # The struct for credential config for each S3 cluster
/// - host is a parameter for the server you want to link
///     - it can be s3.us-east-1.amazonaws.com or a ip, ex 10.1.1.100, for a ceph node
/// - user name is not required, because it only show in the prompt of shell
/// - access_key and secret_key are keys to connect to the cluster providing S3
/// - region is a paramter for the S3 cluster location
///     - if region is not specified, it will take default value us-east-1
/// - s3 type is a shortcut to set up auth type, format, url style for aws or ceph
///     - if s3_type is not specified, it will take aws as default value, aws
/// - secure is the request will send via https or not.  The integrity of requests is provided by
/// HMAC, and the https requests can provid the confidentiality.
///
#[derive(Debug, Clone, Deserialize)]
pub struct CredentialConfig {
    pub host: String,
    pub user: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    pub region: Option<String>,
    pub s3_type: Option<String>,
    pub secure: Option<bool>,
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
#[derive(Copy, Clone)]
pub enum AuthType {
    AWS4,
    AWS2,
}

/// # The response format
/// AWS only support XML format (default)
/// CEPH support JSON and XML
#[derive(Clone)]
pub enum Format {
    JSON,
    XML,
}

/// # The trait for S3Client
/// - handle a valid request
pub(crate) trait S3Client {
    fn request(
        &self,
        method: &str,
        host: &str,
        uri: &str,

        // TODO: refact these into HashMap and break api
        query_strings: &mut Vec<(&str, &str)>,
        headers: &mut Vec<(&str, &str)>,

        payload: &[u8],
    ) -> Result<(StatusCode, Vec<u8>, reqwest::header::HeaderMap), Error>;

    fn redirect_parser(&self, body: Vec<u8>, format: Format) -> Result<String, Error>;
    fn update(&mut self, region: String, secure: bool);
    fn current_region(&self) -> Option<String>;
}

/// # The struct for generate the request
/// - host is a parameter for the server you want to link
///     - it can be s3.us-east-1.amazonaws.com or a ip, ex 10.1.1.100, for a ceph node
/// - auth_type specify the signature version of S3
/// - format specify the s3 response from server
/// - url_style specify the s3 request url style
/// - region is a paramter for the S3 cluster location
///     - if region is not specified, it will take default value us-east-1
/// - handle redirect
/// It can be init from the config structure, for example:
/// ```
/// let config = s3handler::CredentialConfig{
///     host: "s3.us-east-1.amazonaws.com".to_string(),
///     access_key: "akey".to_string(),
///     secret_key: "skey".to_string(),
///     user: None,
///     region: None, // default is us-east-1
///     s3_type: None, // default will try to config as AWS S3 handler
///     secure: None, // dafault is false, because the integrity protect by HMAC
/// };
/// let mut handler = s3handler::Handler::from(&config);
/// ```
pub struct Handler<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub host: &'a str,

    s3_client: Box<dyn S3Client + 'a>,
    pub auth_type: AuthType,
    pub format: Format,
    pub url_style: UrlStyle,
    pub region: Option<String>,

    // redirect related paramters
    domain_name: String,

    // https for switch s3_client
    secure: bool,

    // The chunck size for multipart
    part_size: u64,
}

trait ResponseHandler {
    fn handle_response(&mut self) -> (StatusCode, Vec<u8>, reqwest::header::HeaderMap);
}

impl ResponseHandler for Response {
    fn handle_response(&mut self) -> (StatusCode, Vec<u8>, reqwest::header::HeaderMap) {
        let body: Vec<u8> = self.bytes().map(|b| b.unwrap_or_default()).collect();
        if self.status().is_success() || self.status().is_redirection() {
            info!("Status: {}", self.status());
            info!("Headers:\n{:?}", self.headers());
            info!(
                "Body:\n{}\n\n",
                std::str::from_utf8(&body).unwrap_or_default()
            );
        } else {
            error!("Status: {}", self.status());
            error!("Headers:\n{:?}", self.headers());
            error!(
                "Body:\n{}\n\n",
                std::str::from_utf8(&body).unwrap_or_default()
            );
        }
        (self.status(), body, self.headers().clone())
    }
}

impl Handler<'_> {
    pub fn is_secure(&self) -> bool {
        self.secure
    }

    fn request(
        &mut self,
        method: &str,
        s3_object: &S3Object,
        qs: &[(&str, &str)],
        headers: &mut Vec<(&str, &str)>,
        payload: &[u8],
    ) -> Result<(Vec<u8>, reqwest::header::HeaderMap), Error> {
        let mut query_strings = vec![];
        if let Format::JSON = self.format {
            query_strings.push(("format", "json"));
        }
        query_strings.extend(qs.iter().cloned());

        let (request_host, uri) = match self.url_style {
            UrlStyle::HOST => s3_object.virtural_host_style_links(self.domain_name.to_string()),
            UrlStyle::PATH => s3_object.path_style_links(self.domain_name.to_string()),
        };

        debug!("method: {}", method);
        debug!("request_host: {}", request_host);
        debug!("uri: {}", uri);

        let (status_code, body, response_headers) = self.s3_client.request(
            method,
            &request_host,
            &uri,
            &mut query_strings,
            headers,
            payload,
        )?;
        match status_code.is_redirection() {
            true => {
                self.region = Some(
                    response_headers["x-amz-bucket-region"]
                        .to_str()
                        .unwrap_or("")
                        .to_string(),
                );
                // TODO: This should be better
                // Change the region and request once
                let origin_region = self.s3_client.current_region();
                self.s3_client
                    .update(self.region.clone().unwrap(), self.secure);
                let (_status_code, body, response_headers) = self.s3_client.request(
                    method,
                    &self.s3_client.redirect_parser(body, self.format.clone())?,
                    &uri,
                    &mut query_strings,
                    headers,
                    payload,
                )?;
                self.s3_client.update(origin_region.unwrap(), self.secure);
                Ok((body, response_headers))
            }
            false => Ok((body, response_headers)),
        }
    }
    fn next_marker_xml_parser(&self, body: &str) -> Option<String> {
        // let result = std::str::from_utf8(body).unwrap_or("");
        let mut reader = Reader::from_str(body);
        let mut in_tag = false;
        let mut buf = Vec::new();
        let mut output = "".to_string();
        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) if e.name() == b"NextMarker" => {
                    in_tag = true;
                }
                Ok(Event::End(ref _e)) => {}
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

    /// List all objects in a bucket
    pub fn la(&mut self) -> Result<Vec<S3Object>, Box<dyn std::error::Error>> {
        let mut output = Vec::new();
        let content_re = Regex::new(RESPONSE_CONTENT_FORMAT).unwrap();
        let next_marker_re = Regex::new(RESPONSE_MARKER_FORMAT).unwrap();
        let s3_object = S3Object::from("s3://");
        let res = &self
            .request("GET", &s3_object, &Vec::new(), &mut Vec::new(), &Vec::new())?
            .0;
        let mut buckets = Vec::new();
        match self.format {
            Format::JSON => {
                let result: serde_json::Value = serde_json::from_slice(res).unwrap();
                if let Some(bucket_list) = result[1].as_array() {
                    buckets.extend(
                        bucket_list
                            .iter()
                            .map(|b| b["Name"].as_str().unwrap().to_string()),
                    )
                };
            }
            Format::XML => {
                // TODO handle is_truncated
                buckets.extend(
                    s3object_list_xml_parser(std::str::from_utf8(res).unwrap_or(""))?
                        .0
                        .iter()
                        .map(|o| o.bucket.clone().unwrap()),
                );
            }
        }
        for bucket in buckets {
            let s3_object = S3Object::from(format!("s3://{}", bucket).as_str());
            let mut next_marker = Some("".to_string());
            while next_marker.is_some() {
                let body = &self
                    .request(
                        "GET",
                        &s3_object,
                        &[("marker", &next_marker.clone().unwrap())],
                        &mut Vec::new(),
                        &Vec::new(),
                    )?
                    .0;

                match self.format {
                    Format::JSON => {
                        next_marker = next_marker_re
                            .captures_iter(std::str::from_utf8(body).unwrap_or(""))
                            .next()
                            .map(|c| c[1].to_string());
                        output.extend(
                            content_re
                                .captures_iter(std::str::from_utf8(body).unwrap_or(""))
                                .map(|cap| {
                                    S3Convert::new(
                                        Some(bucket.clone()),
                                        Some(cap[1].to_string()),
                                        Some(cap[2].to_string()),
                                        Some(cap[3].to_string()),
                                        Some(cap[5].to_string()),
                                        None, // TODO: test with cech
                                    )
                                }),
                        );
                    }
                    Format::XML => {
                        next_marker =
                            self.next_marker_xml_parser(std::str::from_utf8(body).unwrap_or(""));
                        output.extend(
                            s3object_list_xml_parser(std::str::from_utf8(body).unwrap_or(""))?.0,
                        );
                    }
                }
            }
        }
        Ok(output)
    }

    /// List all bucket of an account or List all object of an bucket
    pub fn ls(
        &mut self,
        prefix: Option<&str>,
    ) -> Result<Vec<S3Object>, Box<dyn std::error::Error>> {
        let mut output = Vec::new();
        let mut res: String;
        let s3_object = S3Object::from(prefix.unwrap_or("s3://"));
        let s3_bucket = S3Object::new(s3_object.bucket, None, None, None, None, None);
        match s3_bucket.bucket.clone() {
            Some(b) => {
                let re = Regex::new(RESPONSE_CONTENT_FORMAT).unwrap();
                let next_marker_re = Regex::new(RESPONSE_MARKER_FORMAT).unwrap();
                let mut next_marker = Some("".to_string());
                while next_marker.is_some() {
                    res = std::str::from_utf8(
                        &self
                            .request(
                                "GET",
                                &s3_bucket,
                                &[
                                    (
                                        "prefix",
                                        &s3_object.key.clone().unwrap_or_else(|| "/".to_string())
                                            [1..],
                                    ),
                                    ("marker", &next_marker.clone().unwrap()),
                                ],
                                &mut Vec::new(),
                                &Vec::new(),
                            )?
                            .0,
                    )
                    .unwrap_or("")
                    .to_string();
                    match self.format {
                        Format::JSON => {
                            next_marker_re
                                .captures_iter(&res)
                                .next()
                                .map(|c| c[1].to_string());
                            output.extend(re.captures_iter(&res).map(|cap| {
                                S3Convert::new(
                                    Some(b.to_string()),
                                    Some(cap[1].to_string()),
                                    Some(cap[2].to_string()),
                                    Some(cap[3].to_string()),
                                    Some(cap[5].to_string()),
                                    None, // TODO: test with ceph server
                                )
                            }));
                        }
                        Format::XML => {
                            next_marker = self.next_marker_xml_parser(&res);
                            output.extend(s3object_list_xml_parser(&res)?.0);
                        }
                    }
                }
            }
            None => {
                let s3_object = S3Object::from("s3://");
                let body = &self
                    .request("GET", &s3_object, &Vec::new(), &mut Vec::new(), &Vec::new())?
                    .0;
                match self.format {
                    Format::JSON => {
                        let result: serde_json::Value =
                            serde_json::from_str(std::str::from_utf8(body).unwrap_or("")).unwrap();
                        if let Some(bucket_list) = result[1].as_array() {
                            output.extend(bucket_list.iter().map(|b| {
                                S3Convert::new(
                                    Some(b["Name"].as_str().unwrap().to_string()),
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                )
                            }))
                        };
                    }
                    Format::XML => {
                        output.extend(
                            s3object_list_xml_parser(std::str::from_utf8(body).unwrap_or(""))?.0,
                        );
                    }
                }
            }
        };
        Ok(output)
    }

    fn multipart_uplodad(
        &mut self,
        file: &str,
        file_size: u64,
        s3_object: S3Object,
        headers: Vec<(&str, &str)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let total_part_number = (file_size / self.part_size + 1) as usize;
        debug!("upload file in {} parts", total_part_number);
        let res = std::str::from_utf8(
            &self
                .request(
                    "POST",
                    &s3_object,
                    &[("uploads", "")],
                    &mut headers.clone(),
                    &Vec::new(),
                )?
                .0,
        )
        .unwrap_or("")
        .to_string();
        let upload_id = match self.format {
            Format::JSON => {
                let re = Regex::new(r#""UploadId":"(?P<upload_id>[^"]+)""#).unwrap();
                let caps = re.captures(&res).expect("Upload ID missing");
                caps["upload_id"].to_string()
            }
            Format::XML => upload_id_xml_parser(&res)?,
        };

        info!("upload id: {}", upload_id);

        let mut part = 0usize;
        let mut fin = File::open(file)?;
        // Once we have retry mechanism in workers, we can make this bigger
        // Magic number, I do not tune on this currently
        let worker_number = cmp::min(10, total_part_number);
        info!(
            "{} part and {} workers to upload",
            total_part_number, worker_number
        );
        let (host, uri) = match self.url_style {
            UrlStyle::HOST => s3_object.virtural_host_style_links(self.domain_name.to_string()),
            UrlStyle::PATH => s3_object.path_style_links(self.domain_name.to_string()),
        };
        let mut rp = UploadRequestPool::new(
            self.auth_type,
            self.secure,
            self.access_key.to_string(),
            self.secret_key.to_string(),
            host,
            uri,
            self.region.clone().unwrap_or_else(|| "".to_string()),
            upload_id.clone(),
            worker_number,
        );
        loop {
            part += 1;

            let mut buffer = vec![0; self.part_size as usize];
            let mut tail_buffer = Vec::new();
            if part == total_part_number {
                fin.read_to_end(&mut tail_buffer)?;
            } else {
                fin.read_exact(&mut buffer)?
            }

            if part == total_part_number {
                rp.run(MultiUploadParameters {
                    part_number: part,
                    payload: tail_buffer,
                });
            } else {
                rp.run(MultiUploadParameters {
                    part_number: part,
                    payload: buffer.to_vec().clone(),
                });
            };
            if part as u64 * self.part_size >= file_size {
                break;
            }
        }

        let content = rp.wait()?;
        let _ = self.request(
            "POST",
            &s3_object,
            &[("uploadId", upload_id.as_str())],
            &mut headers.clone(),
            &content.into_bytes(),
        )?;
        info!("complete multipart");
        Ok(())
    }

    /// Upload a file to a S3 bucket
    pub fn put(&mut self, file: &str, dest: &str) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: handle XCOPY
        if file.is_empty() || dest.is_empty() {
            return Err(Error::UserError("please specify the file and the destiney").into());
        }

        let mut s3_object = S3Object::from(dest);

        let mut content: Vec<u8>;

        let gusess_mime = from_path(Path::new(file)).first_raw();
        let mut headers = if let Some(mime) = gusess_mime {
            vec![(reqwest::header::CONTENT_TYPE.as_str(), mime)]
        } else {
            Vec::new()
        };

        if s3_object.key.is_none() {
            let file_name = Path::new(file).file_name().unwrap().to_string_lossy();
            s3_object.key = Some(format!("/{}", file_name));
        }

        if !Path::new(file).exists() && file == "test" {
            // TODO: add time info in the test file
            content = vec![83, 51, 82, 83, 32, 116, 101, 115, 116, 10]; // S3RS test/n
            let _ = self.request(
                "PUT",
                &s3_object,
                &Vec::new(),
                &mut vec![(reqwest::header::CONTENT_TYPE.as_str(), "text/plain")],
                &content,
            );
        } else {
            let file_size = match metadata(Path::new(file)) {
                Ok(m) => m.len(),
                Err(e) => {
                    error!("file meta error: {}", e);
                    0
                }
            };

            debug!("upload file size: {}", file_size);
            if file_size > self.part_size {
                self.multipart_uplodad(file, file_size, s3_object, headers)?;
            } else {
                content = Vec::new();
                let mut fin = File::open(file)?;
                let _ = fin.read_to_end(&mut content);
                let _ = self.request("PUT", &s3_object, &Vec::new(), &mut headers, &content)?;
            };
        }
        Ok(())
    }

    /// Download an object from S3 service
    pub fn get(&mut self, src: &str, file: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
        let s3_object = S3Object::from(src);
        if s3_object.key.is_none() {
            return Err(Error::UserError("Please specific the object").into());
        }

        let fout = match file {
            Some(fname) => fname,
            None => Path::new(src)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap_or("s3download"),
        };
        // TODO fetch size then multipart
        let headers = self
            .request(
                "HEAD",
                &s3_object,
                &Vec::new(),
                &mut Vec::new(),
                &Vec::new(),
            )?
            .1;
        let size = if headers.contains_key(reqwest::header::CONTENT_LENGTH) {
            headers[reqwest::header::CONTENT_LENGTH]
                .to_str()?
                .parse::<u64>()
                .unwrap_or_default()
        } else {
            0
        };

        let data = if size > 0 && size > self.part_size {
            let total_part_number = (size / self.part_size + 1) as usize;
            let worker_number = cmp::min(10, total_part_number);
            let (host, uri) = match self.url_style {
                UrlStyle::HOST => s3_object.virtural_host_style_links(self.domain_name.to_string()),
                UrlStyle::PATH => s3_object.path_style_links(self.domain_name.to_string()),
            };
            let mut dp = DownloadRequestPool::new(
                self.auth_type,
                self.secure,
                self.access_key.to_string(),
                self.secret_key.to_string(),
                host,
                uri,
                self.region.clone().unwrap_or_else(|| "".to_string()),
                size as usize,
                worker_number,
            );
            let mut part = 0;
            while part * self.part_size < size {
                let end = cmp::min(size, (part + 1) * self.part_size) as usize;
                let start = (part * self.part_size) as usize;
                dp.run(MultiDownloadParameters(start, end));
                part += 1;
            }
            dp.wait()?
        } else {
            self.request("GET", &s3_object, &Vec::new(), &mut Vec::new(), &Vec::new())?
                .0
        };
        write(fout, data)?;

        Ok(())
    }

    /// Show the content and the content type of an object
    pub fn cat(
        &mut self,
        src: &str,
    ) -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
        let s3_object = S3Object::from(src);
        if s3_object.key.is_none() {
            return Err(Error::UserError("Please specific the object").into());
        }
        let (output, content_type) = self
            .request("GET", &s3_object, &Vec::new(), &mut Vec::new(), &Vec::new())
            .map(|r| {
                (
                    std::str::from_utf8(&r.0).unwrap_or("").to_string(),
                    r.1.get(reqwest::header::CONTENT_TYPE)
                        .and_then(|v| std::str::from_utf8(v.as_bytes()).ok())
                        .map(|s| s.to_string()),
                )
            })?;
        Ok((output, content_type))
    }

    /// Delete with header flags for some deletion features
    /// - AWS - delete-marker
    /// - Bigtera - secure-delete
    pub fn del_with_flag(
        &mut self,
        src: &str,
        headers: &mut Vec<(&str, &str)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("headers: {:?}", headers);
        let s3_object = S3Object::from(src);
        if s3_object.key.is_none() {
            return Err(Error::UserError("Please specific the object").into());
        }
        self.request("DELETE", &s3_object, &Vec::new(), headers, &Vec::new())?;
        Ok(())
    }

    /// Delete an object
    pub fn del(&mut self, src: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.del_with_flag(src, &mut Vec::new())
    }

    /// Make a new bucket
    pub fn mb(&mut self, bucket: &str) -> Result<(), Box<dyn std::error::Error>> {
        let s3_object = S3Object::from(bucket);
        if s3_object.bucket.is_none() {
            return Err(Error::UserError("please specific the bucket name").into());
        }
        self.request("PUT", &s3_object, &Vec::new(), &mut Vec::new(), &Vec::new())?;
        Ok(())
    }

    /// Remove a bucket
    pub fn rb(&mut self, bucket: &str) -> Result<(), Box<dyn std::error::Error>> {
        let s3_object = S3Object::from(bucket);
        if s3_object.bucket.is_none() {
            return Err(Error::UserError("please specific the bucket name").into());
        }
        self.request(
            "DELETE",
            &s3_object,
            &Vec::new(),
            &mut Vec::new(),
            &Vec::new(),
        )?;
        Ok(())
    }

    /// list all tags of an object
    pub fn list_tag(&mut self, target: &str) -> Result<(), Box<dyn std::error::Error>> {
        let res: String;
        debug!("target: {:?}", target);
        let s3_object = S3Object::from(target);
        if s3_object.key.is_none() {
            return Err(Error::UserError("Please specific the object").into());
        }
        let query_string = vec![("tagging", "")];
        res = std::str::from_utf8(
            &self
                .request(
                    "GET",
                    &s3_object,
                    &query_string,
                    &mut Vec::new(),
                    &Vec::new(),
                )?
                .0,
        )
        .unwrap_or("")
        .to_string();
        // TODO:
        // parse tagging output when CEPH tagging json format respose bug fixed
        println!("{}", res);
        Ok(())
    }

    /// Put a tag on an object
    pub fn add_tag(
        &mut self,
        target: &str,
        tags: &[(&str, &str)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("target: {:?}", target);
        debug!("tags: {:?}", tags);
        let s3_object = S3Object::from(target);
        if s3_object.key.is_none() {
            return Err(Error::UserError("Please specific the object").into());
        }
        let mut content = "<Tagging><TagSet>".to_string();
        for tag in tags {
            content.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                tag.0, tag.1
            ));
        }
        content.push_str(&"</TagSet></Tagging>".to_string());
        debug!("payload: {:?}", content);

        let query_string = vec![("tagging", "")];
        self.request(
            "PUT",
            &s3_object,
            &query_string,
            &mut Vec::new(),
            &content.into_bytes(),
        )?;
        Ok(())
    }

    /// Remove aa tag from an object
    pub fn del_tag(&mut self, target: &str) -> Result<(), Box<dyn std::error::Error>> {
        debug!("target: {:?}", target);
        let s3_object = S3Object::from(target);
        if s3_object.key.is_none() {
            return Err(Error::UserError("Please specific the object").into());
        }
        let query_string = vec![("tagging", "")];
        self.request(
            "DELETE",
            &s3_object,
            &query_string,
            &mut Vec::new(),
            &Vec::new(),
        )?;
        Ok(())
    }

    /// Show the usage of a bucket (CEPH only)
    pub fn usage(
        &mut self,
        target: &str,
        options: &[(&str, &str)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let s3_admin_bucket_object = S3Convert::new_from_uri("/admin/buckets");
        let s3_object = S3Object::from(target);
        let mut query_strings = options.to_owned();
        if s3_object.bucket.is_none() {
            return Err(Error::UserError("S3 format not correct.").into());
        };
        let bucket = s3_object.bucket.unwrap();
        query_strings.push(("bucket", &bucket));
        let result = self.request(
            "GET",
            &s3_admin_bucket_object,
            &query_strings,
            &mut Vec::new(),
            &Vec::new(),
        )?;
        match self.format {
            Format::JSON => {
                let json: serde_json::Value;
                json = serde_json::from_str(std::str::from_utf8(&result.0).unwrap_or("")).unwrap();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json["usage"]).unwrap_or_else(|_| "".to_string())
                );
            }
            Format::XML => {
                // TODO:
                // Ceph Ops api may not support xml
                unimplemented!();
            }
        };
        Ok(())
    }

    /// Do a GET request for the specific URL
    /// This method is easily to show the configure of S3 not implemented
    pub fn url_command(&mut self, url: &str) -> Result<(), Box<dyn std::error::Error>> {
        let s3_object;
        let mut raw_qs = String::new();
        let mut query_strings = Vec::new();
        match url.find('?') {
            Some(idx) => {
                s3_object = S3Object::from(&url[..idx]);
                raw_qs.push_str(&String::from_str(&url[idx + 1..]).unwrap());
                for q_pair in raw_qs.split('&') {
                    match q_pair.find('=') {
                        Some(_) => query_strings.push((
                            q_pair.split('=').next().unwrap(),
                            q_pair.split('=').nth(1).unwrap(),
                        )),
                        None => query_strings.push((q_pair, "")),
                    }
                }
            }
            None => {
                s3_object = S3Object::from(url);
            }
        }

        let result = self.request(
            "GET",
            &s3_object,
            &query_strings,
            &mut Vec::new(),
            &Vec::new(),
        )?;
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
            self.s3_client = Box::new(AWS4Client {
                tls: self.secure,
                access_key: self.access_key,
                secret_key: self.secret_key,
                host: self.host,
                region: self.region.clone().unwrap(),
            });
            println!("using aws verion 4 signature, xml format, and host style url");
        } else if command.ends_with("ceph") {
            self.auth_type = AuthType::AWS4;
            self.format = Format::JSON;
            self.url_style = UrlStyle::PATH;
            self.s3_client = Box::new(AWS4Client {
                tls: self.secure,
                access_key: self.access_key,
                secret_key: self.secret_key,
                host: self.host,
                region: self.region.clone().unwrap(),
            });
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
            self.s3_client = Box::new(AWS2Client {
                tls: self.secure,
                access_key: self.access_key,
                secret_key: self.secret_key,
            });
            println!("using aws version 2 signature");
        } else if command.ends_with("aws4") || command.ends_with("aws") {
            self.auth_type = AuthType::AWS4;
            self.s3_client = Box::new(AWS4Client {
                tls: self.secure,
                access_key: self.access_key,
                secret_key: self.secret_key,
                host: self.host,
                region: self.region.clone().unwrap(),
            });
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
}

impl<'a> From<&'a CredentialConfig> for Handler<'a> {
    fn from(credential: &'a CredentialConfig) -> Self {
        debug!("host: {}", credential.host);
        debug!("access key: {}", credential.access_key);
        debug!("secret key: {}", credential.secret_key);

        match credential
            .clone()
            .s3_type
            .unwrap_or_else(|| "".to_string())
            .as_str()
        {
            "aws" => Handler {
                access_key: &credential.access_key,
                secret_key: &credential.secret_key,
                host: &credential.host,

                s3_client: Box::new(AWS4Client {
                    tls: credential.secure.unwrap_or(false),
                    access_key: &credential.access_key,
                    secret_key: &credential.secret_key,
                    host: &credential.host,
                    region: credential.region.clone().unwrap(),
                }),
                auth_type: AuthType::AWS4,
                format: Format::XML,
                url_style: UrlStyle::HOST,
                region: credential.region.clone(),
                secure: credential.secure.unwrap_or(false),
                domain_name: credential.host.to_string(),
                part_size: DEFAULT_PREPART_SIZE,
            },
            "ceph" => Handler {
                access_key: &credential.access_key,
                secret_key: &credential.secret_key,
                host: &credential.host,

                s3_client: Box::new(AWS4Client {
                    tls: credential.secure.unwrap_or(false),
                    access_key: &credential.access_key,
                    secret_key: &credential.secret_key,
                    host: &credential.host,
                    region: credential.region.clone().unwrap(),
                }),
                auth_type: AuthType::AWS4,
                format: Format::JSON,
                url_style: UrlStyle::PATH,
                region: credential.region.clone(),
                secure: credential.secure.unwrap_or(false),
                domain_name: credential.host.to_string(),
                part_size: DEFAULT_PREPART_SIZE,
            },
            _ => Handler {
                access_key: &credential.access_key,
                secret_key: &credential.secret_key,
                host: &credential.host,
                auth_type: AuthType::AWS4,
                format: Format::XML,
                url_style: UrlStyle::PATH,
                region: credential.region.clone(),
                secure: credential.secure.unwrap_or(false),
                domain_name: credential.host.to_string(),
                s3_client: Box::new(AWS4Client {
                    tls: credential.secure.unwrap_or(false),
                    access_key: &credential.access_key,
                    secret_key: &credential.secret_key,
                    host: &credential.host,
                    region: credential
                        .region
                        .clone()
                        .unwrap_or_else(|| DEFAULT_REGION.to_string()),
                }),
                part_size: DEFAULT_PREPART_SIZE,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_s3object_for_dummy_folder() {
        let s3_object = S3Object::from("s3://bucket/dummy_folder/");
        assert_eq!(s3_object.bucket, Some("bucket".to_string()));
        assert_eq!(s3_object.key, Some("/dummy_folder/".to_string()));
        assert_eq!(
            "s3://bucket/dummy_folder/".to_string(),
            String::from(s3_object)
        );
    }
    #[test]
    fn test_s3object_for_bucket() {
        let s3_object = S3Object::from("s3://bucket");
        assert_eq!(s3_object.bucket, Some("bucket".to_string()));
        assert_eq!(s3_object.key, None);
        assert_eq!("s3://bucket".to_string(), String::from(s3_object));
    }
    #[test]
    fn test_s3object_for_dummy_folder_from_uri() {
        let s3_object: S3Object = S3Convert::new_from_uri("/bucket/dummy_folder/");
        assert_eq!(
            "s3://bucket/dummy_folder/".to_string(),
            String::from(s3_object)
        );
    }
    #[test]
    fn test_s3object_for_root() {
        let s3_object = S3Object::from("s3://");
        assert_eq!(s3_object.bucket, None);
        assert_eq!(s3_object.key, None);
    }
    #[test]
    fn test_s3object_for_bucket_from_uri() {
        let s3_object: S3Object = S3Convert::new_from_uri("/bucket");
        assert_eq!("s3://bucket".to_string(), String::from(s3_object));
    }
    #[test]
    fn test_s3object_for_slash_end_bucket_from_uri() {
        let s3_object: S3Object = S3Convert::new_from_uri("/bucket/");
        assert_eq!("s3://bucket".to_string(), String::from(s3_object));
    }
    #[test]
    fn test_s3object_for_bucket_from_bucket_name() {
        let s3_object: S3Object = S3Convert::new_from_uri("bucket");
        assert_eq!("s3://bucket".to_string(), String::from(s3_object));
    }
}
