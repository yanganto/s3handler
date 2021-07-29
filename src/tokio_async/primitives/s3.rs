use async_trait::async_trait;
use base64::encode;
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use dyn_clone::DynClone;
use futures::future::join_all;
use hmac::{Hmac, Mac, NewMac};
use reqwest::{
    header::{self, HeaderMap, HeaderName, HeaderValue},
    Client, Method, Request, Response, Url,
};
use rustc_serialize::hex::ToHex;
use sha2::Sha256 as sha2_256;
use std::fmt;
use url::form_urlencoded;

use super::canal::{Canal, PoolType};
use crate::blocking::{AuthType, Handler};
use crate::error::Error;
use crate::tokio_async::traits::{DataPool, S3Folder};
use crate::utils::{
    s3object_list_xml_parser, upload_id_xml_parser, S3Convert, S3Object, UrlStyle, DEFAULT_REGION,
};

type UTCTime = DateTime<Utc>;

pub trait Authorizer: Send + Sync + DynClone + fmt::Debug {
    /// This method will setup the header and put the authorize string
    fn authorize(&self, _request: &mut Request, _now: &UTCTime) {
        unimplemented!()
    }

    /// This method will be called once the resource change the region stored
    fn update_region(&mut self, _region: String) {}
}

dyn_clone::clone_trait_object!(Authorizer);

#[derive(Clone, Debug)]
pub struct PublicAuthorizer {}

impl Authorizer for PublicAuthorizer {
    fn authorize(&self, _requests: &mut Request, _now: &UTCTime) {}
}

#[derive(Clone, Debug)]
pub struct V2Authorizer {
    pub access_key: String,
    pub secret_key: String,
    pub auth_str: String,
    pub special_header_prefix: String,
}

#[allow(dead_code)]
impl V2Authorizer {
    /// new V2 Authorizer compatible with AWS and CEPH
    pub fn new(access_key: String, secret_key: String) -> Self {
        V2Authorizer {
            access_key,
            secret_key,
            auth_str: "AWS".to_string(),
            special_header_prefix: "x-amz".to_string(),
        }
    }
    /// Setup the Auth string, if you are using customized S3
    /// Default is "AWS"
    pub fn auth_str(mut self, auth_str: String) -> Self {
        self.auth_str = auth_str;
        self
    }

    /// Setup the Special header prefix, if you are using customized S3
    /// Default is "x-amz"
    pub fn special_header_prefix(mut self, special_header_prefix: String) -> Self {
        self.special_header_prefix = special_header_prefix;
        self
    }
}

impl Authorizer for V2Authorizer {
    fn authorize(&self, request: &mut Request, _now: &UTCTime) {
        let authorize_string = format!(
            "{} {}:{}",
            self.auth_str,
            self.access_key,
            <Request as V2Signature>::sign(request, &self.secret_key)
        );
        let headers = request.headers_mut();
        headers.insert(header::AUTHORIZATION, authorize_string.parse().unwrap());
    }
}

#[derive(Clone, Debug)]
pub struct V4Authorizer {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub service: String,
    pub action: String,
    pub auth_str: String,
    pub special_header_prefix: String,
}

#[allow(dead_code)]
impl V4Authorizer {
    /// new V4 Authorizer for AWS and CEPH
    pub fn new(access_key: String, secret_key: String, region: String) -> Self {
        V4Authorizer {
            access_key,
            secret_key,
            region,
            service: "s3".to_string(),
            action: "aws4_request".to_string(),
            auth_str: "AWS4-HMAC-SHA256".to_string(),
            special_header_prefix: "x-amz".to_string(),
        }
    }
    /// Default is "us-east-1"
    pub fn region(mut self, region: String) -> Self {
        self.region = region;
        self
    }
    /// Default is "s3"
    pub fn service(mut self, service: String) -> Self {
        self.service = service;
        self
    }
    /// Default is "aws4_request"
    pub fn action(mut self, action: String) -> Self {
        self.action = action;
        self
    }
    /// Setup the Auth string, if you are using customized S3
    /// Default is "AWS4-HMAC-SHA256"
    pub fn auth_str(mut self, auth_str: String) -> Self {
        self.auth_str = auth_str;
        self
    }

    /// Setup the Special header prefix, if you are using customized S3
    /// Default is "x-amz"
    pub fn special_header_prefix(mut self, special_header_prefix: String) -> Self {
        self.special_header_prefix = special_header_prefix;
        self
    }
}

impl Authorizer for V4Authorizer {
    fn authorize(&self, request: &mut Request, now: &UTCTime) {
        let SignatureInfo {
            signed_headers,
            signature,
        } = <Request as V4Signature>::sign(
            request,
            &self.auth_str,
            now,
            &self.secret_key,
            &self.region,
            &self.service,
            &self.action,
        );
        let authorize_string = format!(
            "{} Credential={}/{}/{}/{}/{}, SignedHeaders={}, Signature={}",
            self.auth_str,
            self.access_key,
            now.format("%Y%m%d").to_string(),
            self.region,
            self.service,
            self.action,
            signed_headers,
            signature
        );
        let headers = request.headers_mut();
        headers.insert(header::AUTHORIZATION, authorize_string.parse().unwrap());
    }
    fn update_region(&mut self, region: String) {
        self.region = region;
    }
}
#[derive(Clone, Debug)]
pub struct S3Pool {
    pub host: String,
    /// To use https or not, please note that integrity is secured by S3 protocol.
    /// If the confidentiality is not under concerned, the http is good.
    pub secure: bool,
    /// Default will be Path style,
    /// because Virtual hosted URLs may be supported for non-SSL requests only.
    pub url_style: UrlStyle,

    /// The part size for multipart, default disabled.
    /// If Some the pull/push will check out the object size first and do mulitpart
    /// If None download and upload will be in one part
    pub part_size: Option<usize>,

    client: Client,

    pub authorizer: Box<dyn Authorizer>,

    objects: Vec<S3Object>,
    start_after: Option<String>,
}

impl S3Pool {
    pub fn bucket(self, bucket_name: &str) -> Canal {
        Canal {
            up_pool: Some(Box::new(self)),
            down_pool: None,
            upstream_object: Some(bucket_name.into()),
            downstream_object: None,
            default: PoolType::UpPool,
        }
    }

    pub fn resource(self, s3_object: S3Object) -> Canal {
        Canal {
            up_pool: Some(Box::new(self)),
            down_pool: None,
            upstream_object: Some(s3_object),
            downstream_object: None,
            default: PoolType::UpPool,
        }
    }

    pub fn new(host: String) -> Self {
        S3Pool {
            host,
            secure: false,
            url_style: UrlStyle::PATH,
            client: Client::new(),
            authorizer: Box::new(PublicAuthorizer {}),
            part_size: None,
            objects: Vec::with_capacity(1000),
            start_after: None,
        }
    }

    pub fn aws_v2(mut self, access_key: String, secret_key: String) -> Self {
        self.authorizer = Box::new(V2Authorizer::new(access_key, secret_key));
        self.url_style = UrlStyle::PATH;
        self
    }

    pub fn aws_v4(mut self, access_key: String, secret_key: String, region: String) -> Self {
        self.authorizer = Box::new(V4Authorizer::new(access_key, secret_key, region));
        self.url_style = UrlStyle::HOST;
        self
    }

    pub fn endpoint_and_virturalhost(&self, desc: S3Object) -> (String, Option<String>) {
        let ((host, uri), virturalhost) = match self.url_style {
            UrlStyle::PATH => (desc.path_style_links(self.host.clone()), None),
            UrlStyle::HOST => {
                let (host, uri) = desc.virtural_host_style_links(self.host.clone());
                ((host.clone(), uri), Some(host))
            }
        };
        if self.secure {
            (format!("https://{}{}", host, uri), virturalhost)
        } else {
            (format!("http://{}{}", host, uri), virturalhost)
        }
    }

    pub fn init_headers(
        &self,
        headers: &mut HeaderMap,
        now: &UTCTime,
        virturalhost: Option<String>,
    ) {
        headers.insert(
            header::DATE,
            HeaderValue::from_str(now.to_rfc2822().as_str()).unwrap(),
        );
        headers.insert(
            header::USER_AGENT,
            HeaderValue::from_static("Rust S3 Handler"),
        );
        if let Some(virtural_host) = virturalhost {
            headers.insert(header::HOST, HeaderValue::from_str(&virtural_host).unwrap());
        } else {
            headers.insert(header::HOST, HeaderValue::from_str(&self.host).unwrap());
        }
    }

    fn handle_list_response(&mut self, body: String) -> Result<(), Error> {
        self.objects = s3object_list_xml_parser(&body)?;
        // TODO
        // parse start_after
        Ok(())
    }

    pub fn part_size(mut self, s: usize) -> Self {
        self.part_size = Some(s);
        self
    }

    /// Init multipart upload session, and return `multipart_id`
    async fn init_multipart_upload(
        &self,
        url: String,
        virturalhost: Option<String>,
    ) -> Result<String, Error> {
        let url = format!("{}?uploads", url);
        let mut request = self.client.post(&url).build()?;

        let now = Utc::now();
        self.init_headers(request.headers_mut(), &now, virturalhost);
        self.authorizer.authorize(&mut request, &now);

        let r = self.client.execute(request).await?;

        Ok(upload_id_xml_parser(&r.text().await?)?)
    }

    async fn generate_part_upload_requests(
        &self,
        desc: S3Object,
        multipart_id: &str,
        part_size: usize,
        object: Bytes,
    ) -> Result<Vec<Result<Response, reqwest::Error>>, Error> {
        let mut part_number = 0;
        let mut start = 0;
        let mut req_list = vec![];
        while start < object.len() {
            part_number += 1;
            let end = if start + part_size >= object.len() {
                object.len()
            } else {
                start + part_size
            };
            let (endpoint, virtural_host) = self.endpoint_and_virturalhost(desc.clone());
            let url = format!(
                "{}?uploadId={}&partNumber={}",
                endpoint, multipart_id, part_number
            );

            let mut request = self
                .client
                .put(&url)
                .body(object.slice(start..end))
                .build()?;

            let now = Utc::now();
            self.init_headers(request.headers_mut(), &now, virtural_host);
            self.authorizer.authorize(&mut request, &now);
            req_list.push(self.client.execute(request));
            start += part_size
        }
        Ok(join_all(req_list).await)
    }

    async fn complete_multi_part_upload(
        &self,
        reqs: Vec<Result<Response, reqwest::Error>>,
        desc: S3Object,
        multipart_id: &str,
    ) -> Result<Response, Error> {
        let mut content = "<CompleteMultipartUpload>".to_string();
        for (idx, res) in reqs.into_iter().enumerate() {
            let r = res?;
            let etag = r.headers()[reqwest::header::ETAG]
                .to_str()
                .expect("unexpected etag from server");

            content.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                idx + 1,
                etag
            ));
        }
        content.push_str(&"</CompleteMultipartUpload>".to_string());
        let (endpoint, virturalhost) = self.endpoint_and_virturalhost(desc);
        let url = format!("{}?uploadId={}", endpoint, multipart_id);
        let mut request = self.client.post(&url).body(content.into_bytes()).build()?;
        let now = Utc::now();
        self.init_headers(request.headers_mut(), &now, virturalhost);
        self.authorizer.authorize(&mut request, &now);
        let r = self.client.execute(request).await?;
        Ok(r)
    }

    async fn generate_part_download_requests(
        &self,
        desc: S3Object,
        part_size: usize,
    ) -> Result<Vec<Result<Response, reqwest::Error>>, Error> {
        let mut start = 0;
        let mut req_list = vec![];
        while start < desc.size.unwrap() {
            let end = if start + part_size >= desc.size.unwrap() {
                desc.size.unwrap()
            } else {
                start + part_size
            };
            let (url, virturalhost) = self.endpoint_and_virturalhost(desc.clone());

            let mut request = self.client.get(&url).build()?;

            let headers = request.headers_mut();
            headers.insert(
                header::RANGE,
                HeaderValue::from_str(&format!("bytes={}-{}", start, end - 1)).unwrap(),
            );

            let now = Utc::now();
            self.init_headers(headers, &now, virturalhost);
            self.authorizer.authorize(&mut request, &now);
            req_list.push(self.client.execute(request));
            start += part_size
        }
        Ok(join_all(req_list).await)
    }

    async fn complete_multi_part_download(
        &self,
        reqs: Vec<Result<Response, reqwest::Error>>,
    ) -> Result<Bytes, Error> {
        let mut output = BytesMut::with_capacity(0);
        for res in reqs.into_iter() {
            let r = res?;
            // TODO: no copy, check out a way of Bytes -> BytesMut then using unsplit
            output.extend_from_slice(&r.bytes().await?);
        }
        Ok(output.into())
    }
}

impl From<Handler<'_>> for S3Pool {
    fn from(handler: Handler) -> Self {
        let secure = handler.is_secure();
        let Handler {
            host,
            access_key,
            secret_key,
            region,
            auth_type,
            url_style,
            ..
        } = handler;

        let authorizer: Box<dyn Authorizer> = match auth_type {
            AuthType::AWS4 => Box::new(V4Authorizer::new(
                access_key.into(),
                secret_key.into(),
                region.unwrap_or_else(|| DEFAULT_REGION.to_string()),
            )),
            AuthType::AWS2 => Box::new(V2Authorizer::new(access_key.into(), secret_key.into())),
        };

        Self {
            host: host.into(),
            secure,
            url_style,
            client: Client::new(),
            authorizer,
            part_size: Some(5242880),
            objects: Vec::with_capacity(1000),
            start_after: None,
        }
    }
}

impl From<&Handler<'_>> for S3Pool {
    fn from(handler: &Handler) -> Self {
        let secure = handler.is_secure();
        let Handler {
            host,
            access_key,
            secret_key,
            region,
            auth_type,
            url_style,
            ..
        } = handler;

        let authorizer: Box<dyn Authorizer> = match auth_type {
            AuthType::AWS4 => Box::new(V4Authorizer::new(
                access_key.to_string(),
                secret_key.to_string(),
                region.clone().unwrap_or_else(|| DEFAULT_REGION.to_string()),
            )),
            AuthType::AWS2 => Box::new(V2Authorizer::new(
                access_key.to_string(),
                secret_key.to_string(),
            )),
        };

        Self {
            host: host.to_string(),
            secure,
            url_style: url_style.clone(),
            client: Client::new(),
            authorizer,
            part_size: Some(5242880),
            objects: Vec::with_capacity(1000),
            start_after: None,
        }
    }
}

#[async_trait]
impl DataPool for S3Pool {
    async fn push(&self, desc: S3Object, object: Bytes) -> Result<(), Error> {
        let part_size = self.part_size.unwrap_or_default();
        let _r = if part_size > 0 && part_size < object.len() {
            let (endpoint, virturalhost) = self.endpoint_and_virturalhost(desc.clone());
            let multipart_id = self.init_multipart_upload(endpoint, virturalhost).await?;

            let reqs = self
                .generate_part_upload_requests(desc.clone(), &multipart_id, part_size, object)
                .await?;
            self.complete_multi_part_upload(reqs, desc, &multipart_id)
                .await?
        } else {
            let (endpoint, virturalhost) = self.endpoint_and_virturalhost(desc);
            let mut request = self.client.put(&endpoint).body(object).build()?;

            let now = Utc::now();
            self.init_headers(request.headers_mut(), &now, virturalhost);
            self.authorizer.authorize(&mut request, &now);
            self.client.execute(request).await?
        };
        // TODO validate _r status code
        Ok(())
    }

    async fn pull(&self, mut desc: S3Object) -> Result<Bytes, Error> {
        self.fetch_meta(&mut desc).await?;
        let part_size = self.part_size.unwrap_or_default();
        if part_size > 0 && part_size < desc.size.unwrap_or_default() {
            let reqs = self
                .generate_part_download_requests(desc, part_size)
                .await?;
            let output = self.complete_multi_part_download(reqs).await?;

            Ok(output)
        } else {
            // TODO reuse the client setting and not only the reqest
            let (endpoint, virturalhost) = self.endpoint_and_virturalhost(desc);
            let mut request = Request::new(Method::GET, Url::parse(&endpoint)?);

            let now = Utc::now();
            self.init_headers(request.headers_mut(), &now, virturalhost);
            self.authorizer.authorize(&mut request, &now);

            let r = self.client.execute(request).await?;
            // TODO validate status code
            Ok(r.bytes().await?)
        }
    }

    async fn list(&self, index: Option<S3Object>) -> Result<Box<dyn S3Folder>, Error> {
        let mut pool = self.clone();
        let (endpoint, virturalhost) = self.endpoint_and_virturalhost(index.unwrap_or_default());
        let mut request = Request::new(Method::GET, Url::parse(&endpoint)?);

        let now = Utc::now();
        pool.init_headers(request.headers_mut(), &now, virturalhost);
        pool.authorizer.authorize(&mut request, &now);
        let body = pool.client.execute(request).await?.text().await?;
        pool.handle_list_response(body)?;
        Ok(Box::new(pool))
    }

    async fn remove(&self, desc: S3Object) -> Result<(), Error> {
        let (endpoint, virturalhost) = self.endpoint_and_virturalhost(desc);
        let mut request = Request::new(Method::DELETE, Url::parse(&endpoint)?);

        let now = Utc::now();
        self.init_headers(request.headers_mut(), &now, virturalhost);
        self.authorizer.authorize(&mut request, &now);

        let _r = self.client.execute(request).await?;
        // TODO validate status code
        Ok(())
    }

    fn check_scheme(&self, scheme: &str) -> Result<(), Error> {
        if scheme.to_lowercase() != "s3" {
            Err(Error::SchemeError())
        } else {
            Ok(())
        }
    }

    async fn fetch_meta(&self, desc: &mut S3Object) -> Result<(), Error> {
        let (endpoint, virturalhost) = self.endpoint_and_virturalhost(desc.clone());
        let mut request = self.client.head(&endpoint).build()?;

        let now = Utc::now();
        self.init_headers(request.headers_mut(), &now, virturalhost);
        self.authorizer.authorize(&mut request, &now);

        let r = self.client.execute(request).await?;
        let headers = r.headers();
        desc.etag = if headers.contains_key(reqwest::header::ETAG) {
            Some(
                headers[reqwest::header::ETAG]
                    .to_str()?
                    .to_string()
                    .replace('"', ""),
            )
        } else {
            None
        };
        desc.mtime = if headers.contains_key(HeaderName::from_lowercase(b"last-modified").unwrap())
        {
            Some(
                headers[HeaderName::from_lowercase(b"last-modified").unwrap()]
                    .to_str()?
                    .into(),
            )
        } else {
            None
        };
        desc.size = if headers.contains_key(reqwest::header::CONTENT_LENGTH) {
            Some(
                headers[reqwest::header::CONTENT_LENGTH]
                    .to_str()?
                    .parse::<usize>()
                    .unwrap_or_default(),
            )
        } else {
            None
        };

        // TODO: check out it is correct or not that the storage class is absent here

        Ok(())
    }
}

#[async_trait]
impl S3Folder for S3Pool {
    async fn next_object(&mut self) -> Result<Option<S3Object>, Error> {
        // if self.objects.is_empty() && self.start_after.is_some() {
        //     // let mut url = self.client.url.clone();
        //     // url.query_pairs_mut()
        //     //     .append_pair("start-after", &self.start_after.take().unwrap());
        //     // let r = self.client.execute(Request::new(Method::GET, url)).await?;
        //     // self.handle_response(r).await?;
        // }
        if self.objects.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.objects.remove(0)))
        }
    }
}

pub struct CanonicalHeadersInfo {
    pub signed_headers: String,
    pub canonical_headers: String,
}

pub struct CanonicalRequestInfo {
    pub signed_headers: String,
    pub canonical_request: String,
}

pub trait Canonical {
    fn canonical_headers_info(&self) -> CanonicalHeadersInfo;
    fn canonical_query_string(&self) -> String;
    fn canonical_request_info(&self, payload_hash: &str) -> CanonicalRequestInfo;
}

impl Canonical for Request {
    fn canonical_headers_info(&self) -> CanonicalHeadersInfo {
        let mut canonical_headers = String::new();
        let mut signed_headers = Vec::new();

        let mut headers: Vec<(String, &str)> = self
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default()))
            .collect();

        headers.sort_by(|a, b| a.0.to_lowercase().as_str().cmp(b.0.to_lowercase().as_str()));
        for h in headers {
            canonical_headers.push_str(h.0.to_lowercase().as_str());
            canonical_headers.push(':');
            canonical_headers.push_str(h.1.trim());
            canonical_headers.push('\n');
            signed_headers.push(h.0.to_lowercase());
        }
        CanonicalHeadersInfo {
            signed_headers: signed_headers.join(";"),
            canonical_headers,
        }
    }

    fn canonical_query_string(&self) -> String {
        let mut encoded = form_urlencoded::Serializer::new(String::new());
        let mut qs: Vec<(String, String)> = self
            .url()
            .query_pairs()
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_owned(), v.as_ref().to_owned()))
            .collect();

        qs.sort_by(|x, y| x.0.cmp(&y.0));

        for (key, value) in qs {
            encoded.append_pair(&key, &value);
        }

        // There is a `~` in upload id, should be treated in a tricky way.
        //
        // >>>
        // In the concatenated string, period characters (.) are not escaped.
        // RFC 3986 considers the period character an unreserved character,
        // so it is **not** URL encoded.
        // >>>
        //
        // ref:
        // https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html#create-canonical-string
        encoded.finish().replace("%7E", "~")
    }

    fn canonical_request_info(&self, payload_hash: &str) -> CanonicalRequestInfo {
        let CanonicalHeadersInfo {
            signed_headers,
            canonical_headers,
        } = self.canonical_headers_info();
        CanonicalRequestInfo {
            signed_headers: signed_headers.clone(),
            canonical_request: format!(
                "{}\n{}\n{}\n{}\n{}\n{}",
                self.method().as_str(),
                self.url().path(),
                self.canonical_query_string(),
                canonical_headers,
                signed_headers,
                payload_hash
            ),
        }
    }
}

pub trait V2Signature
where
    Self: Canonical,
{
    fn string_to_signed(&self) -> String;
    fn sign(&self, sign_key: &str) -> String;
}

impl V2Signature for Request {
    fn string_to_signed(&self) -> String {
        format!(
            "{}\n\n\n{}\n{}{}",
            self.method().as_str(),
            self.headers().get(header::DATE).unwrap().to_str().unwrap(),
            self.url().path(),
            self.canonical_query_string()
        )
    }
    fn sign(&self, sign_key: &str) -> String {
        encode(&hmacsha1::hmac_sha1(
            sign_key.as_bytes(),
            <Request as V2Signature>::string_to_signed(self).as_bytes(),
        ))
    }
}

pub struct RequestHashInfo {
    pub signed_headers: String,
    pub sha256: String,
}

pub struct StringToSignInfo {
    pub signed_headers: String,
    pub string_to_signed: String,
}

pub struct SignatureInfo {
    pub signed_headers: String,
    pub signature: String,
}

pub trait V4Signature
where
    Self: Canonical,
{
    fn string_to_signed(
        &mut self,
        auth_str: &str,
        now: &UTCTime,
        region: &str,
        service: &str,
        action: &str,
    ) -> StringToSignInfo;
    /// calculate hash mac and update header
    fn payload_sha256(&mut self) -> String;
    /// calculate hash mac and update header
    fn request_sha256(&mut self) -> RequestHashInfo;
    fn sign(
        &mut self,
        auth_str: &str,
        now: &UTCTime,
        sign_key: &str,
        region: &str,
        service: &str,
        action: &str,
    ) -> SignatureInfo;
}

impl V4Signature for Request {
    fn string_to_signed(
        &mut self,
        auth_str: &str,
        now: &UTCTime,
        region: &str,
        service: &str,
        action: &str,
    ) -> StringToSignInfo {
        let iso_8601_str = {
            let mut s = now.to_rfc3339();
            s.retain(|c| !['-', ':'].contains(&c));
            format!("{}Z", &s[..15])
        };
        let headers = self.headers_mut();
        headers.insert(
            header::HeaderName::from_static("x-amz-date"),
            HeaderValue::from_str(&iso_8601_str).unwrap(),
        );
        let RequestHashInfo {
            signed_headers,
            sha256,
        } = self.request_sha256();
        StringToSignInfo {
            signed_headers,
            string_to_signed: format!(
                "{}\n{}\n{}/{}/{}/{}\n{}",
                auth_str,
                iso_8601_str,
                &iso_8601_str[..8],
                region,
                service,
                action,
                sha256
            ),
        }
    }

    fn payload_sha256(&mut self) -> String {
        let mut sha = Sha256::new();
        sha.input(
            self.body()
                .map(|b| b.as_bytes())
                .unwrap_or_default()
                .unwrap_or_default(),
        );
        let paload_hash = sha.result_str();
        let headers = self.headers_mut();
        headers.insert(
            header::HeaderName::from_static("x-amz-content-sha256"),
            HeaderValue::from_str(&paload_hash).unwrap(),
        );
        paload_hash
    }

    fn request_sha256(&mut self) -> RequestHashInfo {
        let paload_hash = self.payload_sha256();

        let CanonicalRequestInfo {
            signed_headers,
            canonical_request,
        } = self.canonical_request_info(&paload_hash);

        let mut sha = Sha256::new();
        sha.input_str(canonical_request.as_str());
        RequestHashInfo {
            signed_headers,
            sha256: sha.result_str(),
        }
    }

    fn sign(
        &mut self,
        auth_str: &str,
        now: &UTCTime,
        sign_key: &str,
        region: &str,
        service: &str,
        action: &str,
    ) -> SignatureInfo {
        let StringToSignInfo {
            signed_headers,
            string_to_signed,
        } = <Request as V4Signature>::string_to_signed(
            self, auth_str, now, region, service, action,
        );
        let time_str = {
            let mut s = now.to_rfc3339();
            s.retain(|c| !['-', ':'].contains(&c));
            &s[..8].to_string()
        };

        let mut key: String = auth_str.split('-').next().unwrap_or_default().to_string();
        key.push_str(sign_key);

        let mut mac = Hmac::<sha2_256>::new_from_slice(key.as_str().as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(time_str.as_bytes());
        let result = mac.finalize();
        let code_bytes = result.into_bytes();

        let mut mac1 =
            Hmac::<sha2_256>::new_from_slice(&code_bytes).expect("HMAC can take key of any size");
        mac1.update(region.as_bytes());
        let result1 = mac1.finalize();
        let code_bytes1 = result1.into_bytes();

        let mut mac2 =
            Hmac::<sha2_256>::new_from_slice(&code_bytes1).expect("HMAC can take key of any size");
        mac2.update(service.as_bytes());
        let result2 = mac2.finalize();
        let code_bytes2 = result2.into_bytes();

        let mut mac3 =
            Hmac::<sha2_256>::new_from_slice(&code_bytes2).expect("HMAC can take key of any size");
        mac3.update(action.as_bytes());
        let result3 = mac3.finalize();
        let code_bytes3 = result3.into_bytes();

        let mut mac4 =
            Hmac::<sha2_256>::new_from_slice(&code_bytes3).expect("HMAC can take key of any size");
        mac4.update(string_to_signed.as_bytes());
        let result4 = mac4.finalize();
        let code_bytes4 = result4.into_bytes();

        SignatureInfo {
            signed_headers,
            signature: code_bytes4.to_hex(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocking::CredentialConfig;

    #[tokio::test]
    async fn test_handle_list_response() {
        let s = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>ant-lab</Name><Prefix></Prefix><Marker></Marker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>14M</Key><LastModified>2020-01-31T14:58:45.000Z</LastModified><ETag>&quot;8ff43d748637d249d80d6f45e15c7663-3&quot;</ETag><Size>14336000</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>7M</Key><LastModified>2020-11-21T09:50:46.000Z</LastModified><ETag>&quot;cbe4f29b8b099989ae49afc02aa1c618-2&quot;</ETag><Size>7168000</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>7M.json</Key><LastModified>2020-09-19T14:59:23.000Z</LastModified><ETag>&quot;d34bd3f9aff10629ac49353312a42b0f-2&quot;</ETag><Size>7168000</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>get</Key><LastModified>2020-08-11T06:10:11.000Z</LastModified><ETag>&quot;f895d74af5106ce0c3d6cb008fb3b98d&quot;</ETag><Size>304</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>t</Key><LastModified>2020-09-19T15:10:08.000Z</LastModified><ETag>&quot;5050ef3558233dc04b3fac50eff68de1&quot;</ETag><Size>10</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>t.txt</Key><LastModified>2020-09-19T15:04:46.000Z</LastModified><ETag>&quot;5050ef3558233dc04b3fac50eff68de1&quot;</ETag><Size>10</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>test-orig</Key><LastModified>2020-11-21T09:48:29.000Z</LastModified><ETag>&quot;c059dadd468de1835bc99dab6e3b2cee-3&quot;</ETag><Size>11534336</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>test-s3handle</Key><LastModified>2020-11-21T10:09:39.000Z</LastModified><ETag>&quot;5dd39cab1c53c2c77cd352983f9641e1&quot;</ETag><Size>20</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>test.json</Key><LastModified>2020-08-11T09:54:42.000Z</LastModified><ETag>&quot;f895d74af5106ce0c3d6cb008fb3b98d&quot;</ETag><Size>304</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>";
        let mut pool = S3Pool::new("somewhere.in.the.world".to_string());
        pool.handle_list_response(s.to_string()).unwrap();
        assert!(pool.next_object().await.unwrap().is_some());
    }

    #[test]
    fn test_from_blocking_handle_to_s3_pool() {
        let config = CredentialConfig {
            host: "s3.us-east-1.amazonaws.com".to_string(),
            access_key: "akey".to_string(),
            secret_key: "skey".to_string(),
            user: None,
            region: None,  // default is us-east-1
            s3_type: None, // default will try to config as AWS S3 handler
            secure: None,  // dafault is false, because the integrity protect by HMAC
        };
        let handler = Handler::from(&config);
        let mut pool = S3Pool::from(&handler);
        let s3_pool = S3Pool::new("s3.us-east-1.amazonaws.com".to_string());
        assert_eq!(pool.host, s3_pool.host);

        pool = handler.into();
        let s3_pool = S3Pool::new("s3.us-east-1.amazonaws.com".to_string());
        assert_eq!(pool.host, s3_pool.host);
    }
}
