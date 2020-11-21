use async_trait::async_trait;
use base64::encode;
use bytes::Bytes;
use chrono::prelude::*;
use reqwest::{
    header::{self, HeaderMap, HeaderValue},
    Client, Method, Request, Url,
};
use url::form_urlencoded;

use super::canal::{Canal, PoolType};
use crate::error::Error;
use crate::tokio_async::traits::DataPool;
use crate::utils::{S3Convert, S3Object, UrlStyle};

pub trait Authorizer: Send + Sync {
    /// This method will setup the header and put the authorize string
    fn authorize(&self, _request: &mut Request) {
        unimplemented!()
    }

    /// This method will be called once the resource change the region stored
    fn update_region(&mut self, _region: String) {}
}

pub struct PublicAuthorizer {}

impl Authorizer for PublicAuthorizer {
    fn authorize(&self, _requests: &mut Request) {}
}

pub struct V2Authorizer {
    pub access_key: String,
    pub secret_key: String,
    pub auth_str: String,
    pub special_header_prefix: String,
}

impl V2Authorizer {
    /// new V2 Authorizer compatible with AWS and CEPH
    pub fn new(access_key: String, secret_key: String) -> Self {
        V2Authorizer {
            access_key,
            secret_key,
            auth_str: "AWS".to_string(),
            special_header_prefix: "X-AMZ".to_string(),
        }
    }
    /// Setup the Auth string, if you are using customized S3
    /// Default is "AWS"
    pub fn auth_str(mut self, auth_str: String) -> Self {
        self.auth_str = auth_str;
        self
    }

    /// Setup the Special header prefix, if you are using customized S3
    /// Default is "X-AMZ"
    pub fn special_header_prefix(mut self, special_header_prefix: String) -> Self {
        self.special_header_prefix = special_header_prefix;
        self
    }
}

impl Authorizer for V2Authorizer {
    fn authorize(&self, request: &mut Request) {
        let signature = request.sign(&self.secret_key);
        let authorize_string = format!("{} {}:{}", self.auth_str, self.access_key, signature);
        let headers = request.headers_mut();
        headers.insert(header::AUTHORIZATION, authorize_string.parse().unwrap());
    }
}

pub struct V4Authorizer {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub service: String,  // s3
    pub action: String,   // aws4_request
    pub auth_str: String, // AWS4-HMAC-SHA256
}

impl V4Authorizer {
    /// new V4 Authorizer for AWS and CEPH
    pub fn new(access_key: String, secret_key: String) -> Self {
        V4Authorizer {
            access_key,
            secret_key,
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
            action: "aws4_request".to_string(),
            auth_str: "AWS4-HMAC-SHA256".to_string(),
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
}

impl Authorizer for V4Authorizer {
    fn update_region(&mut self, region: String) {
        self.region = region;
    }
}

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
}

impl S3Pool {
    pub fn new(host: String) -> Self {
        S3Pool {
            host,
            secure: false,
            url_style: UrlStyle::PATH,
            client: Client::new(),
            authorizer: Box::new(PublicAuthorizer {}),
            part_size: None,
        }
    }
    pub fn aws_v2(mut self, access_key: String, secret_key: String) -> Self {
        self.authorizer = Box::new(V2Authorizer::new(access_key, secret_key));
        self
    }

    pub fn endpoint(&self, desc: S3Object) -> String {
        let (host, uri) = match self.url_style {
            UrlStyle::PATH => desc.path_style_links(self.host.clone()),
            UrlStyle::HOST => desc.virtural_host_style_links(self.host.clone()),
        };
        if self.secure {
            format!("https://{}{}", host, uri)
        } else {
            format!("http://{}{}", host, uri)
        }
    }

    pub fn init_headers(&self, headers: &mut HeaderMap) {
        headers.insert(
            header::DATE,
            HeaderValue::from_str(Utc::now().to_rfc2822().as_str()).unwrap(),
        );
        headers.insert(header::HOST, HeaderValue::from_str(&self.host).unwrap());
    }
}

#[async_trait]
impl DataPool for S3Pool {
    async fn push(&self, desc: S3Object, object: Bytes) -> Result<(), Error> {
        if let Some(part_size) = self.part_size {
            // TODO mulitipart
            unimplemented!()
        } else {
            // TODO reuse the client setting and not only the reqest
            let mut request = self.client.put(&self.endpoint(desc)).body(object).build()?;

            self.init_headers(request.headers_mut());
            self.authorizer.authorize(&mut request);

            let _r = self.client.execute(request).await?;
            // TODO validate status code
        }
        Ok(())
    }
    async fn pull(&self, desc: S3Object) -> Result<Bytes, Error> {
        if let Some(part_size) = self.part_size {
            // TODO mulitipart
            unimplemented!()
        } else {
            // TODO reuse the client setting and not only the reqest
            let mut request = Request::new(Method::GET, Url::parse(&self.endpoint(desc))?);

            self.init_headers(request.headers_mut());
            self.authorizer.authorize(&mut request);

            let r = self.client.execute(request).await?;

            // TODO validate status code
            Ok(r.bytes().await?)
        }
    }
    async fn list(
        &self,
        index: Option<S3Object>,
    ) -> Result<(Vec<S3Object>, Option<S3Object>), Error> {
        unimplemented!()
    }
    async fn remove(&self, desc: S3Object) -> Result<(), Error> {
        let mut request = Request::new(Method::DELETE, Url::parse(&self.endpoint(desc))?);

        self.init_headers(request.headers_mut());
        self.authorizer.authorize(&mut request);

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
}
impl S3Pool {
    pub fn bucket(self, bucket_name: &str) -> Canal {
        Canal {
            up_pool: Some(Box::new(self)),
            down_pool: None,
            upstream_object: Some(bucket_name.to_string().into()),
            downstream_object: None,
            default: PoolType::UpPool,
        }
    }
}

pub trait Canonical {
    fn canonical_query_string(&self) -> String;
}

impl Canonical for Request {
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
            self.string_to_signed().as_bytes(),
        ))
    }
}
