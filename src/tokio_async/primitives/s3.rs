use async_trait::async_trait;
use reqwest::{header::HeaderMap, Client, Method, Request, Response, Url};

use super::canal::{Canal, PoolType};
use crate::error::Error;
use crate::tokio_async::traits::DataPool;
use crate::utils::{S3Object, UrlStyle};

pub trait Authorizer: Send + Sync {
    /// This method will setup the header and put the authorize string
    fn authorize(&self, requests: &mut Request) {
        unimplemented!()
    }

    /// This method will be called once the resource change the region stored
    fn update_region(&self, region: String) {}
}

pub struct PublicAuthorizer {}

impl Authorizer for PublicAuthorizer {
    fn authorize(&self, requests: &mut Request) {}
}

pub struct V2Authorizer {
    pub access_key: String,
    pub secret_key: String,
    pub auth_str: String,
}

impl V2Authorizer {
    /// new V2 Authorizer compatible with AWS and CEPH
    pub fn new(access_key: String, secret_key: String) -> Self {
        V2Authorizer {
            access_key,
            secret_key,
            auth_str: "AWS".to_string(),
        }
    }
    /// Setup the Auth string, if you are using customized S3
    /// Default is "AWS"
    pub fn auth_str(mut self, auth_str: String) -> Self {
        self.auth_str = auth_str;
        self
    }
}

impl Authorizer for V2Authorizer {
    fn authorize(&self, requests: &mut Request) {
        // add AUTHORIZATION as  "{auth_str} {access_key}:{signature}"
        // request_headers.insert(header::AUTHORIZATION, "");
        unimplemented!()
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
    fn update_region(&self, region: String) {
        unimplemented!()
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
        }
    }
    pub fn aws_v2(mut self, access_key: String, secret_key: String) -> Self {
        self.authorizer = Box::new(V2Authorizer::new(access_key, secret_key));
        self
    }
}

#[async_trait]
impl DataPool for S3Pool {
    async fn push(&self, desc: S3Object, object: Vec<u8>) -> Result<(), Error> {
        unimplemented!()
    }
    async fn pull(&self, desc: S3Object) -> Result<Vec<u8>, Error> {
        // let mut request: Request;
        // self.authorizor.authorize(request).
        // self.client.execute(request)
        unimplemented!()
    }
    async fn list(
        &self,
        index: Option<S3Object>,
    ) -> Result<(Vec<S3Object>, Option<S3Object>), Error> {
        unimplemented!()
    }
    async fn remove(&self, desc: S3Object) -> Result<(), Error> {
        unimplemented!()
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
