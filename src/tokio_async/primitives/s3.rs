use async_trait::async_trait;
use base64::encode;
use bytes::Bytes;
use chrono::prelude::*;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use dyn_clone::DynClone;
use hmac::{Hmac, Mac};
use reqwest::{
    header::{self, HeaderMap, HeaderValue},
    Client, Method, Request, Url,
};
use rustc_serialize::hex::ToHex;
use sha2::Sha256 as sha2_256;
use url::form_urlencoded;

use super::canal::{Canal, PoolType};
use crate::error::Error;
use crate::tokio_async::traits::{DataPool, S3Folder};
use crate::utils::{s3object_list_xml_parser, S3Convert, S3Object, UrlStyle};

type UTCTime = DateTime<Utc>;

pub trait Authorizer: Send + Sync + DynClone {
    /// This method will setup the header and put the authorize string
    fn authorize(&self, _request: &mut Request, _now: &UTCTime) {
        unimplemented!()
    }

    /// This method will be called once the resource change the region stored
    fn update_region(&mut self, _region: String) {}
}

dyn_clone::clone_trait_object!(Authorizer);

#[derive(Clone)]
pub struct PublicAuthorizer {}

impl Authorizer for PublicAuthorizer {
    fn authorize(&self, _requests: &mut Request, _now: &UTCTime) {}
}

#[derive(Clone)]
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

#[derive(Clone)]
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

#[derive(Clone)]
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
            upstream_object: Some(bucket_name.to_string().into()),
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
        self
    }

    pub fn aws_v4(mut self, access_key: String, secret_key: String, region: String) -> Self {
        self.authorizer = Box::new(V4Authorizer::new(access_key, secret_key, region));
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

    pub fn init_headers(&self, headers: &mut HeaderMap, now: &UTCTime) {
        headers.insert(
            header::DATE,
            HeaderValue::from_str(now.to_rfc2822().as_str()).unwrap(),
        );
        headers.insert(header::HOST, HeaderValue::from_str(&self.host).unwrap());
    }

    fn handle_list_response(&mut self, body: String) -> Result<(), Error> {
        self.objects = s3object_list_xml_parser(&body)?;
        // TODO
        // parse start_after
        Ok(())
    }
}

#[async_trait]
impl DataPool for S3Pool {
    async fn push(&self, desc: S3Object, object: Bytes) -> Result<(), Error> {
        if let Some(_part_size) = self.part_size {
            // TODO mulitipart
            unimplemented!()
        } else {
            // TODO reuse the client setting and not only the reqest
            let mut request = self.client.put(&self.endpoint(desc)).body(object).build()?;

            let now = Utc::now();
            self.init_headers(request.headers_mut(), &now);
            self.authorizer.authorize(&mut request, &now);

            let _r = self.client.execute(request).await?;
            // TODO validate status code
        }
        Ok(())
    }

    async fn pull(&self, desc: S3Object) -> Result<Bytes, Error> {
        if let Some(_part_size) = self.part_size {
            // TODO mulitipart
            unimplemented!()
        } else {
            // TODO reuse the client setting and not only the reqest
            let mut request = Request::new(Method::GET, Url::parse(&self.endpoint(desc))?);

            let now = Utc::now();
            self.init_headers(request.headers_mut(), &now);
            self.authorizer.authorize(&mut request, &now);

            let r = self.client.execute(request).await?;

            // TODO validate status code
            Ok(r.bytes().await?)
        }
    }

    async fn list(&self, index: Option<S3Object>) -> Result<Box<dyn S3Folder>, Error> {
        let mut pool = self.clone();
        let mut request = Request::new(
            Method::GET,
            Url::parse(&self.endpoint(index.unwrap_or_default()))?,
        );

        let now = Utc::now();
        pool.init_headers(request.headers_mut(), &now);
        pool.authorizer.authorize(&mut request, &now);
        let body = pool.client.execute(request).await?.text().await?;
        pool.handle_list_response(body)?;
        Ok(Box::new(pool))
    }

    async fn remove(&self, desc: S3Object) -> Result<(), Error> {
        let mut request = Request::new(Method::DELETE, Url::parse(&self.endpoint(desc))?);

        let now = Utc::now();
        self.init_headers(request.headers_mut(), &now);
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

        let mut key: String = auth_str.split("-").next().unwrap_or_default().to_string();
        key.push_str(sign_key);

        let mut mac = Hmac::<sha2_256>::new(key.as_str().as_bytes());
        mac.input(time_str.as_bytes());
        let result = mac.result();
        let code_bytes = result.code();

        let mut mac1 = Hmac::<sha2_256>::new(code_bytes);
        mac1.input(region.as_bytes());
        let result1 = mac1.result();
        let code_bytes1 = result1.code();

        let mut mac2 = Hmac::<sha2_256>::new(code_bytes1);
        mac2.input(service.as_bytes());
        let result2 = mac2.result();
        let code_bytes2 = result2.code();

        let mut mac3 = Hmac::<sha2_256>::new(code_bytes2);
        mac3.input(action.as_bytes());
        let result3 = mac3.result();
        let code_bytes3 = result3.code();

        let mut mac4 = Hmac::<sha2_256>::new(code_bytes3);
        mac4.input(string_to_signed.as_bytes());
        let result4 = mac4.result();
        let code_bytes4 = result4.code();

        SignatureInfo {
            signed_headers,
            signature: code_bytes4.to_hex(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handle_list_response() {
        let s = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>ant-lab</Name><Prefix></Prefix><Marker></Marker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>14M</Key><LastModified>2020-01-31T14:58:45.000Z</LastModified><ETag>&quot;8ff43d748637d249d80d6f45e15c7663-3&quot;</ETag><Size>14336000</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>7M</Key><LastModified>2020-11-21T09:50:46.000Z</LastModified><ETag>&quot;cbe4f29b8b099989ae49afc02aa1c618-2&quot;</ETag><Size>7168000</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>7M.json</Key><LastModified>2020-09-19T14:59:23.000Z</LastModified><ETag>&quot;d34bd3f9aff10629ac49353312a42b0f-2&quot;</ETag><Size>7168000</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>get</Key><LastModified>2020-08-11T06:10:11.000Z</LastModified><ETag>&quot;f895d74af5106ce0c3d6cb008fb3b98d&quot;</ETag><Size>304</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>t</Key><LastModified>2020-09-19T15:10:08.000Z</LastModified><ETag>&quot;5050ef3558233dc04b3fac50eff68de1&quot;</ETag><Size>10</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>t.txt</Key><LastModified>2020-09-19T15:04:46.000Z</LastModified><ETag>&quot;5050ef3558233dc04b3fac50eff68de1&quot;</ETag><Size>10</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>test-orig</Key><LastModified>2020-11-21T09:48:29.000Z</LastModified><ETag>&quot;c059dadd468de1835bc99dab6e3b2cee-3&quot;</ETag><Size>11534336</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>test-s3handle</Key><LastModified>2020-11-21T10:09:39.000Z</LastModified><ETag>&quot;5dd39cab1c53c2c77cd352983f9641e1&quot;</ETag><Size>20</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>test.json</Key><LastModified>2020-08-11T09:54:42.000Z</LastModified><ETag>&quot;f895d74af5106ce0c3d6cb008fb3b98d&quot;</ETag><Size>304</Size><Owner><ID>54bbddd7c9c485b696f5b188467d4bec889b83d3862d0a6db526d9d17aadcee2</ID><DisplayName>yanganto</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>";
        let mut pool = S3Pool::new("somewhere.in.the.world".to_string());
        pool.handle_list_response(s.to_string()).unwrap();
        assert!(pool.next_object().await.unwrap().is_some());
    }
}
