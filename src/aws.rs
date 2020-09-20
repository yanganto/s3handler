use crate::error::Error;
use crate::{Format, ResponseHandler, S3Client};
use base64::encode;
use chrono::prelude::*;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use hmac::{Hmac, Mac};
use hmacsha1;
use md5;
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::{blocking::Client, header, StatusCode};
use rustc_serialize::hex::ToHex;
use sha2::Sha256 as sha2_256;
use std::str::FromStr;
use url::form_urlencoded;

pub(crate) struct AWS2Client<'a> {
    pub tls: bool,
    pub access_key: &'a str,
    pub secret_key: &'a str,
}

pub(crate) struct AWS4Client<'a> {
    pub tls: bool,
    pub host: &'a str, // handle region redirect
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub region: String,
}

impl S3Client for AWS2Client<'_> {
    fn request(
        &self,
        method: &str,
        host: &str,
        uri: &str,
        query_strings: &mut Vec<(&str, &str)>,
        headers: &mut Vec<(&str, &str)>,
        payload: &Vec<u8>,
    ) -> Result<(StatusCode, Vec<u8>, reqwest::header::HeaderMap), Error> {
        let url = if self.tls {
            format!(
                "https://{}{}?{}",
                host,
                uri,
                canonical_query_string(query_strings)
            )
        } else {
            format!(
                "http://{}{}?{}",
                host,
                uri,
                canonical_query_string(query_strings)
            )
        };
        let utc: DateTime<Utc> = Utc::now();
        let mut request_headers = header::HeaderMap::new();
        let time_str = utc.to_rfc2822();

        // NOTE: ceph has bug using x-amz-date
        let mut signed_headers = vec![("Date", time_str.as_str())];

        let request_headers_name: Vec<String> =
            headers.into_iter().map(|x| x.0.to_string()).collect();

        request_headers.insert("date", time_str.clone().parse().unwrap());

        // Support AWS delete marker feature
        if request_headers_name.contains(&"delete-marker".to_string()) {
            for h in headers {
                if h.0 == "delete-marker" {
                    request_headers.insert("x-amz-delete-marker", h.1.parse().unwrap());
                    signed_headers.push(("x-amz-delete-marker", h.1));
                }
            }
        }

        let signature = aws_s3_v2_sign(
            self.secret_key,
            &aws_s3_v2_get_string_to_signed(method, uri, &mut signed_headers, payload),
        );
        let mut authorize_string = String::from_str("AWS ").unwrap();
        authorize_string.push_str(self.access_key);
        authorize_string.push(':');
        authorize_string.push_str(&signature);
        request_headers.insert(header::AUTHORIZATION, authorize_string.parse().unwrap());

        // get a client builder
        let client = Client::builder()
            .default_headers(request_headers)
            .build()
            .unwrap();

        let action;
        match method {
            "GET" => {
                action = client.get(url.as_str());
            }
            "PUT" => {
                action = client.put(url.as_str());
            }
            "DELETE" => {
                action = client.delete(url.as_str());
            }
            "POST" => {
                action = client.post(url.as_str());
            }
            _ => {
                error!("unspport HTTP verb");
                action = client.get(url.as_str());
            }
        }
        action
            .body((*payload).clone())
            .send()
            .map_err(|e| Error::ReqwestError(format!("{:?}", e)))
            .and_then(|mut res| Ok(res.handle_response()))
    }
    fn redirect_parser(&self, _body: Vec<u8>, _format: Format) -> Result<String, Error> {
        // TODO: implement redirect for aws2
        unimplemented!();
    }
    fn update(&mut self, _region: String, _secure: bool) {
        // AWS2 does not region info
    }
    fn current_region(&self) -> Option<String> {
        None
    }
}

impl S3Client for AWS4Client<'_> {
    fn request(
        &self,
        method: &str,
        host: &str,
        uri: &str,
        query_strings: &mut Vec<(&str, &str)>,
        headers: &mut Vec<(&str, &str)>,
        payload: &Vec<u8>,
    ) -> Result<(StatusCode, Vec<u8>, reqwest::header::HeaderMap), Error> {
        let url = if self.tls {
            format!(
                "https://{}{}?{}",
                host,
                uri,
                canonical_query_string(query_strings)
            )
        } else {
            format!(
                "http://{}{}?{}",
                host,
                uri,
                canonical_query_string(query_strings)
            )
        };
        let utc: DateTime<Utc> = Utc::now();
        let mut request_headers = header::HeaderMap::new();
        let time_str = utc.format("%Y%m%dT%H%M%SZ").to_string();
        let payload_hash = hash_payload(&payload);

        request_headers.insert("x-amz-date", time_str.clone().parse().unwrap());
        request_headers.insert("x-amz-content-sha256", payload_hash.parse().unwrap());

        let request_headers_name: Vec<String> =
            headers.into_iter().map(|x| x.0.to_string()).collect();

        let mut signed_headers = vec![];
        if request_headers_name.contains(&"content-type".to_string()) {
            for h in headers.iter() {
                if h.0 == "content-type" {
                    request_headers.insert("content-type", h.1.parse().unwrap());
                    signed_headers.push(("content-type", h.1));
                }
            }
        }
        signed_headers.append(&mut vec![("X-AMZ-Date", time_str.as_str()), ("Host", host)]);

        // Support AWS delete marker feature
        for h in headers {
            if h.0 == "delete-marker" {
                request_headers.insert("x-amz-delete-marker", h.1.parse().unwrap());
                signed_headers.push(("x-amz-delete-marker", h.1));
            }
        }

        let signature = aws_v4_sign(
            self.secret_key,
            aws_v4_get_string_to_signed(
                method,
                uri,
                query_strings,
                &mut signed_headers,
                &payload,
                utc.format("%Y%m%dT%H%M%SZ").to_string(),
                &self.region,
                false,
            )
            .as_str(),
            utc.format("%Y%m%d").to_string(),
            &self.region,
            false,
        );
        let mut authorize_string = String::from_str("AWS4-HMAC-SHA256 Credential=").unwrap();
        authorize_string.push_str(self.access_key);
        authorize_string.push('/');
        authorize_string.push_str(&format!(
            "{}/{}/s3/aws4_request, SignedHeaders={}, Signature={}",
            utc.format("%Y%m%d").to_string(),
            self.region,
            sign_headers(&mut signed_headers),
            signature
        ));
        request_headers.insert(header::AUTHORIZATION, authorize_string.parse().unwrap());

        // get a client builder
        let client = Client::builder()
            .default_headers(request_headers)
            .build()
            .unwrap();

        let action;
        match method {
            "GET" => {
                action = client.get(url.as_str());
            }
            "PUT" => {
                action = client.put(url.as_str());
            }
            "DELETE" => {
                action = client.delete(url.as_str());
            }
            "POST" => {
                action = client.post(url.as_str());
            }
            _ => {
                error!("unspport HTTP verb");
                action = client.get(url.as_str());
            }
        }
        action
            .body((*payload).clone())
            .send()
            .map_err(|e| Error::ReqwestError(format!("{:?}", e)))
            .and_then(|mut res| Ok(res.handle_response()))
    }
    fn redirect_parser(&self, body: Vec<u8>, _format: Format) -> Result<String, Error> {
        // TODO: hanldle JSON for ceph
        let result = std::str::from_utf8(&body).unwrap_or("");
        let mut endpoint = "".to_string();
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
                Err(e) => return Err(Error::XMLParseError(e)),
                _ => (),
            }
            buf.clear();
        }
        Ok(endpoint)
    }
    fn update(&mut self, region: String, secure: bool) {
        self.region = region;
        self.tls = secure;
    }
    fn current_region(&self) -> Option<String> {
        Some(self.region.to_string())
    }
}

pub fn canonical_query_string(query_strings: &mut Vec<(&str, &str)>) -> String {
    query_strings.sort_by_key(|a| a.0);
    let mut encoded = form_urlencoded::Serializer::new(String::new());
    let mut upload_id = String::new();
    let number_of_qs = query_strings.len();
    for q in query_strings {
        if q.1.find('~') == Some(1) {
            if number_of_qs > 1 {
                upload_id = format!("&{}={}", q.0, q.1);
            } else {
                upload_id = format!("{}={}", q.0, q.1);
            }
        } else {
            encoded.append_pair(q.0, q.1);
        }
    }
    format!("{}{}", encoded.finish(), upload_id)
}

//CanonicalHeaders = CanonicalHeadersEntry0 + CanonicalHeadersEntry1 + ... + CanonicalHeadersEntryN
//CanonicalHeadersEntry = Lowercase(HeaderName) + ':' + Trimall(HeaderValue) + '\n'
fn canonical_headers(headers: &mut Vec<(&str, &str)>) -> String {
    let mut output = String::new();
    headers.sort_by(|a, b| a.0.to_lowercase().as_str().cmp(b.0.to_lowercase().as_str()));
    for h in headers {
        output.push_str(h.0.to_lowercase().as_str());
        output.push(':');
        output.push_str(h.1.trim());
        output.push('\n');
    }
    output
}

fn canonical_amz_headers(headers: &mut Vec<(&str, &str)>) -> String {
    let mut output = String::new();
    headers.sort_by(|a, b| a.0.to_lowercase().as_str().cmp(b.0.to_lowercase().as_str()));
    for h in headers {
        if h.0.to_lowercase().trim().starts_with("x-amz-")
            && h.0.to_lowercase().trim() != "x-amz-date"
        {
            output.push_str(h.0.to_lowercase().as_str());
            output.push(':');
            output.push_str(h.1.trim());
            output.push('\n');
        }
    }
    output
}

//SignedHeaders = Lowercase(HeaderName0) + ';' + Lowercase(HeaderName1) + ";" + ... + Lowercase(HeaderNameN)
pub fn sign_headers(headers: &mut Vec<(&str, &str)>) -> String {
    let mut output = Vec::new();
    headers.sort_by(|a, b| a.0.to_lowercase().as_str().cmp(b.0.to_lowercase().as_str()));
    for h in headers {
        output.push(h.0.to_lowercase());
    }
    output.join(";")
}

//HashedPayload = Lowercase(HexEncode(Hash(requestPayload)))
pub fn hash_payload(payload: &Vec<u8>) -> String {
    let mut sha = Sha256::new();
    sha.input(payload);
    debug!(
        "payload (size: {}) request hash = {}",
        payload.len(),
        sha.result_str()
    );
    sha.result_str()
}

fn aws_v4_canonical_request(
    http_method: &str,
    uri: &str,
    query_strings: &mut Vec<(&str, &str)>,
    headers: &mut Vec<(&str, &str)>,
    payload: &Vec<u8>,
) -> String {
    let mut input = String::new();
    input.push_str(http_method);
    input.push_str("\n");
    input.push_str(uri);
    input.push_str("\n");
    input.push_str(canonical_query_string(query_strings).as_str());
    input.push_str("\n");
    input.push_str(canonical_headers(headers).as_str());
    input.push_str("\n");
    input.push_str(sign_headers(headers).as_str());
    input.push_str("\n");
    input.push_str(hash_payload(payload).as_str());

    debug!("canonical request:\n{}", input);

    let mut sha = Sha256::new();
    sha.input_str(input.as_str());
    debug!("canonical request hash = {}", sha.result_str());
    sha.result_str()
}

pub fn aws_v4_get_string_to_signed(
    http_method: &str,
    uri: &str,
    query_strings: &mut Vec<(&str, &str)>,
    headers: &mut Vec<(&str, &str)>,
    payload: &Vec<u8>,
    time_str: String,
    region: &str,
    iam: bool,
) -> String {
    let mut string_to_signed = String::from_str("AWS4-HMAC-SHA256\n").unwrap();
    string_to_signed.push_str(&time_str);
    string_to_signed.push_str("\n");
    let endpoint_type = match iam {
        true => "iam",
        false => "s3",
    };
    unsafe {
        string_to_signed.push_str(&format!(
            "{}/{}/{}/aws4_request",
            time_str.get_unchecked(0..8),
            region,
            endpoint_type
        ));
    }
    string_to_signed.push_str("\n");
    string_to_signed.push_str(
        aws_v4_canonical_request(http_method, uri, query_strings, headers, payload).as_str(),
    );
    debug!("string_to_signed:\n{}", string_to_signed);
    return string_to_signed;
}

// HMAC(HMAC(HMAC(HMAC("AWS4" + kSecret,"20150830"),"us-east-1"),"iam"),"aws4_request")
pub fn aws_v4_sign(
    secret_key: &str,
    data: &str,
    time_str: String,
    region: &str,
    iam: bool,
) -> String {
    let mut key = String::from("AWS4");
    key.push_str(secret_key);

    let mut mac = Hmac::<sha2_256>::new(key.as_str().as_bytes());
    mac.input(time_str.as_str().as_bytes());
    let result = mac.result();
    let code_bytes = result.code();
    debug!("date_k = {}", code_bytes.to_hex());

    let mut mac1 = Hmac::<sha2_256>::new(code_bytes);
    mac1.input(region.as_bytes());
    let result1 = mac1.result();
    let code_bytes1 = result1.code();
    debug!("region_k = {}", code_bytes1.to_hex());

    let mut mac2 = Hmac::<sha2_256>::new(code_bytes1);
    match iam {
        true => mac2.input(b"iam"),
        false => mac2.input(b"s3"),
    }
    let result2 = mac2.result();
    let code_bytes2 = result2.code();
    debug!("service_k = {}", code_bytes2.to_hex());

    let mut mac3 = Hmac::<sha2_256>::new(code_bytes2);
    mac3.input(b"aws4_request");
    let result3 = mac3.result();
    let code_bytes3 = result3.code();
    debug!("signing_k = {}", code_bytes3.to_hex());

    let mut mac4 = Hmac::<sha2_256>::new(code_bytes3);
    mac4.input(data.as_bytes());
    let result4 = mac4.result();
    let code_bytes4 = result4.code();
    debug!("signature = {}", code_bytes4.to_hex());

    code_bytes4.to_hex()
}

// AWS 2 for S3
// Signature = Base64( HMAC-SHA1( YourSecretAccessKeyID, UTF-8-Encoding-Of( StringToSign ) ) );
pub fn aws_s3_v2_sign(secret_key: &str, data: &str) -> String {
    encode(&hmacsha1::hmac_sha1(secret_key.as_bytes(), data.as_bytes()))
}

// AWS 2 for S3
// StringToSign = HTTP-Verb + "\n" +
// 	Content-MD5 + "\n" +
// 	Content-Type + "\n" +
// 	Date + "\n" +
// 	CanonicalizedAmzHeaders +
// 	CanonicalizedResource;
pub fn aws_s3_v2_get_string_to_signed(
    http_method: &str,
    uri: &str,
    headers: &mut Vec<(&str, &str)>,
    content: &Vec<u8>,
) -> String {
    let mut string_to_signed = String::from_str(http_method).unwrap();
    string_to_signed.push('\n');
    if content.len() > 0 {
        string_to_signed.push_str(&format!("{:x}", md5::compute(content)));
    }
    string_to_signed.push('\n');

    for h in headers.clone() {
        if h.0.to_lowercase().trim() == "content-type" {
            string_to_signed.push_str(h.1);
            break;
        }
    }
    string_to_signed.push('\n');

    let mut has_date = false;
    for h in headers.clone() {
        if h.0.to_lowercase().trim() == "x-amz-date" {
            string_to_signed.push_str(h.1);
            has_date = true;
            break;
        }
    }
    if !has_date {
        for h in headers.clone() {
            if h.0.to_lowercase().trim() == "date" {
                string_to_signed.push_str(h.1);
                break;
            }
        }
    }
    string_to_signed.push('\n');
    string_to_signed.push_str(&canonical_amz_headers(headers));
    string_to_signed.push_str(uri);
    debug!("string to signed:\n{}", string_to_signed);
    return string_to_signed;
}

//  NOTE: This is V2 signature but not for S3 REST, Im not sure where to use
#[cfg(test)]
pub fn aws_v2_get_string_to_signed(
    http_method: &str,
    host: &str,
    uri: &str,
    query_strings: &mut Vec<(&str, &str)>,
) -> String {
    let mut string_to_signed = String::from_str(http_method).unwrap();
    string_to_signed.push_str("\n");
    string_to_signed.push_str(host);
    string_to_signed.push_str("\n");
    string_to_signed.push_str(uri);
    string_to_signed.push_str("\n");
    let qs = canonical_query_string(query_strings);
    string_to_signed.push_str(qs.as_str());
    debug!("QUERY_STRING={}", qs.as_str());
    return string_to_signed;
}

//  NOTE: This is V2 signature but not for S3 REST, Im not sure where to use
#[cfg(test)]
pub fn aws_v2_sign(secret_key: &str, data: &str) -> String {
    let mut mac = Hmac::<sha2_256>::new(secret_key.as_bytes());
    mac.input(data.as_bytes());

    let result = mac.result();
    let code_bytes = result.code();

    encode(code_bytes)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aws_v2_get_string_to_signed() {
        let mut query_strings = vec![
            ("Timestamp", "2011-10-03T15:19:30"),
            ("AWSAccessKeyId", "AKIAIOSFODNN7EXAMPLE"),
            ("Action", "DescribeJobFlows"),
            ("SignatureMethod", "HmacSHA256"),
            ("SignatureVersion", "2"),
            ("Version", "2009-03-31"),
        ];

        let string_need_signed = aws_v2_get_string_to_signed(
            "GET",
            "elasticmapreduce.amazonaws.com",
            "/",
            &mut query_strings,
        );

        assert_eq!(
            "GET\n\
             elasticmapreduce.amazonaws.com\n\
             /\n\
             AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&\
             Action=DescribeJobFlows&\
             SignatureMethod=HmacSHA256&\
             SignatureVersion=2&\
             Timestamp=2011-10-03T15%3A19%3A30\
             &Version=2009-03-31",
            string_need_signed.as_str()
        );
    }

    #[test]
    fn test_aws_v2_get_string_to_signed2() {
        let mut query_strings = vec![("uploadId", "2~abcd")];

        let string_need_signed = aws_v2_get_string_to_signed(
            "GET",
            "elasticmapreduce.amazonaws.com",
            "/",
            &mut query_strings,
        );

        assert_eq!(
            "GET\n\
             elasticmapreduce.amazonaws.com\n\
             /\n\
             uploadId=2~abcd",
            string_need_signed.as_str()
        );
    }

    #[test]
    fn test_aws_v2_sign() {
        let sig = aws_v2_sign(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "GET\n\
             elasticmapreduce.amazonaws.com\n\
             /\n\
             AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&\
             Action=DescribeJobFlows&\
             SignatureMethod=HmacSHA256&\
             SignatureVersion=2&\
             Timestamp=2011-10-03T15%3A19%3A30&\
             Version=2009-03-31",
        );
        assert_eq!("i91nKc4PWAt0JJIdXwz9HxZCJDdiy6cf/Mj6vPxyYIs=", sig.as_str());
    }
    #[test]
    fn test_hash_payload() {
        assert_eq!(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            hash_payload(&Vec::new())
        );
    }

    #[test]
    fn test_aws_v4_get_string_to_signed() {
        let mut headers = vec![
            ("X-AMZ-Date", "20150830T123600Z"),
            ("Host", "iam.amazonaws.com"),
            (
                "Content-Type",
                "application/x-www-form-urlencoded; charset=utf-8",
            ),
        ];

        let mut query_strings = vec![("Version", "2010-05-08"), ("Action", "ListUsers")];

        let string_need_signed = aws_v4_get_string_to_signed(
            "GET",
            "/",
            &mut query_strings,
            &mut headers,
            &Vec::new(),
            "20150830T123600Z".to_string(),
            "us-east-1",
            true,
        );

        assert_eq!(
            "AWS4-HMAC-SHA256\n\
             20150830T123600Z\n\
             20150830/us-east-1/iam/aws4_request\n\
             f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
            string_need_signed.as_str()
        );
    }

    #[test]
    fn test_aws_v4_sign() {
        let sig = aws_v4_sign(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "AWS4-HMAC-SHA256\n\
             20150830T123600Z\n\
             20150830/us-east-1/iam/aws4_request\n\
             f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
            "20150830".to_string(),
            "us-east-1",
            true,
        );

        assert_eq!(
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7",
            sig.as_str()
        );
    }

    #[test]
    fn test_aws_s3_v2_get_string_to_signed() {
        let mut headers = vec![
            ("Host", "johnsmith.s3.amazonaws.com"),
            ("X-AMZ-Date", "Tue, 27 Mar 2007 19:36:42 +0000"),
            ("Action", "DescribeJobFlows"),
            ("SignatureMethod", "HmacSHA256"),
            ("SignatureVersion", "2"),
            ("Version", "2009-03-31"),
        ];
        // NOTE: now we implement path style bucket only
        let string_need_signed = aws_s3_v2_get_string_to_signed(
            "GET",
            "/johnsmith/photos/puppy.jpg",
            &mut headers,
            &Vec::new(),
        );

        assert_eq!(
            "GET\n\
             \n\
             \n\
             Tue, 27 Mar 2007 19:36:42 +0000\n\
             /johnsmith/photos/puppy.jpg",
            string_need_signed.as_str()
        );
    }

    #[test]
    fn test_aws_s3_v2_sign() {
        let mut headers = vec![
            ("Host", "johnsmith.s3.amazonaws.com"),
            ("Date", "Tue, 27 Mar 2007 19:36:42 +0000"),
            ("Action", "DescribeJobFlows"),
            ("SignatureMethod", "HmacSHA256"),
            ("SignatureVersion", "2"),
            ("Version", "2009-03-31"),
        ];
        // NOTE: now we implement path style bucket only
        let string_need_signed = aws_s3_v2_get_string_to_signed(
            "GET",
            "/johnsmith/photos/puppy.jpg",
            &mut headers,
            &Vec::new(),
        );
        println!("string to signed: {}", string_need_signed);
        let sig = aws_s3_v2_sign(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            string_need_signed.as_str(),
        );
        assert_eq!("bWq2s1WEIj+Ydj0vQ697zp+IXMU=", sig);
    }
}
