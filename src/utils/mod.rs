use quick_xml::{events::Event, Reader};
use regex::Regex;
use url::Url;

use crate::error::Error;

pub const DEFAULT_REGION: &'static str = "us-east-1";

/// # Flexible S3 format parser
/// - bucket - the objeck belonge to which
/// - key - the object key
/// - mtime - the last modified time
/// - etag - the etag calculated by server (MD5 in general)
/// - storage_class - the storage class of this object
/// - size - the size of the object
/// ```
/// use s3handler::{S3Object, S3Convert};
///
/// let s3_object = S3Object::from("s3://bucket/object_key");
/// assert_eq!(s3_object.bucket, Some("bucket".to_string()));
/// assert_eq!(s3_object.key, Some("/object_key".to_string()));
/// assert_eq!("s3://bucket/object_key".to_string(), String::from(s3_object));
///
/// let s3_object: S3Object = S3Object::from("S3://bucket/object_key");
/// assert_eq!("s3://bucket/object_key".to_string(), String::from(s3_object));
///
/// let s3_object: S3Object = S3Object::from("/bucket/object_key");
/// assert_eq!("s3://bucket/object_key".to_string(), String::from(s3_object));
///
/// let s3_object: S3Object = S3Object::from("bucket/object_key");
/// assert_eq!("s3://bucket/object_key".to_string(), String::from(s3_object));
/// ```
#[derive(Debug, Clone, Default)]
pub struct S3Object {
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub mtime: Option<String>, // TODO: use some datetime type
    pub etag: Option<String>,
    pub storage_class: Option<String>,
    pub size: Option<usize>,
    pub mime: Option<String>,
}

impl From<&str> for S3Object {
    fn from(s3_path: &str) -> Self {
        if let Ok(url_parser) = Url::parse(s3_path) {
            let bucket = match url_parser.host_str() {
                Some(h) if h != "" => Some(h.to_string()),
                _ => None,
            };
            match url_parser.path() {
                "/" | "" => S3Object {
                    bucket,
                    key: None,
                    mtime: None,
                    etag: None,
                    storage_class: None,
                    size: None,
                    mime: None,
                },
                _ => S3Object {
                    bucket,
                    key: Some(url_parser.path().to_string()),
                    mtime: None,
                    etag: None,
                    storage_class: None,
                    size: None,
                    mime: None,
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
    fn new_from_uri(path: &str) -> Self;
    fn new(
        bucket: Option<String>,
        key: Option<String>,
        mtime: Option<String>,
        etag: Option<String>,
        storage_class: Option<String>,
        size: Option<usize>,
    ) -> Self;
}

impl S3Convert for S3Object {
    fn virtural_host_style_links(&self, host: String) -> (String, String) {
        match self.bucket.clone() {
            Some(b) => (
                format!("{}.{}", b, host),
                self.key.clone().unwrap_or_else(|| "/".to_string()),
            ),
            None => (host, "/".to_string()),
        }
    }

    fn path_style_links(&self, host: String) -> (String, String) {
        match self.bucket.clone() {
            Some(b) => (
                host,
                format!(
                    "/{}{}",
                    b,
                    self.key.clone().unwrap_or_else(|| "/".to_string())
                ),
            ),
            None => (host, "/".to_string()),
        }
    }

    fn new_from_uri(uri: &str) -> S3Object {
        let re = Regex::new(r#"/?(?P<bucket>[A-Za-z0-9\-\._]+)(?P<object>[A-Za-z0-9\-\._/]*)\s*"#)
            .unwrap();
        let caps = re.captures(uri).expect("S3 object uri format error.");
        if &caps["object"] == "" || &caps["object"] == "/" {
            S3Object {
                bucket: Some(caps["bucket"].to_string()),
                key: None,
                mtime: None,
                etag: None,
                storage_class: None,
                size: None,
                mime: None,
            }
        } else {
            S3Object {
                bucket: Some(caps["bucket"].to_string()),
                key: Some(caps["object"].to_string()),
                mtime: None,
                etag: None,
                storage_class: None,
                size: None,
                mime: None,
            }
        }
    }

    fn new(
        bucket: Option<String>,
        object: Option<String>,
        mtime: Option<String>,
        etag: Option<String>,
        storage_class: Option<String>,
        size: Option<usize>,
    ) -> S3Object {
        let key = match object {
            None => None,
            Some(b) => {
                if b.starts_with('/') {
                    Some(b)
                } else {
                    Some(format!("/{}", b))
                }
            }
        };

        S3Object {
            bucket,
            key,
            mtime,
            etag,
            storage_class,
            size,
            mime: None,
        }
    }
}

/// The request URL style
#[derive(Clone, Debug)]
pub enum UrlStyle {
    /// Path style URL
    /// The bucket name will be listed in the URI
    PATH,
    /// Virtual hosted URL
    /// The bucket name will prefix on the host.
    HOST,
}

impl Default for UrlStyle {
    fn default() -> Self {
        UrlStyle::PATH
    }
}

pub fn s3object_list_xml_parser(body: &str) -> Result<Vec<S3Object>, Error> {
    let mut reader = Reader::from_str(body);
    let mut output = Vec::new();
    let mut in_name_tag = false;
    let mut in_key_tag = false;
    let mut in_mtime_tag = false;
    let mut in_etag_tag = false;
    let mut in_storage_class_tag = false;
    let mut in_size_tag = false;
    let mut bucket = String::new();
    let mut key = String::new();
    let mut mtime = String::new();
    let mut etag = String::new();
    let mut storage_class = String::new();
    let mut size = 0;
    let mut buf = Vec::new();
    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name() {
                b"Name" => in_name_tag = true,
                b"Key" => in_key_tag = true,
                b"LastModified" => in_mtime_tag = true,
                b"ETag" => in_etag_tag = true,
                b"StorageClass" => in_storage_class_tag = true,
                b"Size" => in_size_tag = true,
                _ => {}
            },
            Ok(Event::End(ref e)) => match e.name() {
                b"Name" => output.push(S3Convert::new(
                    Some(bucket.clone()),
                    None,
                    None,
                    None,
                    None,
                    None,
                )),
                b"Contents" => output.push(S3Convert::new(
                    Some(bucket.clone()),
                    Some(key.clone()),
                    Some(mtime.clone()),
                    Some(etag[1..etag.len() - 1].to_string()),
                    Some(storage_class.clone()),
                    Some(size.clone()),
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
                if in_size_tag {
                    size = e
                        .unescape_and_decode(&reader)
                        .unwrap()
                        .parse::<usize>()
                        .unwrap_or_default();
                    in_size_tag = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(Error::XMLParseError(e)),
            _ => (),
        }
        buf.clear();
    }
    Ok(output)
}

pub fn upload_id_xml_parser(res: &str) -> Result<String, Error> {
    let mut reader = Reader::from_str(res);
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
                    return Ok(e.unescape_and_decode(&reader).unwrap());
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(Error::XMLParseError(e).into());
            }
            _ => (),
        }
        buf.clear();
    }
    return Err(Error::FieldNotFound("upload_id"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_upload_id() {
        let response = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>ant-lab</Bucket><Key>test-s3handle-big-v4-async-1611237128</Key><UploadId>6lxsB3W3e.Gf6D2mXrDpscWxHeVNloGTDMPUmomjmRYbQ5j4K31mMTcSdzWTHY6cSnA_S36J6GKY.aAxAkjcTXGb3btEB_O9XSpIy9mFRIlYAo0DH_Oyg9KF6D5fppQzPfYBy_OZTIncT6zK_zQIyQ--</UploadId></InitiateMultipartUploadResult>";
        let upload_id = upload_id_xml_parser(response);
        assert!(upload_id.is_ok());
        assert_eq!(upload_id.unwrap(), "6lxsB3W3e.Gf6D2mXrDpscWxHeVNloGTDMPUmomjmRYbQ5j4K31mMTcSdzWTHY6cSnA_S36J6GKY.aAxAkjcTXGb3btEB_O9XSpIy9mFRIlYAo0DH_Oyg9KF6D5fppQzPfYBy_OZTIncT6zK_zQIyQ--");
    }
}
