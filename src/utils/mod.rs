use regex::Regex;
use url::Url;

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
#[derive(Debug, Clone, Default)]
pub struct S3Object {
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub mtime: Option<String>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
}

impl From<String> for S3Object {
    fn from(s3_path: String) -> Self {
        if let Ok(url_parser) = Url::parse(&s3_path) {
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
                },
                _ => S3Object {
                    bucket,
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
            bucket,
            key,
            mtime,
            etag,
            storage_class,
        }
    }
}
