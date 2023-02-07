#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Could not load file: {0:?}")]
    LoadError(std::io::Error),
    #[error("The response should be XML: {0:?}")]
    XMLParseError(quick_xml::Error),
    #[error("The field {0} not found in response")]
    FieldNotFound(&'static str),
    #[error("Unexpected input from user: {0}")]
    UserError(&'static str),
    #[error("Can not make a request: {0}")]
    ReqwestError(String),
    #[error("Error in RequestPool: {0}")]
    RequestPoolError(String),
    #[error("The resource with uncorrect scheme")]
    SchemeError(),
    #[error("Write without bucket")]
    ModifyEmptyBucketError(),
    #[error("Pull bucket wihout object")]
    PullEmptyObjectError(),
    #[error("Resource url error: {0}")]
    ResourceUrlError(String),
    #[error("Pools should be initialized before pull or push on canal")]
    PoolUninitializeError(),
    #[error("Header parsing error")]
    HeaderParsingError(),
    #[error("No object specified to move")]
    NoObject(),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::LoadError(err)
    }
}

impl From<&'static str> for Error {
    fn from(s: &'static str) -> Self {
        Error::UserError(s)
    }
}

impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Self {
        Error::ResourceUrlError(err.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::ReqwestError(err.to_string())
    }
}

impl From<reqwest::header::ToStrError> for Error {
    fn from(_err: reqwest::header::ToStrError) -> Self {
        Error::HeaderParsingError()
    }
}
