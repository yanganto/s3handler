//! Error management module
use failure_derive::*;

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Could not load file: {}", 0)]
    LoadError(std::io::Error),
    // #[fail(display = "The response should be JSON: {}", 0)]
    // JSONParseError(serde_json::Error),
    #[fail(display = "The response should be XML: {}", 0)]
    XMLParseError(quick_xml::Error),
    #[fail(display = "Unexpected input from user: {}", 0)]
    UserError(&'static str),
    #[fail(display = "Can not make a request: {}", 0)]
    ReqwestError(String),
    #[fail(display = "Error in RequestPool: {}", 0)]
    RequestPoolError(String),
    #[fail(display = "The resource with uncorrect scheme")]
    SchemeError(),
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
