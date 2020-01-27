//! Error management module

#[derive(Debug)]
pub enum Error {
    LoadError(std::io::Error),
    JSONParseError(serde_json::Error),
    XMLParseError(quick_xml::Error),
    UserError(&'static str),
    ReqwestError(),
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
