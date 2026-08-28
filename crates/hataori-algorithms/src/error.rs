use hataori_runtime::RuntimeError;
use std::fmt;

#[derive(Debug)]
pub enum AlgorithmError {
    InvalidOptions(&'static str),
    InputShape(&'static str),
    Protocol(&'static str),
    Callback { index: usize, message: String },
    Runtime(RuntimeError),
}
impl fmt::Display for AlgorithmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions(s) => write!(f, "invalid options: {s}"),
            Self::InputShape(s) => write!(f, "invalid input shape: {s}"),
            Self::Protocol(s) => write!(f, "algorithm protocol error: {s}"),
            Self::Callback { index, .. } => write!(f, "callback failed at index {index}"),
            Self::Runtime(e) => write!(f, "runtime error: {e}"),
        }
    }
}
impl std::error::Error for AlgorithmError {}
impl From<RuntimeError> for AlgorithmError {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}
