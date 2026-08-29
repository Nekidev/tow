pub trait TraceError {
    fn err_warn(self) -> Self;
    fn err_error(self) -> Self;
    fn err_trace(self) -> Self;
}

impl<T> TraceError for Result<T, anyhow::Error> {
    fn err_warn(self) -> Self {
        if let Err(error) = &self {
            tracing::warn!("{error:?}");
        }

        self
    }

    fn err_error(self) -> Self {
        if let Err(error) = &self {
            tracing::error!("{error:?}");
        }

        self
    }

    fn err_trace(self) -> Self {
        if let Err(error) = &self {
            tracing::trace!("{error:?}");
        }

        self
    }
}
