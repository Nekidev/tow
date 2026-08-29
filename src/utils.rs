use std::time::{SystemTime, UNIX_EPOCH};

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

/// Returns the current time in milliseconds since the UNIX epoch.
///
/// This function retrieves the current system time, calculates the duration since the UNIX epoch,
/// and converts it to milliseconds. It is useful for timestamping events or measuring time
/// intervals in applications.
///
/// Returns:
/// * `u64` - The current time in milliseconds since the UNIX epoch.
///
/// Panics:
/// * If the system time is before the UNIX epoch, which is unlikely but possible on some systems.
///
/// Returns:
/// [`u64`] - The current time in milliseconds since the UNIX epoch.
pub fn utc_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}
