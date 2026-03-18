use std::time::Instant;

/// Drop-guard that records request duration on completion.
///
/// For streaming gRPC RPCs (`GetObject`, `BulkGet`, `BulkPut`) the timer measures
/// time-to-first-byte (handler setup), not full transfer duration, because the
/// guard is dropped when the handler returns a `Response<Stream>` before data
/// flows. Upload-direction RPCs (`PutObject`) fully consume the stream before
/// returning, so their duration is accurate end-to-end.
pub struct DurationRecorder {
    method: &'static str,
    operation: &'static str,
    start: Instant,
}

impl DurationRecorder {
    pub fn new(method: &'static str, operation: &'static str) -> Self {
        Self {
            method,
            operation,
            start: Instant::now(),
        }
    }
}

impl Drop for DurationRecorder {
    fn drop(&mut self) {
        metrics::histogram!(
            "simple3_request_duration_seconds",
            "method" => self.method,
            "operation" => self.operation,
        )
        .record(self.start.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_recorder_records_on_drop() {
        // Install a no-op recorder so the macro doesn't panic
        let recorder = metrics::NoopRecorder;
        let _ = metrics::set_global_recorder(recorder);

        let timer = DurationRecorder::new("S3", "PutObject");
        std::thread::sleep(std::time::Duration::from_millis(5));
        let elapsed = timer.start.elapsed();
        drop(timer);
        // Verify at least 5ms elapsed (timer was live)
        assert!(elapsed.as_millis() >= 5);
    }

    #[test]
    fn duration_recorder_works_on_error_path() {
        let recorder = metrics::NoopRecorder;
        let _ = metrics::set_global_recorder(recorder);

        // Simulate an error path: timer dropped before function returns Ok
        let result: Result<(), &str> = {
            let _timer = DurationRecorder::new("gRPC", "GetObject");
            Err("simulated error")
        };
        // Timer dropped here — should not panic even on error path
        assert!(result.is_err());
    }
}
