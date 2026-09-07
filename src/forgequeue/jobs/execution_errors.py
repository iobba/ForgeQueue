from forgequeue.jobs.attempts import JobFailureKind


class JobExecutionError(Exception):
    def __init__(
        self,
        *,
        failure_kind: JobFailureKind,
        error_code: str,
        safe_message: str,
    ) -> None:
        normalized_code = error_code.strip()
        normalized_message = safe_message.strip()

        if not normalized_code:
            raise ValueError("error_code must not be blank")
        if len(normalized_code) > 100:
            raise ValueError("error_code must not exceed 100 characters")
        if not normalized_message:
            raise ValueError("safe_message must not be blank")

        self.failure_kind = failure_kind
        self.error_code = normalized_code
        self.safe_message = normalized_message
        super().__init__(normalized_message)


class RetryableJobError(JobExecutionError):
    def __init__(self, *, error_code: str, safe_message: str) -> None:
        super().__init__(
            failure_kind=JobFailureKind.RETRYABLE,
            error_code=error_code,
            safe_message=safe_message,
        )


class PermanentJobError(JobExecutionError):
    def __init__(self, *, error_code: str, safe_message: str) -> None:
        super().__init__(
            failure_kind=JobFailureKind.PERMANENT,
            error_code=error_code,
            safe_message=safe_message,
        )
