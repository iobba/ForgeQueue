from dataclasses import dataclass
from datetime import timedelta
from math import ceil, isfinite, log2

from forgequeue.jobs.attempts import JobFailureKind


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    base_delay_seconds: float = 5.0
    max_delay_seconds: float = 300.0

    def __post_init__(self) -> None:
        if not isfinite(self.base_delay_seconds) or self.base_delay_seconds <= 0:
            raise ValueError("base_delay_seconds must be finite and positive")
        if not isfinite(self.max_delay_seconds) or self.max_delay_seconds <= 0:
            raise ValueError("max_delay_seconds must be finite and positive")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError(
                "max_delay_seconds must be greater than or equal to base_delay_seconds"
            )

    def can_retry(
        self,
        *,
        failure_kind: JobFailureKind,
        attempt_number: int,
        max_attempts: int,
    ) -> bool:
        self._validate_attempt_numbers(attempt_number, max_attempts)
        return (
            failure_kind is JobFailureKind.RETRYABLE and attempt_number < max_attempts
        )

    def delay_after(
        self,
        attempt_number: int,
        *,
        jitter_fraction: float,
    ) -> timedelta:
        if attempt_number < 1:
            raise ValueError("attempt_number must be positive")
        if not isfinite(jitter_fraction) or not 0 <= jitter_fraction <= 1:
            raise ValueError("jitter_fraction must be finite and between 0 and 1")

        delay_ceiling = self._delay_ceiling(attempt_number)
        delay_floor = delay_ceiling / 2
        delay_seconds = delay_floor + (delay_floor * jitter_fraction)
        return timedelta(seconds=delay_seconds)

    def _delay_ceiling(self, attempt_number: int) -> float:
        exponent = attempt_number - 1
        exponent_at_cap = ceil(log2(self.max_delay_seconds / self.base_delay_seconds))
        if exponent >= exponent_at_cap:
            return self.max_delay_seconds
        return self.base_delay_seconds * (2**exponent)

    @staticmethod
    def _validate_attempt_numbers(
        attempt_number: int,
        max_attempts: int,
    ) -> None:
        if attempt_number < 1:
            raise ValueError("attempt_number must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if attempt_number > max_attempts:
            raise ValueError("attempt_number must not exceed max_attempts")
