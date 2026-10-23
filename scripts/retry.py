"""Retry dengan exponential backoff dan jitter.

Dipakai oleh batch_ingest.py. Pada API sungguhan, adapter sumber mengubah
timeout / HTTP 429 / 5xx menjadi TransientError (dan menghormati header
Retry-After); error permanen seperti 400/404 TIDAK diubah, sehingga tidak di-retry.
"""
import logging
import random
import time


class TransientError(Exception):
    """Error sementara yang layak dicoba ulang."""


def call_with_retry(fn, *, max_retries, base_delay, label="request", logger=None):
    """Panggil fn() hingga max_retries kali. Jeda: base_delay * 2^(percobaan-1) + jitter."""
    logger = logger or logging.getLogger("retry")
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except TransientError as error:
            if attempt == max_retries:
                logger.error("%s gagal setelah %s percobaan: %s", label, attempt, error)
                raise
            wait = base_delay * (2 ** (attempt - 1)) + random.uniform(0, base_delay)
            logger.warning(
                "%s percobaan %s/%s gagal (%s). Coba lagi dalam %.2fs",
                label, attempt, max_retries, error, wait,
            )
            time.sleep(wait)
