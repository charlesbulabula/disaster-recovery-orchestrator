import time, functools


def retry(attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            wait = delay
            for n in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    if n == attempts:
                        raise
                    time.sleep(wait)
                    wait *= backoff
        return wrapper
    return decorator

# rev 20260518164315-58fa98bc
