import time
from contextlib import contextmanager

@contextmanager
def record_execution_time(logger, step_name: str):
    start = time.time()
    yield
    duration = round(time.time() - start, 2)
    logger.info(f"{step_name} completed in {duration}s")
