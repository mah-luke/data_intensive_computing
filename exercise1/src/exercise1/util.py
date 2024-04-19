from datetime import time
import logging

from mrjob.parse import wraps

LOG = logging.getLogger()


def timed(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        elapsed = time() - start
        LOG.info(f"Function {func} executed in {elapsed}")
        return result
