import time
from functools import wraps


def timethis(func):
    '''
    Decorator for time function
    :param func:
    :return:
    '''
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        print("=== Totally costs %.6f seconds ===" % (time.time() - start))
        return result

    return wrapper