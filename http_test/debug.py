from functools import wraps


def debug_entry(func):
    '''
    Decorator that print entry of function
    :param func:
    :return:
    '''

    @wraps(func)
    def wrapper(*args, **kwargs):
        print("=> Enter " + func.__name__)
        result = func(*args, **kwargs)
        return result

    return wrapper
