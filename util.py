from functools import wraps
from time import time

def timeit(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r args:[%r, %r] took: %0.0fms' % (f.__name__, args, kw, (te-ts) * 1000.0))
        return result
    return wrap