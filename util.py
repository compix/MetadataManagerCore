from functools import wraps
from time import time
import os

def timeit(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r args:[%r, %r] took: %0.0fms' % (f.__name__, args, kw, (te-ts) * 1000.0))
        return result
    return wrap

def replaceGermanCharacters(input: str):
    return input.replace('ö', 'oe').replace('ü', 'ue').replace('ä', 'ae').replace('Ä', 'AE').replace('Ü', 'UE').replace('Ö', 'OE').replace('ß', 'ss').replace('ẞ', 'SS')

def winapi_path(filepath: str):
    path = os.path.abspath(filepath)

    if path.startswith(u"\\\\?"):
        return path
        
    if path.startswith(u"\\\\"):
        return u"\\\\?\\UNC\\" + path[2:]
        
    return u"\\\\?\\" + path

def resolvePath(filepath: str):
    if os.name == 'nt':
        return winapi_path(filepath)

    return filepath