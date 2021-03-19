from concurrent.futures import ThreadPoolExecutor

def runInThread(func, *args, **kwargs):
    ThreadPoolExecutor(max_workers=1).submit(func, *args, **kwargs)