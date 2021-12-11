# Thanks to Longpoke(https://stackoverflow.com/users/80243/l%cc%b2%cc%b3o%cc%b2%cc%b3%cc%b3n%cc%b2%cc%b3%cc%b3g%cc%b2%cc%b3%cc%b3p%cc%b2%cc%b3o%cc%b2%cc%b3%cc%b3k%cc%b2%cc%b3%cc%b3e%cc%b2%cc%b3%cc%b3)
# https://stackoverflow.com/questions/1092531/event-system-in-python

class Subscriber(object):
    def __init__(self, func, callOnce=False) -> None:
        super().__init__()

        self.func = func
        self.callOnce = callOnce

class Event(list):
    """Event subscription.

    A list of callable objects. Calling an instance of this will cause a
    call to each item in the list in ascending order by index.

    Example Usage:
    >>> def f(x):
    ...     print 'f(%s)' % x
    >>> def g(x):
    ...     print 'g(%s)' % x
    >>> e = Event()
    >>> e()
    >>> e.append(f)
    >>> e(123)
    f(123)
    >>> e.remove(f)
    >>> e()
    >>> e += (f, g)
    >>> e(10)
    f(10)
    g(10)
    >>> del e[0]
    >>> e(2)
    g(2)

    """
    def __call__(self, *args, **kwargs):
        needsRemoval = []
        for subscriber in self:
            subscriber.func(*args, **kwargs)
            if subscriber.callOnce:
                needsRemoval.append(subscriber)

        for s in needsRemoval:
            self.remove(s)

    def __repr__(self):
        return f"Event({list.__repr__(self)})"

    def append(self, func) -> None:
        self.subscribe(func)

    def subscribe(self, func, callOnce=False):
        super().append(Subscriber(func, callOnce))

    def unsubscribe(self, func):
        for subscriber in self:
            if subscriber.func == func:
                self.remove(subscriber)
                break

    def once(self, func):
        self.subscribe(func, True)