# Thanks to Longpoke(https://stackoverflow.com/users/80243/l%cc%b2%cc%b3o%cc%b2%cc%b3%cc%b3n%cc%b2%cc%b3%cc%b3g%cc%b2%cc%b3%cc%b3p%cc%b2%cc%b3o%cc%b2%cc%b3%cc%b3k%cc%b2%cc%b3%cc%b3e%cc%b2%cc%b3%cc%b3)
# https://stackoverflow.com/questions/1092531/event-system-in-python

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
        for f in self:
            f(*args, **kwargs)

    def __repr__(self):
        return f"Event({list.__repr__(self)})"

    def subscribe(self, func):
        self.append(func)

    def unsubscribe(self, func):
        self.remove(func)