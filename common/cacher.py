from functools import wraps

def cache(getter):
    '''Decorator to cache the results of getter methods
    '''
    
    name = '_' + getter.__name__
    @wraps(getter)
    def wrapper(self):
        try:
            return getattr(self, name)
        except AttributeError:
            ans = getter(self)
            setattr(self, name, ans)
            return ans
    return wrapper
