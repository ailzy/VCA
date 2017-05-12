"""Decorators and data structures that cache calculations.
    """
import atexit
import inspect
import weakref

__all__ = ('cached_property', 'memoize', 'singleton')
__docformat__ = 'restructuredtext'


def _register_cache(key, cache):
    """Registers an internal (memoize) cache with `_all_caches_`, which
        is a mapping from weakref(function or class or instance) -> cache."""
    _all_caches_[key] = cache


_all_caches_ = weakref.WeakKeyDictionary()
# Upon exit, caches are usually *not* properly destructed as Python doesn't
# guarantee to call objects' __del__().  So we do it ourselves.  An alternate
# is to clean up the caches in a global variable's __del__(), but that does
# not work well with Shove since Shove does heavy lifting in its __del__,
# while modules that Shove depends on (e.g., urllib) are already gone.
atexit.register(lambda: [v.clear() for v in _all_caches_.values()])


def memoize(func, cache_type=dict):
    """Returns a memoized version of `func`, which can be a function, a
        member function, or a class.
        
        >>> @memoize
        ... def factorial(n):
        ...     print('Compute factorial for %d' % n)
        ...     if n < 2: return 1
        ...     return factorial(n - 1) * n
        >>> factorial(5)    # doctest: +ELLIPSIS
        Compute factorial for 5
        ...
        >>> factorial(6)
        Compute factorial for 6
        720
        
        >>> @memoize
        ... class Cached:
        ...    def __init__(self, x, **kwargs):
        ...        print('Create Cached instance: %r, %r' % (x, kwargs))
        >>> x = Cached(5, key='abc')
        Create Cached instance: 5, {'key': 'abc'}
        >>> x = Cached(5, key='abc')
        
        For static or class methods, ``@memoize`` should appear below the other
        decorator (``@staticmethod`` or ``@classmethod``).
        
        If `func` has ``self`` as the first argument, it is deemed as a member
        method and a special rule kicks in: the cache will be stored with the
        instance.  The benefit is that the cache goes away with the instance.
        
        :attention:
        Function arguments and keyword arguments should be immutable.
        :attention:
        Don't in-place modify the cached outputs (e.g., numpy arrays).
        
        :note:
        "What's New in Python 2.1" mentions that weakref can help write
        memoize without keeping all the objects in cache.  However, I find
        it difficult to "weakref" very basic types (int, list, dict) so it
        is hard to write a general-purpose memoize decorator.
        
        :todo: Add a version that supports common mutable types (e.g., list),
        useful for quickly converting functions to cached ones.
        :todo: Add statistics (for debug/information purposes).
        
        """
    # Using _memoize_method makes sense for member functions since the
    # cache (stored with an instance) will be garbage-collected when the
    # instance is gone.  This benefit doesn't apply to class methods so
    # we stick with the plain implementation for class methods.
    # Note: Only before it becomes an actual member function.
    if _is_unbounded_method(func):
        return _memoize_method(func, cache_type)

    cache = cache_type()
    keygen = _gen_memoize_key(func)

def memoized(*args, **kwargs):
    key = keygen(*args, **kwargs)
    try:
        return cache[key]
    except KeyError:
        return cache.setdefault(key, _unzip_gen(func(*args, **kwargs)))

    memoized = _copy_signature(func, memoized, memoize_cache=cache)
    _register_cache(memoized, cache)
    return memoized


def _memoize_method(func, cache_type=dict):
    """`memoize` for member methods and class methods.  It stores the cache
        with the instance (for member methods) or with the class (for class
        methods).
        
        For member methods, using `_memoize_method` is better than the plain-
        vanilla `memoize` in that the cache will be garbage-collected with the
        instance.  However, our `memoize` detects the use case and automatically
        delegates to `_memoize_method` (so in general just use `memoize`).
        
        >>> class Add:
        ...     def __init__(self, x):
        ...         self.x = x
        ...     @_memoize_method
        ...     def calc(self, y):
        ...         print('Adding %d and %d' % (self.x, y))
        ...         return self.x + y
        >>> a1, a2 = Add(1), Add(2)
        >>> a1.calc(3)
        Adding 1 and 3
        4
        >>> a1.calc(3)
        4
        >>> a2.calc(3)
        Adding 2 and 3
        5
        
        """
    cache_name = '_%s_memoize_cache' % (func.__name__,)
    keygen = _gen_memoize_key(func)
    
    def memoized(self, *args, **kwargs):
        # Compared to cache = self.__dict__.setdefault(cache_name, {}),
        # the following code is faster, and works for classmethod too.
        # Don't use hasattr/getattr which may search up to base classes.
        try:
            cache = self.__dict__[cache_name]
        except KeyError:
            setattr(self, cache_name, cache_type())
            cache = self.__dict__[cache_name]
            _register_cache(self, cache)
        key = keygen(*args, **kwargs)
        try:
            return cache[key]
        except KeyError:
            return cache.setdefault(
                                    key, _unzip_gen(func(self, *args, **kwargs)))

    return _copy_signature(func, memoized)


def _unzip_gen(x):
    import types
    return tuple(x) if isinstance(x, types.GeneratorType) else x


def _copy_signature(src, dst, add_keywords={}, remove_arg0=[], **extra_attrs):
    """Copy the function signature of `src` to `dst`.  Optionally, set
        extra keyword parameters and/or extra attrs of `dst`.
        
        `src` can be normal functions, bound and unbound class member functions,
        and classes.  When `src` is a class, the signature of ``src.__init__``
        (without the 'self' argument) is copied over.
        
        """
    from decorator import FunctionMaker
    
    actual_func = src
    if inspect.isclass(src):                                # classes
        actual_func, remove_arg0 = src.__init__, ['self']
    elif not inspect.ismethod(src):                         # normal functions
        # A member function is NOT a member function before it becomes a
        # bound or unbound method, i.e., still inside the class definition.
        pass        # src.__dict__ will be copied over by FunctionMaker.
    elif src.__self__ is None:                              # unbound methods
        assert not src.__dict__ and src.__dict__ is src.im_func.__dict__
    else:                                                   # bound methods
        assert not src.__dict__ and src.__dict__ is src.im_func.__dict__
        remove_arg0 = ['self', 'cls']
    signature, defaults = _modify_argspec(
                                          actual_func, remove_arg0=remove_arg0, add_keywords=add_keywords)
    fmaker = FunctionMaker(src, signature=signature, defaults=defaults)
    
    # If <newfunc>.func_code.co_filename and func_code.co_firstlineno were
    # not read-only, we should change them to the values from ``src``.
    return fmaker.make(
                       'def %(name)s(%(signature)s): return _NeW_fUnC_(%(signature)s)',
                       dict(_NeW_fUnC_=dst), **extra_attrs)


def _is_unbounded_method(func):
    try:
        spec = inspect.getargspec(func)
    except TypeError:
        return False
    return spec.args and spec.args[0] == 'self' and not inspect.ismethod(func)


def _get_argspec(func):
    try:
        spec = inspect.getargspec(func)
        arg0 = ['self'] if _is_unbounded_method(func) else []
    except TypeError:
        try:
            spec = inspect.getargspec(func.__init__)
        except TypeError:
            spec = inspect.getargspec(func.__new__)
        arg0 = ['self', 'cls']
    return spec, arg0


def _modify_argspec(func, remove_arg0=[], add_keywords={}):
    # Example: ('x, y, *args', (2,)) is returned for this member function:
    #     def __init__(self, x, y=2, *args):
    # if remove_arg0 = ['self'].
    args, varargs, keywords, defaults = inspect.getargspec(func)
    if remove_arg0:
        assert args[0] in remove_arg0, (args, varargs, keywords, defaults)
        del args[0]
    if add_keywords:
        assert all(k not in args for k in add_keywords), (args, add_keywords)
        if False and varargs is None:
            # This "fancy" approach doesn't work with the memoized function.
            keys, vals = zip(*sorted(add_keywords.items()))
            args.extend(keys)
            defaults = (defaults or ()) + vals
        elif keywords is None:
            keywords = '__kwargs__'
            assert keywords not in args and keywords != varargs
    signature = inspect.formatargspec(args, varargs, keywords, defaults,
                                      formatvalue=lambda _: '')
    return (signature[1:-1], defaults)


def _gen_memoize_key(func):
    """The key used in memoizing a function, a class, or a class method."""
    spec, arg0 = _get_argspec(func)
    if spec.keywords is None:
        # func doesn't accept arbitrary keyword arguments (although one
        # can still use keywords on normal arguments).
        if spec.varargs is None and len(spec.args) == 1 + bool(arg0):
            keygen = lambda x: x
        else:
            keygen = lambda *args: args
    else:
        def keygen(*args, **kwargs):
            return (args, frozenset(kwargs.items())) if kwargs else (args,)
    return _copy_signature(func, keygen, remove_arg0=arg0)


class cached_property(object):
    """Similar to the built-in ``property`` but caches the result.
        
        >>> class AddThree(object):
        ...     def __init__(self, x):
        ...         self.x = x
        ...     @cached_property
        ...     def value(self):
        ...         print('Add 3 to %r' % self.x)
        ...         return self.x + 3
        >>> a = AddThree(5)
        >>> a.value
        Add 3 to 5
        8
        >>> a.value
        8
        >>> del a.value; a.value
        Add 3 to 5
        8
        
        :note: This is very similar to property.Lazy of zope.cachedescriptors.
        
        """
    __slots__ = ('func', 'name')
    
    def __init__(self, func):
        self.func = func
        self.name = func.__name__
    
    def __get__(self, instance, owner_class):
        if instance is None:    # Accessed through the owner_class
            return self.func    # A cute trick to return the function __doc__
        
        ret = self.func(instance)
        setattr(instance, self.name, ret)
        return ret


class Singleton(type):
    """A metaclass that helps memoize class instances (i.e., singletons).
        See `singleton` for the usage and benefit.
        
        By default, the arguments to `__init__` are combined to form the key
        in the cache.  This can be customized by defining a ``_singleton_key``
        (staticmethod or classmethod).  One may further define `__init__` to
        only take the cache key as input, which means multiple variants of
        input arguments can be "normalized" at ``_singleton_key``.
        
        """
    def __init__(cls, name, bases, dict):
        type.__init__(cls, name, bases, dict)
        
        keygen = nwk = getattr(cls, '_singleton_key', None)
        if keygen:
            spec, _ = _get_argspec(cls)
            nwk = not (spec.keywords or spec.varargs or len(spec.args) != 2)
        else:
            try:
                keygen = _gen_memoize_key(cls)
            except TypeError:
                return  # we don't care about {object,singleton}.__init__
        cls._singleton_cache = cache = ({}, keygen, bool(nwk))
        _register_cache(cls, cache[0])
    
    def __call__(cls, *args, **kwargs):
        # Try to load from the instance cache.
        cache, keygen, new_with_key = cls._singleton_cache
        key = keygen(*args, **kwargs)
        if key in cache:
            return cache[key]
        # Create a new instance.
        if new_with_key:
            new_inst = type.__call__(cls, key)
        else:
            new_inst = type.__call__(cls, *args, **kwargs)
        assert type(new_inst) is cls, (cls, new_inst)
        assert key not in cache, ('recursive ref?', key)
        return cache.setdefault(key, new_inst)


class singleton(object):
    """
        The benefit over `memoize` is that the class is still a class, and
        ``type(self)`` would still trigger the caching mechanism.  E.g.,
        
        >>> class Base(object):
        ...     def __init__(self, x):
        ...         self.x = x
        ...         print 'Create instance', type(self).__name__, x
        ...     def copy(self):
        ...         return type(self)(self.x)
        >>> @memoize
        ... class A(Base): pass
        >>> class B(Base, singleton): pass
        >>> A(1).copy() is A(1), B(2).copy() is B(2)
        Create instance A 1
        Create instance A 1
        Create instance B 2
        (False, True)
        
        Two "advanced" use cases: key normalization and ID-based cache.
        
        >>> class KeyNorm(singleton):
        ...     @classmethod
        ...     def _singleton_key(cls, name, suffix):
        ...         return name + '_' + suffix.upper()
        ...     def __init__(self, key):
        ...         print 'Create KeyNorm instance', key
        ...         self.name = key
        >>> KeyNorm('Example', 'a').name
        Create KeyNorm instance Example_A
        'Example_A'
        >>> KeyNorm('Example', 'A').name
        'Example_A'
        
        >>> class IDBased(singleton):
        ...     @classmethod
        ...     def _singleton_key(cls, name, **kwargs):
        ...         return name
        ...     def __init__(self, name, **kwargs):
        ...         self.args = kwargs
        >>> x = IDBased('Example', apple=2, pear=3)
        >>> IDBased('Example').args['pear']
        3
        
        """
    __metaclass__ = Singleton
