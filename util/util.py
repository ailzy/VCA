__author__ = 'lizhengyang'

def singleton(cls):
    """used to make a class to be singleton.
    In fact, a better choice is to use cache.py
    :param cls:
    :return:
    """
    instances = {}
    def _singleton(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton
