import datetime
import functools
import logging
from typing import Optional, Callable, List, Any

from utils.StoppableThreadWithReturnValue import StoppableThreadWithReturnValue

LOGGER: logging.Logger = logging.getLogger(__name__)


class timeout:
    """
    USE THIS CAREFULLY!

    This method is able to go with multi-threading, but if a function times out, the thread will be stopped forcefully.
    Therefore all resources occupied and states set and stuff will be no more. This can lead to confusing behaviour, be
    aware of that.

    Furthermore it is not a good design pattern to forcefully let a function timeout from outer scope, but sometimes it
    necessary.

    Decorator to let a function timeout
    """

    def __init__(self, timeout: int, timeout_caching: int = None):
        """
        :param timeout: after how many secs to time out
        :param timeout_caching: caches the timeout result for the amount of secs. This is to prevent a timeouting retry
        """
        self.timeout: int = timeout
        self.timeout_caching: int = timeout_caching
        self.last_timed_out: Optional[datetime.datetime] = None

    def __call__(self, func: Callable[[List[Any], List[Any]], Any]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if self.timeout_caching is not None \
                    and self.last_timed_out \
                    and self.last_timed_out + datetime.timedelta(seconds=self.timeout_caching) > \
                    datetime.datetime.now():
                return None

            name: str = f'{args[0].__class__.__name__ + "." if args and hasattr(args[0], func.__name__) else ""}' \
                        f'{func.__name__} thread'
            t = StoppableThreadWithReturnValue(target=func, args=args, kwargs=kwargs, name=name)
            t.setDaemon(True)
            t.start()
            result = t.join(self.timeout)
            if t.is_alive():
                LOGGER.warning(f'{name} timed out after {self.timeout} secs')
                t.stop()
                if self.timeout_caching is not None:
                    self.last_timed_out = datetime.datetime.now()
            return result

        return wrapper
