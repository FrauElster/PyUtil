import datetime
import functools
from dataclasses import dataclass
from typing import Optional, Any, Union, Callable, List


@dataclass
class CallableCache:
    refresh_rate: Union[int, str]
    callable: Callable[[], Any]
    cache: Any = None
    last_updated: Optional[datetime.datetime] = None
    key: Optional[str] = None

    def __call__(self, *args, **kwargs):
        """
        If "_force_refresh=True" in kwargs, it will force a refresh
        :param args:
        :param kwargs:
        :return:
        """
        if "_force_refresh" in kwargs.keys() and kwargs["_force_refresh"]:
            kwargs.pop("_force_refresh")
            self.cache = self.callable(*args, **kwargs)
            self.last_updated = datetime.datetime.now()
            return self.cache
        if "_force_refresh" in kwargs.keys():
            kwargs.pop("_force_refresh")
        if isinstance(self.refresh_rate, str):
            if self.refresh_rate.lower() == "never":
                return self.cache
            try:
                now: datetime.datetime = datetime.datetime.now()
                today: datetime.datetime = datetime.datetime(year=now.year, month=now.month, day=now.day)
                hour: int = int(self.refresh_rate.split(":")[0])
                minute: int = int(self.refresh_rate.split(":")[1])
                update_date: datetime.datetime = datetime.datetime(year=now.year, month=now.month, day=now.day,
                                                                   hour=hour, minute=minute)
                if not self.last_updated or (self.last_updated < today and update_date < now):
                    self.cache = self.callable(*args, **kwargs)
                    self.last_updated = datetime.datetime.now()
                return self.cache
            except ValueError as e:
                raise e.__class__(f'{self.refresh_rate} was not parsable in {self.key if self.key else str(self)}')

        if self.last_updated and isinstance(self.refresh_rate, int) \
                and self.last_updated + datetime.timedelta(seconds=self.refresh_rate) > datetime.datetime.now():
            return self.cache
        self.cache = self.callable(*args, **kwargs)
        self.last_updated = datetime.datetime.now()
        return self.cache


def callable_cache(refresh_rate: Union[str, int] = 10):
    """
    Caches the result of a function
    :param refresh_rate: determines how long a result is cached
    If "_force_refresh=True" in kwargs, it will force a refresh
    :return:
    """

    def decorator(func: Callable[[List[Any], List[Any]], Any]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """
            If "_force_refresh=True" in kwargs, it will force a refresh
            :param args:
            :param kwargs:
            :return:
            """
            self = args[0]
            if not hasattr(self, "_query_function_dict") or not self._query_function_dict:
                self._query_function_dict = {}
            if func not in self._query_function_dict.keys():
                self._query_function_dict[func] = CallableCache(
                    refresh_rate=refresh_rate,
                    key=func.__name__,
                    callable=func
                )
            return self._query_function_dict[func](*args, **kwargs)

        return wrapper

    return decorator
