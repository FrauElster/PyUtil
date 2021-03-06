import dataclasses
import functools
import inspect
import logging
import re
import socket
import subprocess
from copy import copy
from inspect import isclass
from typing import Dict, Union, Optional, List, Any, Callable

LOGGER: logging.Logger = logging.getLogger(__name__)
# time formats
TIME_FORMAT: str = "%H:%M:%S"
DATETIME_FORMAT: str = f'%d.%m:%Y {TIME_FORMAT}'


def can_fail(func: Callable[[List[Any], List[Any]], Any]):
    """
    Decorator to try - catch a given function and log it
    :param func:
    :return:

    example:

    ```
    @can_fail
    def parse_to_int(x: str) -> Optional[int]
        return int(x)
    ```
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0] if args and hasattr(args[0], func.__name__) else None
        try:
            return func(*args, **kwargs)
        except Exception as e:
            LOGGER.warning(f'{self.__class__.__name__ + "." if self else ""}{func.__name__} '
                           f'failed with {e.__class__.__name__}: {e}')
            return None

    return wrapper


def list_to_dict(items: List[Any], key: str) -> Dict[Any, Any]:
    """
    transforms a list to a dict.
    This requires the list items to have a __getitem__ implemented. If not it will raise an exception

    key specifies which attribute of the list elements should be used as key.
    it has to be a attribute of the list items obviously

    example:

    ```
    @dataclass
    class Foo:
        id: int
        bar: Any

        def __getitem__(self, item):
            if item not in Foo.__dict__['__annotations__'].keys():
                raise IndexError(f'{item} is not a attribute of {Foo}')
            return self.__getattribute__(item)

    foo_list = [Foo(id=0, bar="hello"), Foo(id=1, bar="world")]
    foo_dict = list_to_dict(foo_list, "id")  # {0: Foo(id=0, bar="hello"), 1: Foo(id=1, bar="world")}
    ```
    """
    if not items:
        return {}
    return dict(map(lambda list_item: (list_item[key], list_item), items))


def str_to_time_str(string: str) -> Optional[str]:
    """
    converts a string from 00:10 to 00:10:00
    :param string:
    :return:
    """
    while len(string.split(":")) < 3:
        string += ":00"
    match = re.match(re.compile(r"^(0[0-9]|1[0-9]|2[0-3]|[0-9]):[0-5][0-9](:[0-5][0-9]|)"), string)
    return string if match else None


def pub_props_of_class(cls: Union[Callable[[], Any], object]) -> List[str]:
    """
    for dataclasses this only works on attributes that have a default value
    returns a list of all public properties of a class
    """
    cls = cls if isclass(cls) else cls.__class__
    return_list: List[str] = [i[0] for i in inspect.getmembers(cls) if i[0][:1] != '_']
    if dataclasses.is_dataclass(cls):
        return_list.extend([i for i in cls.__annotations__ if i[:1] != '_'])
    return return_list


def props_of_class(cls: Union[Callable[[], Any], object]) -> List[str]:
    """
    returns a list of all attributes of a class
    """
    cls = cls if isclass(cls) else cls.__class__
    return_list: List[str] = [i[0] for i in inspect.getmembers(cls) if i[0][:2] != '__']
    if dataclasses.is_dataclass(cls):
        return_list.extend([i for i in cls.__annotations__ if i[:1] != '__'])
    return list(set(return_list))


def get_pub_attr_of_class(cls: Union[Callable[[], Any], object]) -> List[str]:
    """
    Returns a list of public attributes of a given class or object.
    For dataclasses this only works for attributes that have a default value
    :param cls: the class or object you want the public attr from
    :return: a List[str] with the names of the attributes as values
    """
    return list(filter(lambda prop: not callable(cls.__dict__.get(prop)), pub_props_of_class(cls)))


def get_attr_of_class(cls: Union[Callable[[], Any], object]) -> List[str]:
    """
       Returns a list of attributes of a given class or object.
       For dataclasses this only works for attributes that have a default value
       :param cls: the class or object you want the attr from
       :return: a List[str] with the names of the attributes as values
    """
    return list(filter(lambda prop: not inspect.isfunction(prop), props_of_class(cls)))


def check_if_any_prop_none(obj: object) -> List[str]:
    """
    Returns a list of properties that are None or falsy of a given object
    :param obj: the object to inspect
    :return: a list of the names of the attributes that are falsy
    """
    return list(filter(lambda prop: obj.__dict__[prop] is None or (
            not isinstance(obj.__dict__[prop], bool) and not obj.__dict__[prop]), props_of_class(obj.__class__)))


def generate_obj_from_dict(dictionary: Dict, class_: Callable[[Any], Any]) -> Any:
    """
    tries to generate an Object from a dictionary.
    !ATTENTION!
        - Unrecognized keys will be ignored
        - Attributes not found as keys will be None

    :param dictionary: the values, where (k,v) is (attributeName, attributeValue) of class_
    :param class_: the class of the object to generate
    :return:
    """
    dictionary = copy(dictionary)
    attrs: List[str] = pub_props_of_class(class_)
    for key in attrs:
        if key not in dictionary.keys():
            LOGGER.info(f'{key} not found while generating {class_.__name__}.')

    unrecognized_keys: List[str] = list(filter(lambda key: key not in attrs, dictionary.keys()))
    for key in unrecognized_keys:
        LOGGER.warning(f'Unrecognized key {key} gets ignored on {class_.__name__} generation..')
        dictionary.pop(key)

    return class_(**dictionary)


def all_subclasses(cls) -> set:
    """
    :param cls: the class of which sub class shall be found
    :return: a list of all sub*classes recursively
    """
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)])


def exec_cmd(command: str) -> subprocess.CompletedProcess:
    """
    executes a given command and waits for completion.
    :param command: the command to be executed
    """
    return subprocess.run(command, shell=True, stdout=subprocess.PIPE)


def is_int(s: Union[str, int]) -> Optional[int]:
    if isinstance(s, int):
        return s
    try:
        return int(s)
    except Exception:
        return None


def is_internet_connected() -> bool:
    try:
        s = socket.create_connection(("8.8.8.8", 80), 3)
        s.close()
        return True
    except:
        pass
    return False


def clean_dict(dictionary: Dict[str, Any], to_clean: str, replacement: str) -> Dict[str, Any]:
    """
    Cleans all keys of dictionaries recursively.
    :param dictionary: the dictionary to clean
    :param to_clean: the string to replace
    :param replacement: the string to replace it with
    :return: a dictionary where all keys that had the [to_clean] in them got replaced with the [replacement]
    """
    def _clean_list(list_: List[Any], to_clean: str, replacement: str) -> List[Any]:
        new_list = []
        for value in list_:
            if isinstance(value, dict):
                new_value = clean_dict(value, to_clean, replacement)
            elif isinstance(value, list):
                new_value = _clean_list(value, to_clean, replacement)
            else:
                new_value = value
            new_list.append(new_value)
        return new_list

    new_dict: Dict[str, Any] = {}
    for key, value in dictionary.items():
        new_key = key.replace(to_clean, replacement) if to_clean in key else key
        if isinstance(value, dict):
            new_value = clean_dict(value, to_clean, replacement)
        elif isinstance(value, list):
            new_value = _clean_list(value, to_clean, replacement)
        else:
            new_value = value
        new_dict[new_key] = new_value

    return new_dict


def levenshtein(s1: str, s2: str) -> int:
    """
    Calculates the levenshtein distance of the given strings
    :param s1:
    :param s2:
    :return:
    """
    if len(s1) < len(s2):
        return levenshtein(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]
