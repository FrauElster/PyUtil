import functools
import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Callable, Optional, Union

from .Singleton import Singleton
from .filehandler import check_if_file_exists, to_abs_file_path, load_file

LOGGER: logging.Logger = logging.getLogger(__name__)


@dataclass
class KeyType:
    """
    Defines a config value.
        - The [name] is the config key
        - The [type] is the type to be expected
        - The required indicates if the config value is mandatory for the program. e.g. Api Key
        - The [model_from_dict] is an optional callable to parse raw data to a class.
          See the code example for more details

    ```
    @dataclass
    class User:
        username: str
        birthday: datetime

    def _user_from_raw(raw: Dict[str, Any]) -> User:
        return User(username=raw["username"],
                    birthday=datetime.strptime(raw["birthday"], "%Y.%m.%d"))

    key = KeyType(name="user", type=dict, model_from_dict=_user_from_raw)
    ```

    Would parse a config like:
    ```
    {
        "user": {
            "username": "foo"
            "birthday" "1970.1.1"
        }
    }
    ```
    To a User object
    """
    name: str
    type: Any
    required: bool = False
    model_from_dict: Optional[Callable[[Dict[str, Any]], Any]] = None


def _instance_check(func: Callable[[List[Any], Dict], Any]):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self: ConfigLoader = args[0]
        if self is None:
            LOGGER.warning(
                f"Decorator 'instance_check' not on none-static object method applied ('{func.__name__}'). 'self' is None")
        elif not hasattr(self, "_is_instantiated"):
            LOGGER.warning(f"{type(self)} has no '_is_instantiated' attribute")
        else:
            _is_instantiated = getattr(self, "_is_instantiated")
            if not isinstance(_is_instantiated, bool):
                LOGGER.warning(
                    f"{type(self)}'s attribute '_is_instantiated' is not of type 'bool' but of type '{type(_is_instantiated)}'")
            elif not _is_instantiated:
                LOGGER.warning(f"{type(self)} is not instantiated. Called on '{type(self)}.{func.__name__}'")
                return None
        return func(*args, **kwargs)

    return wrapper


class ConfigLoader:
    """
    This class helps to load and validate config entries.
    It checks a given config file for settings and caches them.

    After initialization the `instantiate` function has to be called.
    """
    _is_instantiated: bool = False
    _key_types: List[KeyType]
    _config_file: str
    _config: Dict[str, Any] = {}

    def instantiate(self, key_types: List[KeyType], config_file: str) -> None:
        """
        Setup method
        :param key_types: a list of [KeyType] to check for.
        :param config_file: the file to look for
        :return: None
        """
        self._config_file = config_file
        self._key_types = key_types
        self._is_instantiated = True

        self.check()

    @_instance_check
    def get_value(self, key: str, default: Any = None) -> Any:
        """
        Gets a config value with key [key]
        :param key: the key of the value
        :param default: if set and no key is provided, it returns the default
        :return: the value associated with the [key]. If it is none, [default] will be returned
        """
        if key in self._config:
            return self._config[key]
        return default

    @_instance_check
    def add_keys(self, key_types: Union[KeyType, List[KeyType]]) -> None:
        """
        Adds additional [KeyType]s and refreshes the cached config
        :param key_types: the new key types to check for
        :return: None
        """
        if isinstance(key_types, KeyType):
            key_types = [key_types]
        for key_type in key_types:
            self._config[key_type.name] = key_type
        self.check()

    @_instance_check
    def remove_keys(self, key_types: Union[KeyType, List[KeyType]]) -> bool:
        """
        removes a set of keys from the config
        :param key_types: the keys to remove
        :return: bool of success. Could fail due to some unknown keys.
        It only indicates that some failed, the rest could be successful
        """
        if isinstance(key_types, KeyType):
            key_types = [key_types]
        success: bool = True
        for key_type in key_types:
            if key_type.name in self._config:
                self._config.pop(key_type.name)
            else:
                success = False
        return success

    @_instance_check
    def check(self) -> None:
        """
        Refreshes the cached config values.
        It checks the [self._config_file] for the specified keys, if the keys are of the corresponding type.
        If some required key_types fail, an exception will be raised
        :return: None
        :raises FileNotFoundError, ValueError
        """
        if not check_if_file_exists(self._config_file):
            msg: str = f"No config found at {to_abs_file_path(self._config_file)}"
            LOGGER.error(msg)
            raise FileNotFoundError(msg)

        raw_config = load_file(self._config_file)
        if not raw_config:
            msg: str = f"Config is empty or not valid! ({to_abs_file_path(self._config_file)})"
            LOGGER.error(msg)
            raise ValueError(msg)
        if not isinstance(raw_config, dict):
            msg: str = f"Config must be dictionary! ({to_abs_file_path(self._config_file)})"
            LOGGER.error(msg)
            raise ValueError(msg)

        failure: bool = False
        for keyType in self._key_types:
            if keyType.name not in raw_config.keys():
                msg: str = f"Key '{keyType.name}' is not provided in {to_abs_file_path(self._config_file)}"
                if keyType.required:
                    LOGGER.error(msg)
                    failure = True
                else:
                    LOGGER.debug(msg)
                continue

            raw_key: Any = raw_config[keyType.name]
            if not isinstance(raw_key, keyType.type):
                msg: str = f"Key '{keyType.name}' has to be of type '{keyType.type}', not '{type(raw_key)}'"
                if keyType.required:
                    LOGGER.error(msg)
                    failure = True
                else:
                    LOGGER.warning(f"{msg}. Value gets ignored")
                continue

            if keyType.model_from_dict:
                try:
                    self._config[keyType.name] = keyType.model_from_dict(raw_key)
                except Exception:
                    msg: str = f"Could not parse {keyType.name}"
                    if keyType.required:
                        LOGGER.error(msg)
                        failure = True
                    else:
                        LOGGER.warning(f"{msg}. Ignoring key.")
                    continue
            else:
                self._config[keyType.name] = raw_key

        if failure:
            raise ValueError(f"Required config values missing. Look in the logs for details")


class ConfigLoaderSingleton(ConfigLoader, Singleton):
    """
    A [ConfigLoader] that can be globally initialized to get a reference to.
    This is just a convenience class of the [ConfigLoader], for more detail look at its documentation
    """
    pass
