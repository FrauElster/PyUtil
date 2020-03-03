import functools
import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Callable, Optional

from .Singleton import Singleton
from .filehandler import check_if_file_exists, to_abs_file_path, load_file

LOGGER: logging.Logger = logging.getLogger(__name__)


@dataclass
class KeyType:
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


class ConfigLoader(Singleton):
    _is_instantiated: bool = False
    key_types: List[KeyType]
    config_file: str
    config: Dict[str, Any] = {}

    def instantiate(self, key_types: List[KeyType], config_file: str):
        self.config_file = config_file
        self.key_types = key_types
        self._is_instantiated = True

        self.check()

    @_instance_check
    def get_value(self, key: str, default: Any = None):
        if key in self.config:
            return self.config[key]
        return default

    @_instance_check
    def check(self):
        if not check_if_file_exists(self.config_file):
            LOGGER.error(f"No config found at {to_abs_file_path(self.config_file)}")
            exit(1)

        raw_config = load_file(self.config_file)
        if not raw_config:
            LOGGER.error(f"Config is empty or not valid! ({to_abs_file_path(self.config_file)})")
            exit(1)
        if not isinstance(raw_config, dict):
            LOGGER.error(f"Config must be dictionary! ({to_abs_file_path(self.config_file)})")
            exit(1)

        failure: bool = False
        for keyType in self.key_types:
            if keyType.name not in raw_config.keys():
                msg: str = f"Key '{keyType.name}' is not provided in {to_abs_file_path(self.config_file)}"
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
                    self.config[keyType.name] = keyType.model_from_dict(raw_key)
                except Exception:
                    msg: str = f"Could not parse {keyType.name}"
                    if keyType.required:
                        LOGGER.error(msg)
                        failure = True
                    else:
                        LOGGER.warning(f"{msg}. Ignoring key.")
                    continue
            else:
                self.config[keyType.name] = raw_key

        if failure:
            exit(1)
