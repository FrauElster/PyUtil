############### requirements ###############
#
# numpy
# Faker
#
############################################


import collections
import functools
import logging
import string
import numpy as np
from datetime import datetime
from ipaddress import IPv4Address, IPv6Address
from random import randint, uniform, choices, random, getrandbits
from typing import List, Dict, Callable, Any, Optional

from faker import Faker

LOGGER: logging.Logger = logging.getLogger(__name__)
ITERLIMIT: int = 1000

facts = [
    "Some cats are allergic to humans",
    "Competitive art was an olypiadic discipline",
    "A chef's hat has exactly 100 folds",
    "Oranges are not naturally occurring fruits",
    "High Heels were originally worn by men",
    "Queen Elizabth II is a trained mechanic",
    "2014 was the first Tinder Match on the Antarctic",
    "Hot water freezes faster than cold water",
    "Dolphins have names for each other",
    "Otters holding hands while sleeping",
    "The national animal of Scotland is a unicorn",
    "Bees sometimes sting other bees",
    "Koalas have fingerprints",
    "The author of Dracula was never in Transylvania",
    "Humans sneeze faster than cheetahs",
    "The patent for hydrants was lost in a fire",
    "Cows kill more people than sharks",
    "Sharks have been around longer than trees",
    "The Twitter bird's name is Larry",
    "Banging your head against the wall burns about 150 calories",
    "When hippos are angry, their sweat is red",
    "A flock of crows is called \"murderer\"",
    "The average woman uses about 34cm of lipstick per year",
    "If you lift a kangaroo's tail, it cannot hop",
    "Catfish are the only animals with an uneven number of whiskers",
    "The French language has 7 different words for \"surrender\"",
    "The Eifel Tower has 1665 steps",
    "Los Angeles is actually called \n\"El Pueblo de Nuestra Senora la Reina de los Angeles\""
]


def _unique(func: Callable[[Optional[List[Any]], Optional[Dict[Any, Any]]], Any]):
    """
    if "unique" in kwargs and it is true the func will be called until a value which has never occurred occurs or the
    ITERLIMIT is reached
    :param func:
    :return:
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if "unique" not in kwargs or not kwargs["unique"]:
            return func(*args, **kwargs)

        func_name: str = func.__name__
        self: Randomizer = args[0]
        _uniques: Dict[str, List] = getattr(self, "_uniques")

        x = func(*args, **kwargs)
        iteration: int = 0
        while x not in _uniques[func_name]:
            if iteration > ITERLIMIT:
                LOGGER.warning(
                    f"Tried {ITERLIMIT} times to get a new unique value for {func_name}. Maybe in the given limitations there is no more unique value")
                return x
            x = func(*args, **kwargs)
        _uniques[func_name].append(x)
        return x

    return wrapper


class Randomizer:
    _uniques: Dict[str, List[int]]
    _faker: Faker

    def __init__(self):
        self._uniques = collections.defaultdict(list)
        self._faker = Faker()

    @_unique
    def random_name(self) -> str:
        return self._faker.name()

    @_unique
    def random_int(self, min: int, max: int, unique: bool = False) -> int:
        assert min < max, "min cant be smaller than max"
        return randint(min, max)

    @_unique
    def random_float(self, min: float, max: float, unique: bool = False) -> float:
        assert min < max, "min cant be smaller than max"
        return uniform(min, max)

    @_unique
    def random_str(self, length: int, chars: str = None, upper_case: bool = True, lower_case: bool = True,
                   digits: bool = True, special_cars: bool = True, unique: bool = False) -> str:
        assert min < max, "min cant be smaller than max"
        assert chars or upper_case or lower_case or digits or special_cars, "A random string option ['upper_case', 'lower_case', 'digits', 'special_chars'] or a given string must be given"

        if not chars:
            chars: List[str] = []
            if lower_case:
                chars.extend(string.ascii_lowercase)
            if upper_case:
                chars.extend(string.ascii_uppercase)
            if digits:
                chars.extend(string.digits)
            if special_cars:
                chars.extend(string.punctuation)

        return ''.join(choices(chars, k=length))

    @_unique
    def random_time(self, min: datetime, max: datetime, unique: bool = False):
        return min + random() * (max - min)

    @staticmethod
    def random_bool(true_probability: float = 0.5) -> bool:
        assert 0 <= true_probability <= 1, f"'true_probability' has to be a value between 0 and 1"
        return np.random.choice([True, False], p=[true_probability, 1 - true_probability])

    @_unique
    def random_ip(self, ip_v6: bool = False, unique: bool = False) -> str:
        if ip_v6:
            bits = getrandbits(128)
            addr = IPv6Address(bits)
            return addr.exploded
        else:
            bits = getrandbits(32)
            addr = IPv4Address(bits)
            return str(addr)

    @_unique
    def random_fact(self, unique: bool = False) -> str:
        return facts[randint(0, len(facts) - 1)]
