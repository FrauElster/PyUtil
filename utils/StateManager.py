import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, Any, Callable, List

from utils.Singleton import Singleton

LOGGER: logging.Logger = logging.getLogger(__name__)


@dataclass
class _Subscriber:
    id: str
    callback: Callable[[Any], Any]

    def notify(self, state: Any):
        self.callback(state)


class _State:
    name: str
    _value: Any
    _subscribers: Dict[str, _Subscriber]

    def __init__(self, name: str, value: Any = None):
        self.name = name
        self._value = value
        self._subscribers = {}

    def subscribe(self, subscriber: _Subscriber):
        self._subscribers[subscriber.id] = subscriber

    def unsubscribe(self, subscriber_id: str):
        if subscriber_id in self._subscribers:
            self._subscribers.pop(subscriber_id)
        else:
            LOGGER.debug(f"Could not unsubscribe {subscriber_id} from {self.name}: Not subscribed")

    def notify(self):
        for subscriber in self._subscribers.values():
            subscriber.notify(deepcopy(self._value))

    def set_value(self, value: Any):
        self._value = deepcopy(value)
        self.notify()


class StateManager(Singleton):
    _states: Dict[str, _State] = {}

    def set_state(self, state_name: str, state: Any) -> None:
        if state_name in self._states:
            self._states[state_name].set_value(state)
        else:
            self._states[state_name] = _State(name=state_name, value=state)

    def subscribe(self, state_name: str, caller_id: str, callback: Callable[[Any], Any]) -> bool:
        if state_name in self._states:
            self._states[state_name].subscribe(_Subscriber(id=caller_id, callback=callback))
            return True
        else:
            LOGGER.debug(f"Could not subscribe {caller_id} to {state_name}: No such state name")
            return False

    def unsubscribe(self, state_name: str, caller_id: str) -> bool:
        if state_name in self._states:
            self._states[state_name].unsubscribe(caller_id)
            return True
        else:
            LOGGER.debug(f"Could not unsubscribe {caller_id} from {state_name}: No such state name")
            return False

    def get_state_names(self) -> List[str]:
        return list(self._states.keys())

