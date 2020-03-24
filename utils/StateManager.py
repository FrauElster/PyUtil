import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, Any, Callable, List, Optional, Union

from .Singleton import Singleton
from .util import get_pub_attr_of_class

LOGGER: logging.Logger = logging.getLogger(__name__)


@dataclass
class Subscriber:
    id: str
    callback: Callable[[Any], None]

    def notify(self, state: Any):
        self.callback(state)


class _State:
    name: str
    _value: Any
    _subscribers: Dict[str, Subscriber]
    _type: type

    def __init__(self, name: str, value: Any = None, type=None):
        self.name = name
        self._value = value
        self._subscribers = {}
        self._type = type

    def subscribe(self, subscriber: Subscriber) -> bool:
        if subscriber.id in self._subscribers:
            return False
        self._subscribers[subscriber.id] = subscriber
        return True

    def unsubscribe(self, subscriber_id: str) -> bool:
        if subscriber_id in self._subscribers:
            self._subscribers.pop(subscriber_id)
            return True
        else:
            LOGGER.debug(f"Could not unsubscribe {subscriber_id} from {self.name}: Not subscribed")
            return False

    def notify(self):
        for subscriber in self._subscribers.values():
            subscriber.notify(self.get_value())

    def get_value(self) -> Any:
        return deepcopy(self._value)

    def set_value(self, value: Any) -> bool:
        if self._type is not None:
            if not isinstance(value, self._type):
                return False
        self._value = deepcopy(value)
        self.notify()
        return True

    def get_subscriber_ids(self) -> List[str]:
        return list(self._subscribers.keys())


class StateManager:
    """
        A [StateManager] is an instance to hold any states and notify [Subscribers] on change.

        usage:

        ```
        class PortListener:
            ip: IPAddress
            _identifier: str = "my PortListener"

            def __init__(self):
                StateManager().subscribe("ip", self._identifier, self.on_ip_change)  # Subscribe to channel

            def __del__(self):
                StateManager().unsubscribe_all(self._identifier)

            def on_ip_change(self, ip: IPAddress):  # callback for changes
                self.ip = ip
                print(f"New IP: {self.ip}")


        def main():
            port_listener = PortListener()

            state_manager = StateManager()
            state_manager.set_state("ip", getIpAddress())
        ```

        """
    _states: Dict[str, _State] = {}

    def set_state(self, state_name: str, state: Any, type: type = None) -> bool:
        """
        Updates a state or creates it if the [state_name] is new
        :param state_name: the state identifier
        :param state: the new state
        :param type: if specified and [state_name] is new, the state will be type secure.
                     Meaning it will only set to the specified [type]
        :return: bool of success. It could fail due [state] is not of the type-secure type
        """
        if state_name in self._states:
            return self._states[state_name].set_value(state)
        else:
            self._states[state_name] = _State(name=state_name, value=state, type=type)
            return True

    def get_state(self, state_name: str) -> Optional[Any]:
        """
        Returns the current state of a given [state_name]
        :param state_name: the state identifier
        :return: the states value or None if [state_name] unknown
        """
        if state_name in self._states:
            return self._states[state_name].get_value()
        else:
            LOGGER.debug(f"Could get_state {state_name}: No such state name")

    def subscribe(self, state_name: str, subscriber: Subscriber) -> bool:
        """
        Subscribes to a state
        :param subscriber: the subscriber to be notified on changes
        :param state_name: The state_identifier
        :return: bool of success. It could fail due subscribing with an already subscribed [subscriber_id]
                 or to an unknown [state_name]
        """
        if state_name in self._states:
            return self._states[state_name].subscribe(subscriber)
        else:
            LOGGER.debug(f"Could not subscribe {subscriber.id} to {state_name}: No such state name")
            return False

    def unsubscribe(self, state_name: str, subscriber_id: str) -> bool:
        """
        Unsubscribe a [subscriber_id] from a state with [state_name] identifier
        :param state_name: the identifier of the state you want to unsub from
        :param subscriber_id: the identifier of the subscriber
        :return: bool of success. It could fail due to an unknown [state_name] 
                or because [subscriber_id] is not a subscriber of [state_name]
        """
        if state_name in self._states:
            return self._states[state_name].unsubscribe(subscriber_id)
        else:
            LOGGER.debug(f"Could not unsubscribe {subscriber_id} from {state_name}: No such state name")
            return False

    def unsubscribe_all(self, subscriber_id: str) -> bool:
        """
        Unsubscribe a given [subscriber_id] from all subscribed states.
        This is especially helpful on destructors.
        :param subscriber_id: the identifier of the subscriber
        :return: bool of success. It could fail due to no subscriptions of [caller_id]
        """
        success: bool = False
        for state in self._states.values():
            if subscriber_id in state.get_subscriber_ids():
                state.unsubscribe(subscriber_id)
                success = True
        return success

    def get_state_names(self) -> List[str]:
        """
        :return: A list of all state identifiers
        """
        return list(self._states.keys())


class StateManagerSingleton(StateManager, Singleton):
    """
    A [StateManager] that is conveniently a Singleton and therefore can be just initialized where it is needed,
    to get a reference on it.
    """
    pass


class State:
    """
    A class to inherit from.
    All public setters will notify
    """
    _subscriber: Dict[str, List[Subscriber]] = {}

    def __setattr__(self, key: str, value):
        object.__setattr__(self, key, deepcopy(value))
        if key[0] != "_" and key in self._subscriber:
            for subscriber in self._subscriber[key]:
                subscriber.notify(deepcopy(value))

    def subscribe(self, subscriber: Subscriber, attributes: Union[str, List[str]] = None) -> bool:
        """
        Subscribe to a given list of [attributes]
        :param subscriber: the subscriber to notify
        :param attributes: the attributes to listen on
        :return: bool of success. It could fail due invalid [attributes] or already subscription of [subscriber].
                 A fail only means 'something went wrong' not all.
        """
        success: bool = True
        if attributes is None:
            attributes = get_pub_attr_of_class(self.__class__)
        if isinstance(attributes, str):
            attributes = [attributes]

        for attribute in attributes:
            if attribute not in get_pub_attr_of_class(self):
                LOGGER.debug(f"{attribute} is not an subscribable attribute of {self.__class__.__name__}: "
                             f"not subscribed")
                success = False
                continue
            if attribute in self._subscriber and subscriber in self._subscriber[attribute]:
                LOGGER.debug(f"{subscriber.id} has already subscribed to {self.__class__.__name__}.{attribute}")
                success = False
                continue
            if attribute not in self._subscriber:
                self._subscriber[attribute] = []
            self._subscriber[attribute].append(subscriber)
        return success

    def unsubscribe(self, subscriber_id: str, attributes: Union[str, List[str]] = None) -> bool:
        """
        Unsubscribes a subsciber from certain attributes.
        :param subscriber_id: the subscriber identifier
        :param attributes: the attributes to unsub from
        :return: bool of success. It could fail due to unknown attribute or no valid subscription of the subscriber.
                 A fail only means 'something went wrong' not all.
        """
        if attributes is None:
            return self.unsubscribe_all(subscriber_id)
        if isinstance(attributes, str):
            attributes = [attributes]

        subscriber = self._get_subscriber(subscriber_id)
        success: bool = True
        for attribute in attributes:
            if attribute not in self._subscriber:
                LOGGER.debug(f"{attribute} is not an subscribable attribute of {self.__class__.__name__}: "
                             f"not unsubscribed")
                success = False
                continue
            if attribute in self._subscriber and subscriber not in self._subscriber[attribute]:
                LOGGER.debug(f"{subscriber.id} has not subscribed to {self.__class__.__name__}.{attribute}")
                success = False
                continue
            self._subscriber[attribute].remove(subscriber)
        return success

    def unsubscribe_all(self, subscriber_id: str) -> bool:
        """
        Unsubscribes the notification from all attributes the subscriber with the provided [subscriber_id] listens on
        :param subscriber_id: the identifier of the subscriber
        :return: bool of success. Could be False due no valid subscriptions.
        """
        success: bool = False
        subscriber = self._get_subscriber(subscriber_id)
        for attribute in self._subscriber.keys():
            if subscriber in self._subscriber[attribute]:
                self._subscriber[attribute].remove(subscriber)
                success = True
        return success

    def _get_subscriber(self, subscriber_id: str) -> Optional[Subscriber]:
        all_subscribers = set()
        for subscriber_list in self._subscriber.values():
            all_subscribers.update(subscriber_list)

        for subscriber in all_subscribers:
            if subscriber.id == subscriber_id:
                return subscriber
