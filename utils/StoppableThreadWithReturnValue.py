import ctypes
import logging
import threading
from threading import Thread
from typing import Any

LOGGER: logging.Logger = logging.getLogger(__name__)


class StoppableThreadWithReturnValue(Thread):
    """
    This thread returns the target return value on join.
    If "stop()" is called, it terminates the thread.
    """

    class StopSignal(Exception):
        pass

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        if kwargs is None:
            kwargs = {}
        self._return = None

    def run(self) -> None:
        """
        Executes the target function
        :return:
        """
        if self._target is not None:
            try:
                self._return = self._target(*self._args, **self._kwargs)
            except StoppableThreadWithReturnValue.StopSignal:
                LOGGER.debug(f'Stopped {self.name}')
            except Exception as e:
                LOGGER.exception(f'{e.__class__.__name__} occurred in thread {self.name}: {e}')

    def get_id(self) -> int:
        """
        :return: the threads id
        """
        if hasattr(self, "_thread_id"):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                setattr(self, "_thread_id", id)
                return id

    def stop(self) -> None:
        """
        Terminates the thread

        This works a little bit complicated, through injecting a exception on c level into the run method
        :return:
        """
        thread_id: int = self.get_id()
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(thread_id),
                                                         ctypes.py_object(StoppableThreadWithReturnValue.StopSignal))
        if 1 < res:
            ctypes.pythonapi.PythonThreadState_SetAsyncExc(thread_id, 0)

    def join(self, *args, **kwargs) -> Any:
        """
        Waits for the thread to finish and forwards the return value of the target function
        :param args:
        :return:
        """
        Thread.join(self, *args, **kwargs)
        return self._return
