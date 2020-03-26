from __future__ import annotations
import sys
import time
import threading

from typing import Generator, Iterator, Optional, Text

_real_print = print


def print(*values: object, sep: Optional[Text] = ' ', end: Optional[Text] = '\n', file=None,
          flush: bool = None) -> None:
    """
    Monkey patches the print, so that the old spinner cursor will be removed.
    For param explanation look at builtins.print.

    ```python
    from utils.spinner import print
    with Spinner():
        while True:
            print("Foo")
            sleep(1)
    ```
    :param values:
    :param sep:
    :param end:
    :param file:
    :param flush:
    :return:
    """
    sys.stdout.write('\b\b')
    _real_print(*values, sep=sep, end=end, file=file, flush=flush)


class Spinner:
    """
    creates a spinning wheel on command line

    usage:

    ```python
    with Spinner():
        do_stuff()
    ```
    """
    busy = False
    delay = 0.1

    @staticmethod
    def spinning_cursor() -> Generator[Iterator[str]]:
        while True:
            for cursor in '|/-\\':
                yield cursor

    def __init__(self, delay=None) -> None:
        self.spinner_generator = self.spinning_cursor()
        if delay and float(delay):
            self.delay = delay

    def spinner_task(self) -> None:
        while self.busy:
            sys.stdout.write(f"{next(self.spinner_generator)}\t")
            sys.stdout.flush()
            time.sleep(self.delay)
            sys.stdout.write('\b\b')
            sys.stdout.flush()

    def __enter__(self) -> None:
        self.busy = True
        threading.Thread(target=self.spinner_task).start()

    def __exit__(self, exception, value, tb) -> bool:
        self.busy = False
        time.sleep(self.delay)
        if exception is not None:
            return False
