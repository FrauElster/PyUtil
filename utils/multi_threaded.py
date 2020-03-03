import inspect
from collections import Iterable
from typing import Callable, Optional, Dict, Any, Union, List, Tuple

from utils.StoppableThreadWithReturnValue import StoppableThreadWithReturnValue


def multi_threaded(funcs: List[Union[Callable, Tuple[Callable, List[Any], Dict[str, Any], str]]]) \
        -> Dict[Union[Callable, str], Any]:
    """
    Runs all given funcs in parallel
    :param funcs: tuples with function to call, args as list, kwargs as dict and an optional identification string
    :return: Dict with <function, result> or if identification is provided <id, result> as <k,v>
    """
    result_dict: Dict[Union[Callable, str], Any] = {}
    for func_tuple in funcs:
        id: str
        args = []
        kwargs = {}
        func: Optional[Callable] = None
        if isinstance(func_tuple, Iterable):
            for item in func_tuple:
                if inspect.ismethod(item):
                    func = item
                elif isinstance(item, list):
                    args = item
                elif isinstance(item, dict):
                    kwargs = item
                elif isinstance(item, str):
                    id = item
        else:
            func = func_tuple

        thread = StoppableThreadWithReturnValue(target=func, args=args, kwargs=kwargs)
        result_dict[id if id else func] = thread
        thread.start()

    for func, thread in result_dict.items():
        result_dict[func] = thread.join()

    return result_dict
