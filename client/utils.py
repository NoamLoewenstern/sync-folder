from typing import Callable
from threading import Timer

from typing import Any, Callable
from threading import Timer
from functools import wraps


def debounce(ms: int) -> Callable[..., Callable[..., None]]:
    """Postpone a function's execution until after some time has elapsed.

    :param ms: The amount of milliseconds to wait before the next call can execute.
    :return: The debounced function.
    """
    wait_seconds = ms / 1000

    def decorator(fun: Callable[..., None]) -> Callable[..., None]:
        @wraps(fun)
        def debounced(*args: Any, **kwargs: Any) -> None:
            def call_it() -> None:
                fun(*args, **kwargs)

            try:
                debounced.t.cancel()  # type: ignore
            except AttributeError:
                pass

            debounced.t = Timer(wait_seconds, call_it)  # type: ignore
            debounced.t.start()  # type: ignore

        return debounced

    return decorator  # type: ignore
