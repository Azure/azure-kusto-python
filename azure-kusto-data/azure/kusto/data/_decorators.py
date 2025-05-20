from typing import Callable, TypeVar, Any

F = TypeVar('F', bound=Callable[..., Any])

def aio_documented_by(original: F) -> Callable[[F], F]:
    def wrapper(target: F) -> F:
        target.__doc__ = "Aio function: {original_doc}".format(original_doc=original.__doc__)
        return target

    return wrapper


def documented_by(original: F) -> Callable[[F], F]:
    def wrapper(target: F) -> F:
        target.__doc__ = original.__doc__
        return target

    return wrapper
