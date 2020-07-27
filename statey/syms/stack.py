from typing import Type as PyType, Any, Callable, Sequence


INTERNAL_CODE = set()


def internalcode(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """
	Mark the given function as internal (don't show in tracebacks when using)
	"""
    INTERNAL_CODE.add(func.__code__)
    return func


def default_stack_func(stack: Sequence[Any]) -> Sequence[Any]:
    """
	Default stack behavior, just hiding internal calls
	"""
    out = []
    for tb in stack:
        if tb.tb_frame.f_code in INTERNAL_CODE:
            continue
        out.append(tb)
    return out


@internalcode
def rewrite_tb(exc_type: PyType[Exception], exc_value: Any, tb: Any) -> None:

    # Always skip first frame
    # tb = tb.tb_next

    stack = []

    while tb is not None:
        stack.append(tb)
        tb = tb.tb_next

    stack = default_stack_func(stack)

    tb_next = None

    for tb in reversed(stack):
        tb_next, tb.tb_next = tb, tb_next

    raise exc_value.with_traceback(tb)
