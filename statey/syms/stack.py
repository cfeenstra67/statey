"""
Modified from the Jinja code
"""
import contextlib
import dataclasses as dc
import platform
import sys
import textwrap as tw
from types import CodeType
from typing import Type as PyType, Any, Callable, Sequence, Optional


INTERNAL_CODE = set()


RESOLUTION_START = set()


@dc.dataclass(frozen=True)
class FrameSnapshot:
    """
    Marks the attributes of a frame at a particular point in time
    """

    lineno: int
    name: str
    filename: str


def frame_snapshot(depth: int = 0) -> FrameSnapshot:
    """
    Take a snapshot of the frame at the given depth (depth relative to the caller)
    """
    frame = sys._getframe(depth + 1)
    return FrameSnapshot(
        lineno=frame.f_lineno,
        filename=frame.f_code.co_filename,
        name=frame.f_code.co_name,
    )


def internalcode(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """
    Mark the given function as internal (don't show in tracebacks when using)
    """
    INTERNAL_CODE.add(func.__code__)
    return func


def resolutionstart(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """
    Mark the given function as the first frame before resolution of objects begins. Additional
    frames will be injected
    """
    RESOLUTION_START.add(func.__code__)
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
def rewrite_tb(
    exc_type: PyType[Exception],
    exc_value: Any,
    tb: Any,
    resolution_stack: Optional["ResolutionStack"] = None,
) -> None:
    """

    """
    raise rewrite_traceback_stack(exc_type, exc_value, tb, resolution_stack)


@contextlib.contextmanager
@internalcode
def rewrite_ctx(resolution_stack: Optional["ResolutionStack"] = None):
    """

    """
    try:
        yield
    except Exception:
        rewrite_tb(*sys.exc_info(), resolution_stack)


def rewrite_traceback_stack(
    exc_type, exc_value, tb, resolution_stack=None, source=None
):
    """Rewrite the current exception to replace any tracebacks from
    within compiled template code with tracebacks that look like they
    came from the template source.

    This must be called within an ``except`` block.

    :param source: For ``TemplateSyntaxError``, the original source if
        known.
    :return: The original exception with the rewritten traceback.
    """
    # Skip the frame for the render function.
    tb = tb.tb_next

    stack = []

    # Build the stack of traceback object, replacing any in template
    # code with the source file and line information.
    while tb is not None:
        # Skip frames decorated with @internalcode. These are internal
        # calls that aren't useful in template debugging output.
        if tb.tb_frame.f_code in INTERNAL_CODE:
            tb = tb.tb_next
            continue

        if tb.tb_frame.f_code in RESOLUTION_START and resolution_stack is not None:
            for snapshot, objs in resolution_stack.get_object_stack():
                fake_tb = fake_traceback(exc_value, tb, snapshot, objs)
                stack.append(fake_tb)

        stack.append(tb)

        tb = tb.tb_next

    tb_next = None

    # Assign tb_next in reverse to avoid circular references.
    for tb in reversed(stack):
        tb_next = tb_set_next(tb, tb_next)

    return exc_value.with_traceback(tb_next)


def fake_traceback(exc_value, tb, snapshot, objs):
    """Produce a new traceback object that looks like it came from the
    template source instead of the compiled code. The filename, line
    number, and location name will point to the template, and the local
    variables will be the current template context.

    :param exc_value: The original exception to be re-raised to create
        the new traceback.
    :param tb: The original traceback to get the local variables and
        code info from.
    :param filename: The template filename.
    :param lineno: The line number in the template source.
    """
    # if tb is not None:
    #     # Replace the real locals with the context that would be
    #     # available at that point in the template.
    #     locals = get_template_locals(tb.tb_frame.f_locals)
    #     locals.pop("__statey_exception__", None)
    # else:
    locals = {}
    lineno = snapshot.lineno

    globals = {
        "__name__": snapshot.filename,
        "__file__": snapshot.filename,
        "__statey_exception__": exc_value,
    }
    # Raise an exception at the correct line number.
    code = compile(
        "\n" * (lineno - 1) + "raise __statey_exception__", snapshot.filename, "exec"
    )

    # Build a new code object that points to the template file and
    # replaces the location with a block name.
    header = "  => "
    whitespace = header
    assert len(header) == len(whitespace)
    obj_lines = "\n".join(
        (header if idx == 0 else whitespace) + repr(obj) for idx, obj in enumerate(objs)
    )
    location = snapshot.name + "\n" + tw.indent(obj_lines, "  ")

    try:
        # Collect arguments for the new code object. CodeType only
        # accepts positional arguments, and arguments were inserted in
        # new Python versions.
        code_args = []

        for attr in (
            "argcount",
            "posonlyargcount",  # Python 3.8
            "kwonlyargcount",
            "nlocals",
            "stacksize",
            "flags",
            "code",  # codestring
            "consts",  # constants
            "names",
            "varnames",
            ("filename", snapshot.filename),
            ("name", location),
            "firstlineno",
            "lnotab",
            "freevars",
            "cellvars",
        ):
            if isinstance(attr, tuple):
                # Replace with given value.
                code_args.append(attr[1])
                continue

            try:
                # Copy original value if it exists.
                code_args.append(getattr(code, "co_" + attr))
            except AttributeError:
                # Some arguments were added later.
                continue

        code = CodeType(*code_args)
    except Exception:
        # Some environments such as Google App Engine don't support
        # modifying code objects.
        pass

    # Execute the new code, which is guaranteed to raise, and return
    # the new traceback without this frame.
    try:
        exec(code, globals, locals)
    except BaseException:
        return sys.exc_info()[2].tb_next


if sys.version_info >= (3, 7):
    # tb_next is directly assignable as of Python 3.7
    def tb_set_next(tb, tb_next):
        tb.tb_next = tb_next
        return tb


elif platform.python_implementation() == "PyPy":
    # PyPy might have special support, and won't work with ctypes.
    try:
        import tputil
    except ImportError:
        # Without tproxy support, use the original traceback.
        def tb_set_next(tb, tb_next):
            return tb

    else:
        # With tproxy support, create a proxy around the traceback that
        # returns the new tb_next.
        def tb_set_next(tb, tb_next):
            def controller(op):
                if op.opname == "__getattribute__" and op.args[0] == "tb_next":
                    return tb_next

                return op.delegate()

            return tputil.make_proxy(controller, obj=tb)


else:
    # Use ctypes to assign tb_next at the C level since it's read-only
    # from Python.
    import ctypes

    class _CTraceback(ctypes.Structure):
        _fields_ = [
            # Extra PyObject slots when compiled with Py_TRACE_REFS.
            ("PyObject_HEAD", ctypes.c_byte * object().__sizeof__()),
            # Only care about tb_next as an object, not a traceback.
            ("tb_next", ctypes.py_object),
        ]

    def tb_set_next(tb, tb_next):
        c_tb = _CTraceback.from_address(id(tb))

        # Clear out the old tb_next.
        if tb.tb_next is not None:
            c_tb_next = ctypes.py_object(tb.tb_next)
            c_tb.tb_next = ctypes.py_object()
            ctypes.pythonapi.Py_DecRef(c_tb_next)

        # Assign the new tb_next.
        if tb_next is not None:
            c_tb_next = ctypes.py_object(tb_next)
            ctypes.pythonapi.Py_IncRef(c_tb_next)
            c_tb.tb_next = c_tb_next

        return tb
