import abc
import dataclasses as dc
import inspect
from typing import Sequence, Dict, Any, Callable, Optional

from statey.syms import types, utils, Object, stack


class Function(abc.ABC):
    """
    A function takes some arguments and produces a result
    """

    type: types.FunctionType
    name: str

    def __call__(self, *args: Sequence[Any], **kwargs: Dict[str, Any]) -> Object:
        """
        Create a FunctionCall object by applying this function to the given arguments.
        """
        return utils.wrap_function_call(self, args, kwargs, frame_depth=1)

    @abc.abstractmethod
    def apply(self, arguments: Dict[str, Any]) -> Any:
        """
        Only API method for a function--actually apply the underlying implementation
        """
        raise NotImplementedError


@dc.dataclass(frozen=True)
class NativeFunction(Function):
    """
    Regular python function
    """

    type: types.FunctionType
    func: Callable[[Any], Any] = dc.field(repr=False)
    name: str = "<function>"

    @property
    def signature(self) -> Optional[inspect.Signature]:
        sig = self.__dict__.get("_signature", utils.MISSING)
        if utils.is_missing(sig):
            try:
                sig = inspect.signature(self.func)
            except TypeError:
                sig = None
            self.__dict__["_signature"] = sig
        return sig

    def apply(self, arguments: Dict[str, Any]) -> Any:
        if not self.signature:
            return self.func(**arguments)

        args = []
        kwargs = {}

        for name, param in self.signature.parameters.items():
            if name not in arguments:
                continue

            value = arguments[name]
            if param.kind == inspect._ParameterKind.POSITIONAL_ONLY:
                args.append(value)
            elif param.kind == inspect._ParameterKind.VAR_POSITIONAL:
                args.extend(value)
            elif param.kind == inspect._ParameterKind.VAR_KEYWORD:
                kwargs.update(value)
            else:
                kwargs[name] = value

        return self.func(*args, **kwargs)
