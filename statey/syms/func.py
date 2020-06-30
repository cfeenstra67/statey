import abc
import dataclasses as dc
from typing import Sequence, Dict, Any, Callable, Optional

from statey.syms import types, utils, Object


class Function(abc.ABC):
    """
	A function takes some arguments and produces a result
	"""

    type: types.FunctionType
    name: str

    def __call__(self, *args: Sequence[Any], **kwargs: Dict[str, Any]) -> Any:
        """
		Create a FunctionCall object by applying this function to the given arguments.
		"""
        return Object(utils.wrap_function_call(self, args, kwargs))

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
    func: Callable[[Any], Any]
    name: str = "<function>"

    def apply(self, arguments: Dict[str, Any]) -> Any:
        args = []
        for arg in self.type.args:
            args.append(arguments[arg.name])
        return self.func(*args)
