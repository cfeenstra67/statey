import abc
import dataclasses as dc
from typing import Sequence, Dict, Any, Callable

from statey.syms import types


class Function(abc.ABC):
	"""
	A function takes some arguments and produces a result
	"""
	type: types.FunctionType

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

	def apply(self, arguments: Dict[str, Any]) -> Any:
		return self.func(**arguments)
