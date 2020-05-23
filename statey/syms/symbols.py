import abc
import dataclasses as dc
import inspect
import operator
import os
from functools import partial
from itertools import count, chain
from typing import Any, Callable, Sequence, Dict, Union, Optional, Hashable

import statey as st
from statey.syms import utils, types, exc


# TODO: Theoretically this is unbounded and could fill up memory eventually, figure out something
# a bit better
NEXT_ID = partial(next, count(1))


class Symbol(abc.ABC, utils.Cloneable):
	"""
	A symbol represents some value within a session
	"""
	type: types.Type
	registry: 'Registry'
	# Must be globally unique among all symbols that exist in a namespace
	symbol_id: int

	@abc.abstractmethod
	def get_attr(self, attr: Any) -> 'Symbol':
		"""
		Get the given attribute on the value of this symbol.
		"""
		raise NotImplementedError

	def _binary_operator_method(op_func, typ=utils.MISSING):
		def method(self, other):
			ret_typ = self.type if typ is utils.MISSING else typ
			if not isinstance(other, Symbol):
				other_type = st.registry.infer_type(other)
				other = Literal(other, other_type, self.registry)
			return Function(ret_typ, self.registry, op_func, (self, other))
		return method

	__eq__ = _binary_operator_method(operator.eq, types.BooleanType(False))
	__ne__ = _binary_operator_method(operator.eq, types.BooleanType(False))
	__gt__ = _binary_operator_method(operator.gt, types.BooleanType(False))
	__lt__ = _binary_operator_method(operator.lt, types.BooleanType(False))
	__ge__ = _binary_operator_method(operator.ge, types.BooleanType(False))
	__le__ = _binary_operator_method(operator.le, types.BooleanType(False))
	__add__ = _binary_operator_method(operator.add)
	__radd__ = _binary_operator_method(operator.add)
	__sub__ = _binary_operator_method(operator.sub)
	__rsub__ = _binary_operator_method(operator.sub)
	__mul__ = _binary_operator_method(operator.mul)
	__rmul__ = _binary_operator_method(operator.mul)
	__div__ = _binary_operator_method(operator.floordiv)
	__rdiv__ = _binary_operator_method(operator.floordiv)
	__truediv__ = _binary_operator_method(operator.truediv)
	__rtruediv__ = _binary_operator_method(operator.truediv)
	__mod__ = _binary_operator_method(operator.mod)
	__rmod__ = _binary_operator_method(operator.mod)

	def _unary_operator_method(op_func, typ=utils.MISSING):
		def method(self):
			ret_typ = self.type if typ is utils.MISSING else typ
			return Function(ret_typ, op_func, (self,))
		return method

	__invert__ = _unary_operator_method(operator.invert)
	__neg__ = _unary_operator_method(operator.neg)

	def __getattr__(self, attr: str) -> Any:
		"""
		If this symbol is a struct, struct attributes can be accessed by __getattr__
		"""
		try:
			return getattr(super(), attr)
		except AttributeError as err1:
			# Safety measure--we always want to be able to access __dict__ safely
			if attr == '__dict__':
				raise
			try:
				return self[attr]
			except exc.SymbolKeyError as err2:
				raise err2 from err1

	def __getitem__(self, attr: Any) -> 'Symbol':
		return self.get_attr(attr)

	def map(self, func: Callable[[Any], Any], return_type: types.Type = utils.MISSING) -> 'Symbol':
		"""
		Apply `func` to this symbol, optionally with an explicit return type. If return type
		is not provided, we will try to infer it from `func` or fall back to the curren type
		"""
		if return_type is utils.MISSING:
			try:
				sig = inspect.signature(func)
			# No signature
			except ValueError:
				return_type = self.type
			else:
				if sig.return_annotation is inspect._empty:
					return_type = self.type
				else:
					return_type = self.registry.get_type(sig.return_annotation)

		return Function(
			type=return_type,
			registry=self.registry,
			func=func,
			args=(self,)
		)


# We want to explicitly disable hashing for symbols because they can contain non-hashable values
# and be deeply nested
@dc.dataclass(frozen=True)
class Reference(Symbol):
	"""
	A reference references some value within a session
	"""
	path: str
	type: types.Type
	ns: 'Namespace' = dc.field(repr=False, hash=False)
	symbol_id: int = dc.field(init=False, default=None, repr=False)

	def __post_init__(self) -> None:
		self.__dict__['symbol_id'] = f'{type(self).__name__}:{self.path}'

	@property
	def registry(self) -> 'Registry':
		return self.ns.registry

	def get_attr(self, attr: Any) -> 'Symbol':
		ns = self.__dict__['ns']
		semantics = ns.registry.get_semantics(self.type)
		typ = semantics.attr_type(attr)
		path = ns.path_parser.join([self.__dict__['path'], attr])
		if typ is None:
			raise exc.SymbolKeyError(path, ns)
		return type(self)(path, typ, ns)


class ValueSemantics(Symbol):
	"""
	For all values other than references, we need to implement get_attr by wrapping
	the underlying semantics.get_attr method in a Function.
	"""
	def get_attr(self, attr: Any) -> 'Symbol':
		registry = self.__dict__['registry']
		semantics = registry.get_semantics(self.type)
		typ = semantics.attr_type(attr)
		if typ is None:
			raise exc.SymbolAttributeError(self, attr)
		return Function(
			type=typ,
			registry=registry,
			func=lambda x: semantics.get_attr(x, attr),
			args=(self,)
		)


@dc.dataclass(frozen=True)
class Literal(ValueSemantics):
	"""
	A literal is a symbol that represents a concrete value
	"""
	value: Any
	type: types.Type
	registry: 'Registry'  = dc.field(repr=False, hash=False)
	symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)


@dc.dataclass(frozen=True)
class Function(ValueSemantics):
	"""
	A symbol that is the result of applying `func` to the given args
	"""
	type: types.Type
	registry: 'Registry'  = dc.field(repr=False, hash=False)
	func: Callable[[Any], Any]
	args: Sequence[Symbol] = dc.field(default_factory=tuple)
	kwargs: Dict[Hashable, Symbol] = dc.field(default_factory=dict)
	symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)


@dc.dataclass(frozen=True)
class Future(ValueSemantics):
	"""
	A future is a symbol that may or may not yet be set
	"""
	type: types.Type
	registry: 'Registry' = dc.field(repr=False, hash=False)
	refs: Sequence[Reference] = ()
	result: Any = dc.field(init=False, default=utils.MISSING)
	symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)

	def get_result(self) -> Any:
		"""
		Get the result of the future, raising exc.FutureResultNotSet
		if it hasn't been set yet
		"""
		if self.result is utils.MISSING:
			raise exc.FutureResultNotSet(self)
		return self.result

	def set_result(self, result: Any) -> None:
		"""
		Set the result, raising exc.FutureResultAlreadySet if it has
		already been set
		"""
		if self.result is not utils.MISSING:
			raise exc.FutureResultAlreadySet(self)
		self.__dict__['result'] = result


@dc.dataclass(frozen=True)
class Unknown(Symbol):
	"""
	Some value that is as-yet unknown. It may or may not be known at some
	time in the future. Note this is NOT a symbol
	"""
	symbol: Symbol
	refs: Sequence[Reference] = ()
	symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)

	@property
	def registry(self) -> 'Registry':
		return self.symbol.registry

	@property
	def type(self) -> types.Type:
		return self.symbol.type

	def clone(self, **kwargs) -> 'Unknown':
		kws = {'symbol': self.symbol.clone()}
		kws.update(kwargs)
		return super().clone(**kws)

	def map(self, func: Callable[[Any], Any]) -> 'Unknown':
		return self.clone(symbol=self.symbol.map(func))

	def get_attr(self, attr: str) -> Any:
		return self.clone(symbol=self.__dict__['symbol'].get_attr(attr))
