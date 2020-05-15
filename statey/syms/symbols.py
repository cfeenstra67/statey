import abc
import dataclasses as dc
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


class Symbol(abc.ABC):
	"""
	A symbol represents some value within a session
	"""
	type: types.Type
	# Must be globally unique among all symbols that exist in a namespace
	symbol_id: int

	def _binary_operator_method(op_func, typ=utils.MISSING):
		def method(self, other):
			ret_typ = self.type if typ is utils.MISSING else typ
			if not isinstance(other, Symbol):
				other_type = st.registry.infer_type(other)
				other = Literal(other, other_type)
			return Function(ret_typ, op_func, (self, other))
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


# We want to explicitly disable hashing for symbols because they can contain non-hashable values
# and be deeply nested
@dc.dataclass(frozen=True)
class Reference(Symbol):
	"""
	A reference references some value within a session
	"""
	path: str
	type: types.Type
	ns: 'syms.session.Namespace' = dc.field(repr=False, hash=False)
	symbol_id: int = dc.field(init=False, default=None, repr=False)

	def __post_init__(self) -> None:
		self.__dict__['symbol_id'] = f'{type(self).__name__}:{self.path}'

	def __getattr__(self, attr: str) -> Any:
		"""
		If this symbol is a struct, struct attributes can be accessed by __getattr__
		"""
		try:
			return getattr(super(), attr)
		except AttributeError as err1:
			try:
				return self[attr]
			except exc.SymbolKeyError as err2:
				raise err2 from err1

	# def __getitem__(self, attr: Union[slice, str]) -> Symbol:
	# 	out_path = self.ns.path_parser.join([self.path, attr])

	# 	def get_struct_field(typ):
	# 		fields_by_name = {field.name: field for field in typ.fields}
	# 		if attr not in fields_by_name:
	# 			raise exc.SymbolKeyError(out_path, self.ns)
	# 		field = fields_by_name[attr]
	# 		return field

	# 	if isinstance(self.type, types.StructType):
	# 		typ = get_struct_field(self.type).type
	# 	elif isinstance(self.type, types.ArrayType):
	# 		if isinstance(attr, slice):
	# 			typ = self.type
	# 		elif isinstance(attr, int):
	# 			typ = self.type.element_type
	# 		elif isinstance(self.type.element_type, types.StructType):
	# 			typ = types.ArrayType(get_struct_field(self.type.element_type).type, self.type.nullable)
	# 			out_path = self.ns.path_parser.join([self.path, utils.EXPLODE, attr])
	# 		else:
	# 			raise exc.SymbolKeyError(out_path, self.ns)
	# 	else:
	# 		raise exc.SymbolKeyError(out_path, self.ns)
	# 	return Reference(out_path, typ, self.ns)

	def __getitem__(self, attr: Union[slice, str]) -> Symbol:
		path = self.ns.path_parser.join([self.path, attr])
		typ = self.ns.resolve(path)
		return Reference(path, typ, self.ns)

	@property
	def x(self) -> Symbol:
		"""
		If this reference is an ArrayType, return a reference with utils.EXPLODE appended to the path
		"""
		return self[utils.EXPLODE]


@dc.dataclass(frozen=True)
class Literal(Symbol):
	"""
	A literal is a symbol that represents a concrete value
	"""
	value: Any
	type: types.Type
	symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)


@dc.dataclass(frozen=True)
class Function(Symbol):
	"""
	A symbol that is the result of applying `func` to the given args
	"""
	type: types.Type
	func: Callable[[Any], Any]
	args: Sequence[Symbol] = dc.field(default_factory=tuple)
	kwargs: Dict[Hashable, Symbol] = dc.field(default_factory=dict)
	symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)
