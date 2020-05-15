import abc
import dataclasses as dc
from functools import partial
from typing import Dict, Any, Optional, Tuple, Union

import marshmallow as ma

import statey as st
from statey.registry import Registry
from statey.syms import types, symbols, exc, utils, path


class Namespace(abc.ABC):
	"""
	A namespace contains information about names and their associated types
	"""
	def __init__(self, path_parser: path.PathParser = path.PathParser()) -> None:
		self.path_parser = path_parser

	@abc.abstractmethod
	def new(self, key: str, type: types.Type) -> symbols.Symbol:
		"""
		Create a new symbol for the given key and schema and add it to the current namespace
		"""
		raise NotImplementedError

	def ref(self, key: str) -> symbols.Symbol:
		"""
		Get a reference to a key whose type is already registered in this namespace. Will raise
		SymbolKeyError is none exists
		"""
		typ = self.resolve(key)
		return symbols.Reference(key, typ, self)

	@abc.abstractmethod
	def resolve(self, key: str) -> types.Type:
		"""
		Get the type of the given key, raising an error if it is not in the current schema
		"""
		raise NotImplementedError


@dc.dataclass(frozen=True)
class NamedSessionSetter:
	"""
	This is returned from Session.__getitem__ and can allow a few different
	expression syntax options like
	a = session['a'] << A() # This returns a reference correctly instead of an A object
	"""
	key: str
	annotation: Any
	session: 'Session'

	def __lshift__(self, other: Any):
		return self.session.set(self.key, other, self.annotation)


class Session(abc.ABC):
	"""
	A session contains a namespace and associated data and symbols
	"""
	def __init__(self, ns: Namespace, registry: Optional[Registry] = None) -> None:
		if registry is None:
			registry = st.registry
		self.ns = ns
		self.registry = registry

	def set(self, key: str, value: Any, annotation: Any = utils.MISSING) -> symbols.Symbol:
		"""
		Set the given data, using the given registry to determine a schema for value
		"""
		if isinstance(value, symbols.Symbol):
			typ = value.type
		elif annotation is utils.MISSING:
			typ = self.registry.infer_type(value)
		else:
			typ = self.registry.get_type(annotation)
		ref = self.ns.new(key, typ)
		self.set_data(key, value)
		return ref

	def __setitem__(self, key: Union[slice, str], value: Any) -> None:
		"""
		Allow dictionary syntax for adding items to the session
		"""
		annotation = utils.MISSING
		if isinstance(key, slice):
			key, annotation = key.start, key.stop
		self.set(key, value, annotation)

	def __getitem__(self, key: Union[slice, str]) -> NamedSessionSetter:
		"""
		Return a special object to provide better syntax for certain operations
		"""
		annotation = utils.MISSING
		if isinstance(key, slice):
			key, annotation = key.start, key.stop
		return NamedSessionSetter(key, annotation, self)

	# Abstract methods
	@abc.abstractmethod
	def resolve(self, symbol: symbols.Symbol) -> Any:
		"""
		Resolve the given symbol with the given input data.
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def set_data(self, key: str, data: Any) -> None:
		"""
		Set the given data at the given key. Data can be or contain symbols provided they
		are correctly typed
		"""
		raise NotImplementedError
