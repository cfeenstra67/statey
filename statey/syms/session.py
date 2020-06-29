import abc
import dataclasses as dc
from functools import partial
from typing import Dict, Any, Optional, Tuple, Union, Iterator

import marshmallow as ma
import networkx as nx

import statey as st
from statey import exc
from statey.syms import types, utils, path, impl, Object


class Namespace(abc.ABC):
    """
	A namespace contains information about names and their associated types
	"""

    def __init__(
        self,
        registry: Optional["Registry"] = None,
        path_parser: path.PathParser = path.PathParser(),
    ) -> None:
        if registry is None:
            registry = st.registry
        self.path_parser = path_parser
        self.registry = registry

    @abc.abstractmethod
    def new(self, key: str, type: types.Type) -> Object:
        """
		Create a new symbol for the given key and schema and add it to the current namespace.
		Will raise an error if the key already exists.
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, key: str) -> None:
        """
		Delete the type at the given key.

		Note this should be used with EXTREME CAUTION because it can leave dependent sessions
		with invalid references.
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def keys(self) -> Iterator[str]:
        """
		Iterate through the currently defined keys in this namespace.
		"""
        raise NotImplementedError

    def ref(self, key: str) -> Object:
        """
		Get a reference to a key whose type is already registered in this namespace. Will raise
		SymbolKeyError is none exists
		"""
        typ = self.resolve(key)
        semantics = self.registry.get_semantics(typ)
        new_impl = impl.Reference(key, self)
        return Object(new_impl, semantics.type, self.registry)

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
    session: "Session"

    def __lshift__(self, other: Any) -> Object:
        return self.session.set(self.key, other, self.annotation)


class SessionHooks:
    """
	Hook for pluggable functionality in sessions
	"""

    @st.hookspec(firstresult=True)
    def before_set(self, key: str, value: Any) -> Tuple[Any, types.Type]:
        """
		Hook to handle a value before the usual logic in the set() method. This should
		return a (value, type) tuple, where type can be utils.MISSING to indicate we want
		to continue to use the default type inference logic.
		"""

    @st.hookspec(firstresult=True)
    def after_set(self, key: str, value: Any, type: types.Type) -> Any:
        """
		Hook after we've already set a value to customize the value returned instead of a
		reference to the 
		"""


class Session(abc.ABC):
    """
	A session contains a namespace and associated data and objects
	"""

    def __init__(self, ns: Namespace) -> None:
        self.ns = ns
        self.pm = st.create_plugin_manager()
        self.pm.add_hookspecs(SessionHooks)

    def set(
        self, key: str, value: Any, annotation: Any = utils.MISSING
    ) -> Object:
        """
		Set the given data, using the given registry to determine a schema for value
		"""
        hook_resp = self.pm.hook.before_set(key=key, value=value)
        typ = utils.MISSING
        if hook_resp is not None:
            value, typ = hook_resp

        if typ is utils.MISSING:
            if isinstance(value, Object):
                typ = value.type
            elif annotation is utils.MISSING:
                typ = self.ns.registry.infer_type(value)
            else:
                typ = self.ns.registry.get_type(annotation)

        ref = self.ns.new(key, typ)
        self.set_data(key, value)

        other_result = self.pm.hook.after_set(key=key, value=value, type=typ)
        return ref if other_result is None else other_result

    def delete(self, key: str) -> None:
        """
		Convenience method to delete the key from the namespace, then delete the data
		"""
        self.ns.delete(key)
        self.delete_data(key)

    def symbolify(self, data: Any, type: types.Type = utils.MISSING) -> Object:
        """
		Convert the input data into a symbol, optionally with the given type.
		If it is already a symbol, it will be returned unchanged.
		"""
        if isinstance(data, Object):
            return data

        if type is utils.MISSING:
            type = self.ns.registry.infer_type(data)
        semantics = self.ns.registry.get_semantics(type)
        return Object(impl.Data(data), type, self.ns.registry)

    def __lshift__(self, other: Any) -> Object:
        """
		lshift on the top level of a session will simply convert the input to a symbol
		using symbolify()
		"""
        return self.symbolify(other)

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
    def resolve(
        self, symbol: Object, allow_unknowns: bool = False, decode: bool = True
    ) -> Any:
        """
		Resolve the given symbol with the given input data.
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def set_data(self, key: str, data: Any) -> None:
        """
		Set the given data at the given key. Data can be or contain objects provided they
		are correctly typed. `key` must be a ROOT key, not an attribute
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def delete_data(self, key: str) -> None:
        """
		Delete the data at the given key
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def dependency_graph(self) -> nx.MultiDiGraph:
        """
		Return a graph whose nodes are the top-level names registered in this session's namespace
		and whose edges are the dependencies between those nodes within this session. Each edge should
		include `path` as a property with the relative path reference that the dependency represents.
		Note that all paths in this result should be returned as tuples so that they are not dependent
		on this session's path parser implementation.
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def clone(self) -> "Session":
        """
		Return a copy of this session that can be modified without affecting this one
		"""
        raise NotImplementedError
