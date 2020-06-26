import abc
import copy
import dataclasses as dc
import inspect
import operator
import os
from functools import partial
from itertools import count, chain
from typing import Any, Callable, Sequence, Dict, Union, Optional, Hashable, Iterable

import networkx as nx

import statey as st
from statey import exc
from statey.syms import utils, types
from statey.syms.semantics import Semantics, StructSemantics


# TODO: Theoretically this is unbounded and could fill up memory eventually, figure out something
# a bit better
NEXT_ID = partial(next, count(1))


class Symbol(abc.ABC, utils.Cloneable):
    """
	A symbol represents some value within a session
	"""

    semantics: Semantics
    # Must be globally unique among all symbols that exist in a namespace
    symbol_id: int

    @property
    def type(self) -> types.Type:
        """
		Shortcut for self.semenatics.type
		"""
        return self.semantics.type

    @abc.abstractmethod
    def get_attr(self, attr: Any) -> "Symbol":
        """
		Get the given attribute on the value of this symbol.
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def _upstreams(self, session: "Session") -> Iterable['Symbol']:
        """
        Return any symbols that this one directly depends on
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _apply(self, dag: nx.DiGraph, session: "Session") -> Any:
        """
        Apply the resolution logic that this symbol implies, optionally utilizing
        the current dag to fetch results as needed. Note that this method can return
        a symbol
        """
        raise NotImplementedError

    def _binary_operator_method(op_func, typ=utils.MISSING):
        def method(self, other):
            ret_typ = self.type if typ is utils.MISSING else typ
            if not isinstance(other, Symbol):
                other_type = st.registry.infer_type(other)
                other_semantics = st.registry.get_semantics(other_type)
                other = Literal(other, other_semantics)
            ret_semantics = st.registry.get_semantics(ret_typ)
            return Function(ret_semantics, op_func, (self, other))

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
            if attr == "__dict__":
                raise
            try:
                return self[attr]
            except exc.SymbolKeyError as err2:
                raise err2 from err1

    def __getitem__(self, attr: Any) -> "Symbol":
        return self.get_attr(attr)

    def map(
        self, func: Callable[[Any], Any], return_type: types.Type = utils.MISSING
    ) -> "Symbol":
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
                    return_type = st.registry.get_type(sig.return_annotation)

        return_semantics = st.registry.get_semantics(return_type)
        return Function(semantics=return_semantics, func=func, args=(self,))


# We want to explicitly disable hashing for symbols because they can contain non-hashable values
# and be deeply nested
@dc.dataclass(frozen=True)
class Reference(Symbol):
    """
	A reference references some value within a session
	"""

    path: str
    semantics: Semantics = dc.field(repr=False, compare=False, hash=False)
    ns: "Namespace" = dc.field(repr=False, hash=False, compare=False)
    symbol_id: int = dc.field(init=False, default=None, repr=False)
    # So this will comparisons and hashes, and included in the repr
    type: types.Type = dc.field(init=False, default=None)

    def __post_init__(self) -> None:
        self.__dict__["symbol_id"] = f"{type(self).__name__}:{self.path}"
        self.__dict__["type"] = self.semantics.type

    def get_attr(self, attr: Any) -> "Symbol":
        semantics = self.semantics.attr_semantics(attr)
        ns = self.__dict__["ns"]
        path = ns.path_parser.join([self.__dict__["path"], attr])
        if semantics is None:
            raise exc.SymbolKeyError(path, ns)
        return type(self)(path, semantics, ns)

    def _upstreams(self, session: "Session") -> Iterable['Symbol']:
        data = session.get_encoded_data(self.path)
        expanded = self.semantics.expand(data)

        syms = []

        def collect_symbols(x):
            if isinstance(x, Symbol):
                syms.append(x)

        self.semantics.map(collect_symbols, expanded)
        return syms

    def _apply(self, dag: nx.DiGraph, session: "Session") -> Any:
        return session.get_encoded_data(self.path)


class ValueSemantics(Symbol):
    """
	For all values other than references, we need to implement get_attr by wrapping
	the underlying semantics.get_attr method in a Function.
	"""

    def get_attr(self, attr: Any) -> "Symbol":
        semantics = self.semantics.attr_semantics(attr)
        if semantics is None:
            raise exc.SymbolAttributeError(self, attr)
        return Function(
            semantics=semantics,
            func=lambda x: self.semantics.get_attr(x, attr),
            args=(self,),
        )


@dc.dataclass(frozen=True)
class Literal(ValueSemantics):
    """
	A literal is a symbol that represents a concrete value
	"""

    value: Any
    semantics: Semantics = dc.field(repr=False, compare=False, hash=False)
    symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)
    # So this will comparisons and hashes, and included in the repr
    type: types.Type = dc.field(init=False, default=None)

    def __post_init__(self) -> None:
        self.__dict__["type"] = self.semantics.type

    def _upstreams(self, session: "Session") -> Iterable['Symbol']:
        data = self.value
        expanded = self.semantics.expand(data)

        syms = []

        def collect_symbols(x):
            if isinstance(x, Symbol):
                syms.append(x)

        self.semantics.map(collect_symbols, expanded)
        return syms

    def _apply(self, dag: nx.DiGraph, session: "Session") -> Any:
        return self.value


@dc.dataclass(frozen=True)
class Function(ValueSemantics):
    """
	A symbol that is the result of applying `func` to the given args
	"""

    semantics: Semantics = dc.field(repr=False, compare=False, hash=False)
    func: Callable[[Any], Any]
    args: Sequence[Symbol] = dc.field(default_factory=tuple)
    kwargs: Dict[Hashable, Symbol] = dc.field(default_factory=dict)
    symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)
    # So this will comparisons and hashes, and included in the repr
    type: types.Type = dc.field(init=False, default=None)

    def __post_init__(self) -> None:
        self.__dict__["type"] = self.semantics.type

    def _upstreams(self, session: "Session") -> Iterable['Symbol']:
       yield from self.args
       yield from self.kwargs.values()

    def _apply(self, dag: nx.DiGraph, session: "Session") -> Any:
        args = []
        unknowns = []

        for sym in self.args:
            arg = dag.nodes[sym.symbol_id]['result']
            if isinstance(arg, Unknown):
                unknowns.append(arg)
            args.append(arg)

        kwargs = {}
        for key, sym in self.kwargs.items():
            arg = dag.nodes[sym.symbol_id]['result']
            if isinstance(arg, Unknown):
                unknowns.append(arg)
            kwargs[key] = arg

        if unknowns:
            raise exc.UnknownError(unknowns)

        return self.func(*args, **kwargs)


@dc.dataclass(frozen=True)
class FutureResult:
    """
	Helper so that we can clone futures while still retaining their behavior.
	"""

    result: Any = dc.field(default=utils.MISSING)

    def get(self) -> Any:
        """
		Get the result of the future, raising exc.FutureResultNotSet
		if it hasn't been set yet
		"""
        if self.result is utils.MISSING:
            raise exc.FutureResultNotSet(self)
        return self.result

    def set(self, result: Any) -> None:
        """
		Set the result, raising exc.FutureResultAlreadySet if it has
		already been set
		"""
        if self.result is not utils.MISSING:
            raise exc.FutureResultAlreadySet(self)
        self.__dict__["result"] = result


@dc.dataclass(frozen=True)
class Future(ValueSemantics):
    """
	A future is a symbol that may or may not yet be set
	"""

    semantics: Semantics = dc.field(repr=False, compare=False, hash=False)
    refs: Sequence[Symbol] = ()
    symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)
    result: FutureResult = dc.field(default_factory=FutureResult)
    # Expected output, potentially containing unknowns
    expected: Any = dc.field(default=utils.MISSING, compare=False)
    # So this will comparisons and hashes, and included in the repr
    type: types.Type = dc.field(init=False, default=None)

    def __post_init__(self) -> None:
        self.__dict__["type"] = self.semantics.type

    def expecting(self, data: Any) -> "Future":
        """
		Modify this future in place to expect the given output
		"""
        # Returns a shallow copy, so we'll still point to the same result.
        future = Future(self.semantics, self.refs, expected=data)
        future.__dict__["result"] = self.result
        return future

    def get_result(self) -> Any:
        """
		Get the result of the future, raising exc.FutureResultNotSet
		if it hasn't been set yet
		"""
        return self.result.get()

    def set_result(self, result: Any) -> None:
        """
		Set the result, raising exc.FutureResultAlreadySet if it has
		already been set
		"""
        self.result.set(result)

    def _upstreams(self, session: "Session") -> Iterable['Symbol']:
       return self.refs

    def _apply(self, dag: nx.DiGraph, session: "Session") -> Any:
        try:
            return self.get_result()
        except exc.FutureResultNotSet as err:
            raise errors.UnknownError(self.refs, expected=self.expected) from err


@dc.dataclass(frozen=True)
class Unknown(Symbol):
    """
	Some value that is as-yet unknown. It may or may not be known at some
	time in the future. Note this is NOT a symbol
	"""

    symbol: Symbol
    refs: Sequence[Symbol] = ()
    symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)

    @property
    def semantics(self) -> Semantics:
        return self.symbol.semantics

    def clone(self, **kwargs) -> "Unknown":
        kws = {"symbol": self.symbol.clone(), "refs": tuple(self.refs)}
        kws.update(kwargs)
        return super().clone(**kws)

    def map(self, func: Callable[[Any], Any]) -> "Unknown":
        return self.clone(symbol=self.symbol.map(func))

    def get_attr(self, attr: str) -> Any:
        return self.clone(symbol=self.__dict__["symbol"].get_attr(attr))

    def _upstreams(self, session: "Session") -> Iterable['Symbol']:
       return self.refs

    def _apply(self, dag: nx.DiGraph, session: "Session") -> Any:
        raise exc.UnknownError(self.refs)


@dc.dataclass(frozen=True)
class StructSymbolField:
    """
    Single field in a StructSymbol
    """
    name: str
    symbol: Symbol


@dc.dataclass(frozen=True)
class StructSymbol(ValueSemantics):
    """
    Combines multiple symbols into a struct
    """
    fields: Sequence[StructSymbolField]
    symbol_id: int = dc.field(init=False, default_factory=NEXT_ID, repr=False)

    @property
    def semantics(self) -> Semantics:
        struct_fields = []
        for field in self.fields:
            struct_fields.append(types.StructField(field.name, field.symbol.semantics.type))
        struct_type = types.StructType(struct_fields)

        field_semantics = {}
        for field in self.fields:
            field_semantics[field.name] = field.symbol.semantics

        return StructSemantics(struct_type, field_semantics)

    def get_attr(self, attr: str) -> Any:
        field_map = {field.name: field for field in self.fields}
        return field_map[attr].symbol

    def _upstreams(self, session: "Session") -> Iterable['Symbol']:
        for field in self.fields:
            yield from field.symbol._upstreams(session)

    def _apply(self, dag: nx.DiGraph, session: "Session") -> Any:
        out = {}
        for field in self.fields:
            out[field.name] = field.symbol._apply(dag, session)
        return out
