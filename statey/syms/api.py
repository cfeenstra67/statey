"""
This module contains data structures that are only used in order to write
expressive code, and basically boil down to factories for the data structures
contained in the the other modules in this package
"""
import dataclasses as dc
import inspect
from typing import Any, Sequence, Dict, Callable, Type, Tuple

import statey as st
from statey.syms import utils, symbols, types
from statey.syms.plugins import ParseDataClassPlugin, EncodeDataClassPlugin


def symbol(value: Any, type: types.Type = utils.MISSING) -> symbols.Symbol:
    """
    Generic function to create literals from values
    """
    if isinstance(value, symbols.Symbol):
        return value
    if type is utils.MISSING:
        type = st.registry.infer_type(value)
    return symbols.Literal(arg, st.registry.get_semantics(type))


@dc.dataclass(frozen=True)
class _FunctionFactory(utils.Cloneable):
    """
	An interface for creating symbols.Function objects
	"""

    annotation: Any = utils.MISSING

    def __call__(self, func: Callable[[Any], Any]) -> "_FunctionFactoryWithFunction":
        return _FunctionFactoryWithFunction(func, self)

    def __getitem__(self, annotation: Any) -> "_FunctionFactory":
        # Use double square brackets to indicate the annotation is the function/callable
        if isinstance(annotation, list) and len(annotation) == 1:
            return self.clone(annotation=annotation[0])(annotation[0])
        return self.clone(annotation=annotation)


@dc.dataclass(frozen=True)
class _FunctionFactoryWithFunction(utils.Cloneable):
    func: Any
    factory: _FunctionFactory

    def __call__(self, *args, **kwargs):
        # If we can ge the function signature, we can use that to better infer the arg types
        try:
            inspect.signature(self.func)
        except ValueError:
            wrapped_args = list(map(symbol, args))
            wrapped_kwargs = {key: symbol(val) for key, val in kwargs.items()}
            wrapped_return = st.registry.get_type(self.factory.annotation)
        else:
            wrapped_args, wrapped_kwargs, wrapped_return = utils.wrap_function_call(
                st.registry, self.func, *args, **kwargs
            )
            if self.factory.annotation is not utils.MISSING:
                wrapped_return = st.registry.get_type(self.factory.annotation)
        semantics = st.registry.get_semantics(wrapped_return)
        return symbols.Function(
            semantics=semantics,
            func=self.func,
            args=wrapped_args,
            kwargs=wrapped_kwargs,
        )


F = _FunctionFactory()


def autoencode(cls: Type[Any]) -> Type[Any]:
    """
	This method attempts to wrap the given class with a proper base class so that
	it will be deserialized properly when bieng added to the session
	"""
    if isinstance(cls, type) and dc.is_dataclass(cls):
        st.registry.pm.register(ParseDataClassPlugin(cls))
        st.registry.pm.register(EncodeDataClassPlugin(cls))
        return cls
    raise NotImplementedError(f"No known encoding plugin implemented for {cls}.")


def filtered_type(data: Any, type: types.Type) -> types.Type:
    """
    Get a sub-structure of `type` given a set of input data
    """
    if isinstance(type, types.ArrayType):
        return [sub_structure(item, type.element_type) for item in data]
    if isinstance(type, types.StructType):
        fields = []
        for field in type.fields:
            if field.name in data:
                fields.append(field)
        return types.StructType(fields, type.nullable)
    return type


def join(head: symbols.Symbol, *tail: Sequence[Any]) -> symbols.Symbol:
    """
    Pass all of the given argument to a symbolic function that will just
    return the first element, but the result will depend on all of the
    additional arguments symbolically as well
    """
    return symbols.Function(
        func=lambda head, *tail: head, args=(head, *tail), semantics=head.semantics
    )


class _StructSymbolFactory:
    """
    Utility for simply create struct symbols
    """
    def new(self, fields: Sequence[Tuple[str, Any]]) -> symbols.StructSymbol:
        out_fields = []
        for name, value in fields:
            out_fields.append(symbols.StructSymbolField(name, symbol(value)))
        return symbols.StructSymbol(out_fields)

    def dict(self, data: Dict[str, Any]) -> symbols.StructSymbol:
        return self.new(data.items())

    def __call__(self, *args: Sequence[Tuple[str, Any]], **kwargs: Dict[str, Any]) -> symbols.StructSymbol:
        """
        'call' factory method. Pass a dictionary with symbols or 
        """
        items = []
        items.extend(args)
        items.extend(kwargs.items())
        return self.new(args)

    def __getitem__(self, fields: Sequence[slice]) -> symbols.StructSymbol:
        """
        'getitem' interfce for creating structs using __getitem__ syntax
        """
        out_fields = []
        for slc in fields:
            out_fields.append((slc.start, slc.stop))
        return self.new(out_fields)


struct = _StructSymbolFactory()
