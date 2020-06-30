"""
This module contains data structures that are only used in order to write
expressive code, and basically boil down to factories for the data structures
contained in the the other modules in this package
"""
import dataclasses as dc
import inspect
from typing import Any, Sequence, Dict, Callable, Type, Tuple

import statey as st
from statey.syms import utils, types, func, impl, Object
from statey.syms.plugins import ParseDataClassPlugin, EncodeDataClassPlugin


def function(func: Callable[[Any], Any], type: types.Type = utils.MISSING, registry: "Registry" = utils.MISSING) -> func.Function:
    """
    Construct a Function object for a regular python function
    """
    return utils.native_function(func, type, registry)


@dc.dataclass(frozen=True)
class _FunctionFactory(utils.Cloneable):
    """
	An interface for creating function call objects
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
        return utils.wrap_function_call(
            func=self.func,
            args=args,
            kwargs=kwargs,
            return_annotation=self.factory.annotation
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


def join(head: Object, *tail: Sequence[Any]) -> Object:
    """
    Pass all of the given argument to a symbolic function that will just
    return the first element, but the result will depend on all of the
    additional arguments symbolically as well
    """
    def join_func(left: head._type, right: Sequence[Any]) -> head._type:
        return left

    new_func = function(join_func)
    new_impl = impl.FunctionCall(new_func, (head, tail))

    return Object(new_impl)


class _StructSymbolFactory:
    """
    Utility for simply create struct objects
    """

    def new(self, fields: Sequence[Tuple[str, Any]]) -> Object:
        out_fields = []
        for name, value in fields:
            out_fields.append(impl.StructField(name, Object(value)))
        return Object(impl.Struct(out_fields))

    def dict(self, data: Dict[str, Any]) -> Object:
        return self.new(data.items())

    def __call__(
        self, *args: Sequence[Tuple[str, Any]], **kwargs: Dict[str, Any]
    ) -> Object:
        """
        'call' factory method.
        """
        items = []
        items.extend(args)
        items.extend(kwargs.items())
        return self.new(args)

    def __getitem__(self, fields: Sequence[slice]) -> Object:
        """
        'getitem' interfce for creating structs using __getitem__ syntax
        """
        out_fields = []
        for slc in fields:
            out_fields.append((slc.start, slc.stop))
        return self.new(out_fields)


struct = _StructSymbolFactory()
