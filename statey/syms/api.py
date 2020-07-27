"""
This module contains data structures that are only used in order to write
expressive code, and basically boil down to factories for the data structures
contained in the the other modules in this package
"""
import dataclasses as dc
import inspect
from functools import wraps
from typing import Any, Sequence, Dict, Callable, Type, Tuple

import statey as st
from statey.syms import utils, types, func, impl, Object
from statey.syms.plugins import ParseDataClassPlugin, EncodeDataClassPlugin


def function(
    func: Callable[[Any], Any],
    type: types.Type = utils.MISSING,
    registry: "Registry" = utils.MISSING,
) -> func.Function:
    """
    Construct a Function object for a regular python function
    """
    return utils.native_function(func, type, registry)


def map(
    func: Callable[[Any], Any], value: Any, return_type: types.Type = utils.MISSING
) -> Any:
    """
    Apply the function to the given object. If it is an object, convert `func` to a native
    function and apply it to the object's underlying value
    """
    if not isinstance(value, Object):
        return func(value)
    func_type = utils.single_arg_function_type(value._type, return_type)
    func_obj = function(func, func_type)
    return value._inst.map(func_obj)


def declarative(func: Callable[[Any], Any] = utils.MISSING) -> Callable[[Any], Any]:
    """
    Wrap the given function as a function that declares statey objects
    """

    def dec(_func):
        @wraps(_func)
        def wrapper(session, *args, name_key=lambda x: x, **kwargs):
            @utils.scope_update_handler
            def update_handler(frame, name, value):
                absolute = name_key(name)
                # Ignore reassignments
                if (
                    isinstance(value, st.Object)
                    and isinstance(value._impl, impl.Reference)
                    and value._impl.path == absolute
                ):
                    return

                try:
                    typ = session.ns.resolve(absolute)
                except st.exc.SymbolKeyError:
                    annotations = frame.f_locals.get("__annotations__", {})
                    name_annotation = annotations.get(absolute, utils.MISSING)
                    frame.f_locals[name] = session[absolute:name_annotation] << value
                else:
                    session.set_data(absolute, value)
                    frame.f_locals[name] = session.ns.ref(absolute)

            update_handler(_func)(session, *args, **kwargs)
            return session

        return wrapper

    return dec if utils.is_missing(func) else dec(func)


@dc.dataclass(frozen=True)
class _FunctionFactory(utils.Cloneable):
    """
	An interface for creating function call objects
	"""

    annotation: Any = utils.MISSING

    def __call__(self, func: Callable[[Any], Any]) -> "_FunctionFactoryWithFunction":
        typ = self.annotation
        if not utils.is_missing(typ):
            return_type = st.registry.get_type(typ)
            typ = utils.function_type(inspect.signature(func), return_type=return_type)
        return function(func, typ)

    def __getitem__(self, annotation: Any) -> "_FunctionFactory":
        # Use double square brackets to indicate the annotation is the function/callable
        if isinstance(annotation, list) and len(annotation) == 1:
            return self.clone(annotation=annotation[0])(annotation[0])
        return self.clone(annotation=annotation)


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
    tail_type = st.registry.infer_type(tail)

    def join_func(left: head._type, right: tail_type) -> head._type:
        return left

    return utils.wrap_function_call(join_func, (head, tail)) >> head


def replace(obj: Object, **kwargs: Dict[str, Any]) -> Object:
    """
    Replace the given attributes in `object`
    """
    if not hasattr(obj._type, "fields"):
        raise TypeError(f"Invalid input type for replace(): {obj._type}")

    out = {}
    for field in obj._type.fields:
        if field.name in kwargs:
            out[field.name] = kwargs[field.name]
        else:
            out[field.name] = obj[field.name]

    return Object(out, obj._type, obj._registry)


def fill(
    obj: Object, output_type: types.StructType, get_value: Callable[[str], Any]
) -> Object:
    """
    Convert `obj` to `output_type` by filling missing fields with values resolved from get_value
    """
    if not hasattr(obj._type, "fields"):
        raise TypeError(f"Invalid input type for replace(): {obj._type}")

    fields = {field.name: field for field in obj._type.fields}

    out = {}
    for field in output_type.fields:
        if field.name in fields:
            out[field.name] = obj[field.name]
        else:
            out[field.name] = get_value(field.name)

    return Object(out, output_type, obj._registry)


def filter_struct(obj: Object, output_type: types.StructType):
    """
    fill() which will raise an error if any exist that appears in output_type
    doesn't appear in `obj`
    """

    def getter(name):
        raise ValueError(f"Unknown field {name}.")

    return fill(obj, output_type, getter)


def fill_unknowns(obj: Object, output_type: types.StructType) -> Object:
    """
    Convert `obj` to `output_type` by filling missing values with unknowns
    """
    unknown = Object(impl.Unknown(return_type=output_type), output_type, obj._registry)

    def get_value(name):
        return unknown[name]

    return fill(obj, output_type, get_value)


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
