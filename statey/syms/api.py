"""
This module contains data structures that are only used in order to write
expressive code, and basically boil down to factories for the data structures
contained in the the other modules in this package
"""
import dataclasses as dc
import inspect
from functools import wraps
from typing import Any, Sequence, Dict, Callable, Type, Tuple, Union, Optional

import statey as st
from statey.syms import utils, types, func, impl, Object, path as path_mod, session
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


def declarative(
    func: Callable[[Any], Any] = utils.MISSING,
    name_key: Callable[[str], str] = lambda x: x,
    ignore: Callable[[str], str] = lambda x: x.startswith("_"),
) -> Callable[[Any], Any]:
    """
    Wrap the given function as a function that declares statey objects
    """

    def dec(_func):
        @wraps(_func)
        def wrapper(session, *args, name_key=name_key, ignore=ignore, **kwargs):
            @utils.scope_update_handler
            def update_handler(frame, name, value):
                absolute = name_key(name)
                # Ignore reassignments
                if (
                    ignore(name)
                    or isinstance(value, st.Object)
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
        st.registry.register(ParseDataClassPlugin(cls))
        st.registry.register(EncodeDataClassPlugin(cls))
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
    head = Object(head)

    tail_type = st.registry.infer_type(tail)

    def join_func(left: head._type, right: tail_type) -> head._type:
        return left

    return utils.wrap_function_call(join_func, (head, tail)) >> head


def struct_replace(obj: Object, keep_type=True, **kwargs: Dict[str, Any]) -> Object:
    """
    Replace the given attributes in `object`
    """
    if isinstance(obj, types.Type):
        type_fields = []

        if not hasattr(obj, "fields"):
            raise TypeError(f"Invalid input type for replace(): {obj}")

        for field in obj.fields:
            if field.name in kwargs:
                continue
            type_fields.append(field)

        for key, value in kwargs.items():
            type_fields.append(types.Field(key, value))

        return obj.with_fields(type_fields)

    if not hasattr(obj._type, "fields"):
        raise TypeError(f"Invalid input type for replace(): {obj._type}")

    out = []
    for field in obj._type.fields:
        if field.name in kwargs:
            out.append((field.name, kwargs[field.name]))
        else:
            out.append((field.name, obj[field.name]))

    return Object(dict(out), obj._type) if keep_type else struct.new(out)


def struct_interpolate_one(
    obj: Object,
    path: str,
    new_value: Any,
    keep_type: bool = True,
    path_parser: Optional[path_mod.PathParser] = None,
) -> Object:
    """
    Interpolate a new value in a struct object or type, possibly
    nested
    """
    if path_parser is None:
        path_parser = path_mod.PathParser()

    comps = path_parser.split(path)
    if len(comps) > 1:
        head, *tail = comps
        rel_path = path_parser.join(tail)

        if isinstance(obj, types.Type):
            if not hasattr(obj, "fields"):
                raise TypeError(f"Invalid type for struct_interpolate: {obj}")

            field_type = obj[head].type
            new_type = struct_interpolate_one(
                obj=field_type,
                path=rel_path,
                new_value=new_value,
                path_parser=path_parser,
                keep_type=keep_type,
            )
            return struct_replace(obj, keep_type, **{head: new_type})

        field_value = obj[head]
        new_value = struct_interpolate_one(
            obj=field_value,
            path=rel_path,
            new_value=new_value,
            path_parser=path_parser,
            keep_type=keep_type,
        )
        return struct_replace(obj, keep_type, **{head: new_value})

    return struct_replace(obj, keep_type, **{path: new_value})


def struct_interpolate(
    obj: Object,
    new_values: Dict[str, Any],
    keep_type: bool = True,
    path_parser: Optional[path_mod.PathParser] = None,
) -> Object:
    """
    Extend struct_interpolate_one to allow providing a dictionary
    of new values
    """
    if path_parser is None:
        path_parser = path_mod.PathParser()

    out = obj
    for key, value in new_values.items():
        out = struct_interpolate_one(
            obj=out,
            path=key,
            new_value=value,
            keep_type=keep_type,
            path_parser=path_parser,
        )

    return out


def fill(
    obj: Object, output_type: types.StructType, get_value: Callable[[str], Any]
) -> Object:
    """
    Convert `obj` to `output_type` by filling missing fields with values resolved from get_value
    """
    if not hasattr(obj._type, "fields"):
        raise TypeError(f"Invalid input type for replace(): {obj._type}")

    if isinstance(get_value, Object):
        get_value = get_value.__getitem__

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


def ifnull(obj: Object, otherwise: Any) -> Object:
    """
    If the object is None, substitute the value from the other object
    """
    non_nullable = obj._type.with_nullable(otherwise is None)

    def _ifnull(first: obj._type) -> non_nullable:
        return otherwise if first is None else first

    _ifnull.__name__ = "ifnull"
    return function(_ifnull)(obj, otherwise)


def struct_drop(obj: Union[Object, types.Type], *fields: str) -> Object:
    """
    Drop the fields with the given names from `obj`
    """
    if isinstance(obj, types.Type):
        type_fields = []

        if not hasattr(obj, "fields"):
            raise TypeError(f"Invalid input type for replace(): {obj}")

        for field in obj.fields:
            if field.name in fields:
                continue
            type_fields.append(field)

        return st.StructType(type_fields, nullable=obj.nullable, meta=obj.meta)

    if not hasattr(obj._type, "fields"):
        raise TypeError(f"Invalid input type for replace(): {obj._type}")

    out = {}

    new_type = struct_drop(obj._type, *fields)
    for field in new_type.fields:
        out[field.name] = obj[field.name]

    res = Object(out, new_type)
    return res


def struct_add(
    obj: Union[Object, types.Type], *fields: Sequence[Tuple[str, Any, types.Type]]
) -> Object:
    """
    Add the given fields to `obj`
    """
    if isinstance(obj, types.Type):

        if not hasattr(obj, "fields"):
            raise TypeError(f"Invalid input type for replace(): {obj}")

        type_fields = list(obj.fields)

        for tup in fields:
            if len(tup) == 2:
                name, typ = tup
                typ = st.registry.get_type(typ)
                type_fields.append(types.Field(name, typ))
            else:
                raise ValueError(f"Invalid tuple length: {len(tup)} for {tup}.")

        return st.StructType(type_fields, nullable=obj.nullable, meta=obj.meta)

    if not hasattr(obj._type, "fields"):
        raise TypeError(f"Invalid input type for replace(): {obj._type}")

    type_fields = list(obj._type.fields)
    values = {field.name: obj[field.name] for field in fields}

    for tup in fields:
        if len(tup) == 2:
            name, obj = tup
            obj = st.registry.get_object(obj)
        elif len(tup) == 3:
            name, obj, typ = tup
            obj = Object(obj, typ)
        else:
            ValueError(f"Invalid tuple length: {len(tup)} for {tup}.")

        type_fields.append(types.Field(name, obj._type))
        values[name] = obj

    new_type = st.StructType(
        type_fields, nullable=obj._type.nullable, meta=obj._type.meta
    )
    return Object(values, new_type)


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


def str(input: Any) -> Object:
    @function
    def str(input: st.Any) -> st.String:
        return __builtins__["str"](input)

    return str(input)


def int(input: Any) -> Object:
    @function
    def int(input: st.Any) -> st.Integer:
        return __builtins__["int"](input)

    return int(input)
