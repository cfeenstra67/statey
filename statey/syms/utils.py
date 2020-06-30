import dataclasses as dc
import inspect
from contextlib import contextmanager
from functools import partial, reduce
from itertools import product
from typing import Type, Optional, Union, Any, Dict, Hashable, Sequence, Callable

import marshmallow as ma
import networkx as nx

from statey import NS, exc


LRU_MAX_SIZE = 100


@dc.dataclass(frozen=True)
class Global:
    """
	An arbitrary named value intended to be used at the global level
	"""

    name: str

    def __repr__(self) -> str:
        return self.name


MISSING = Global("MISSING")


EXPLODE = Global("EXPLODE")


def extract_optional_annotation(annotation: Any) -> Any:
    """
	Determine if the given annotation is an Optional field
	"""
    if (
        hasattr(annotation, "__origin__")
        and annotation.__origin__ is Union
        and getattr(annotation, "__args__", None) is not None
        and len(annotation.__args__) == 2
        and annotation.__args__[-1] is type(None)
    ):
        return extract_inner_annotation(annotation)
    return None


def extract_inner_annotation(annotation: Any) -> Any:
    if not getattr(annotation, "__args__", None):
        return None
    return annotation.__args__[0]


def dict_get_path(
    data: Dict[Hashable, Any], path: Sequence[Hashable], explode: bool = True
) -> Any:
    """
	Get the key at the given path from the dictionary
	"""
    current = data
    current_path = list(path)
    while current_path:
        comp = current_path.pop(0)
        if comp is EXPLODE and explode:
            return [dict_get_path(item, current_path) for item in current]
        elif comp is EXPLODE:
            continue
        current = current[comp]

    return current


def map_dims(
    func: Callable[[Any], Any], arr: Sequence[Any], ndims: int = 1
) -> Sequence[Any]:
    """
	Apply func to the elements of a multi-dimensional array with `ndims` dimensions
	"""
    if ndims == 0:
        return func(arr)
    return [map_dims(func, item, ndims - 1) for item in arr]


def dict_set_path(
    data: Dict[Hashable, Any], path: Sequence[Hashable], value: Any
) -> Any:
    """
	Set the given value at the given path in the dict
	"""
    current = data
    current_path = path[:-1]
    while current_path:
        comp = current_path.pop(0)
        if comp is EXPLODE:
            for item in current:
                dict_set_path(item, current_path, value)
            return
        current = current.setdefault(comp, {})

    current[path[-1]] = value


@contextmanager
def reraise_ma_validation_error():
    try:
        yield
    except ma.ValidationError as err:
        raise exc.InputValidationError(err.messages) from err


class PossiblySymbolicField(ma.fields.Field):
    """
	A field that will validate the type if given a symbol or the value if given anything else
	"""

    def __init__(self, field: ma.fields.Field, type: "Type", *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.field = field
        self.type = type

    def _serialize(self, value: Any, attr: str, obj: str, **kwargs) -> Any:
        from statey.syms import Object

        # Just pass this through when serializing
        if isinstance(value, Object):
            return value
        return self.field._serialize(value, attr, obj, **kwargs)

    def _deserialize(
        self,
        value: Any,
        attr: Optional[str] = None,
        data: Optional[Any] = None,
        **kwargs,
    ) -> Any:
        from statey.syms import types, Object

        if not isinstance(value, Object):
            return self.field._deserialize(value, attr, data, **kwargs)

        if self.type != value._type:
            raise ma.ValidationError(
                f"Invalid symbol type (expected {self.type}, got {value._type})."
            )
        return value

    def __repr__(self) -> str:
        return f"{type(self).__name__}[{self.type}]({type(self.field).__name__}(...))"


class SingleValueFunction(ma.fields.Function):
    """
	Use encoding and decoding functions to process a specific field
	"""

    _CHECK_ATTRIBUTE = True

    def _serialize(self, value, attr, obj, **kwargs):
        return self._call_or_raise(self.serialize_func, value, attr)


def filter_dict(
    keep: Callable[[Any], bool], data: Dict[Hashable, Any], and_sequences: bool = True
) -> Dict[Hashable, Any]:
    """
	Similar to the filter() function, but applied recursively to a dict
	"""
    out = {}
    for key, val in data.items():
        if isinstance(val, dict):
            filtered = filter_dict(keep, val, and_sequences)
            if filtered:
                out[key] = filtered
        if and_sequences and isinstance(val, Sequence) and and_sequences:
            items = out[key] = []
            for item in val:
                if isinstance(item, dict):
                    item = filter_dict(keep, item, and_sequences)
                    if item:
                        items.append(item)
                elif keep(item):
                    items.append(item)
            if items:
                out[key] = items
        elif keep(val):
            out[key] = val
    return out


def invert_filter(func: Callable[[Any], bool]) -> Callable[[Any], bool]:
    """
	Return a new function that returns `not func(x)`
	"""

    def wrapper(x):
        return not func(x)

    return wrapper


def is_missing(value: Any) -> bool:
    """
    Check if a value is equal to MISSING
    """
    return isinstance(value, Global) and value == MISSING


def infer_annotation(obj: Any) -> Any:
    """
	Attempt to infer an annotation from obj, falling back on `type(obj)`
	"""
    obj_type = type(obj)
    if (
        isinstance(obj, Sequence)
        and obj
        and reduce(lambda x, y: x is y, map(type, obj))
    ):
        return Sequence[infer_annotation(obj[0])]
    return obj_type


def function_type(
    sig: inspect.Signature, registry: "Registry" = MISSING
) -> "FunctionType":
    """
    Convert a python function signature to a FunctionType object
    """
    from statey.syms import types

    if is_missing(registry):
        import statey

        registry = statey.registry

    def get_type(annotation):
        if annotation is inspect._empty:
            return types.AnyType()
        return registry.get_type(annotation)

    out_fields = []
    for name, param in sig.parameters.items():
        out_fields.append(types.Field(name, get_type(param.annotation)))

    return_type = get_type(sig.return_annotation)
    return types.NativeFunctionType(tuple(out_fields), return_type)


def single_arg_function_type(
    from_type: "Type", to_type: "Type" = MISSING, arg_name: str = "x", **kwargs
) -> "FunctionType":
    """
    Return a NativeFunctionType for a simple single-argument function
    """
    from statey.syms import types

    if is_missing(to_type):
        to_type = from_type
    fields = (types.Field(arg_name, from_type),)
    return types.NativeFunctionType(fields, to_type, **kwargs)


def native_function(
    input: Callable[[Any], Any], type: "Type" = MISSING, registry: "Registry" = MISSING
) -> "Function":
    """
    Construct a Function object for a regular python function
    """
    from statey.syms import func

    if is_missing(type):
        type = function_type(inspect.signature(input), registry)
    kws = {}
    if hasattr(input, "__name__"):
        kws["name"] = input.__name__
    return func.NativeFunction(type, input, **kws)


def wrap_function_call(
    func: Union["Function", Callable[[Any], Any]],
    args: Sequence[Any] = (),
    kwargs: Optional[Dict[str, Any]] = None,
    registry: Optional["Registry"] = None,
    return_annotation: Any = MISSING,
) -> "Object":

    from statey.syms import api, impl, types, func as func_module, Object

    if registry is None:
        from statey import registry

    if kwargs is None:
        kwargs = {}

    args = tuple(map(lambda x: Object(x, registry=registry), args))
    kwargs = {key: Object(val, registry=registry) for key, val in kwargs.items()}

    if isinstance(func, func_module.Function):
        function_obj = func
    else:
        try:
            function_obj = native_function(func, registry=registry)
        except ValueError:
            return_annotation = (
                func.return_annotation
                if is_missing(return_annotation)
                else return_annotation
            )
            return_type = registry.get_type(return_annotation)

            fields = []
            for idx, arg in enumerate(args):
                fields.append(types.Field(str(idx), arg._type))

            for name, val in kwargs.items():
                fields.append(types.Field(name, val._type))

            func_type = types.NativeFunctionType(tuple(fields), return_type)
            function_obj = func_module.NativeFunction(func_type, func)

    return Object(impl.FunctionCall(function_obj, args, kwargs), registry=registry)


def bind_function_args(
    func_type: "FunctionType", args: Sequence[Any], kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Given a function type, bind the given args and kwargs to names
    """
    out_args = {}

    for field, arg in zip(func_type.args, args):
        out_args[field.name] = arg

    names = {arg.name for arg in func_type.args}
    out_args.update({key: val for key, val in kwargs.items() if key in names})
    return out_args


def encodeable_dataclass_fields(data: Any) -> Sequence[dc.field]:
    """.
	Respect the statey-specific field metadtaa and don't encode fields where statey.encode = False
	"""
    out = []
    for field in dc.fields(data):
        syms_meta = field.metadata.get(NS) or {}
        encode = syms_meta.get("encode", True)
        if encode:
            out.append(field)
    return out


class Cloneable:
    """
	Mixin for dataclasses adding a clone() method
	"""

    def clone(self, **kwargs) -> "Cloneable":
        """
		Return a copy of the current object with the given attributes replaced. This current
		object should be an instance of a dataclass
		"""
        return dc.replace(self, **kwargs)


def subgraph_retaining_dependencies(dag: nx.DiGraph, keep_nodes: Sequence[str]) -> None:
    """
	Remove nodes while retaining any indirect dependencies between them. Modifies the graph
	in place.
	"""
    for node in list(nx.topological_sort(dag)):
        if node in keep_nodes:
            continue
        for predecessor, successor in product(dag.pred[node], dag.succ[node]):
            dag.add_edge(predecessor, successor)
        dag.remove_node(node)


@dc.dataclass(frozen=True)
class Location(Cloneable):
    """
    Object that simply keeps track of where it is upon successive __getitem__
    operations
    """

    path: Sequence[Any] = ()

    def __getitem__(self, attr: Any) -> "Location":
        return self.clone(path=tuple(self.path) + (attr,))
