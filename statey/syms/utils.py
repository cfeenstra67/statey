import collections
import dataclasses as dc
import inspect
import sys
from contextlib import contextmanager
from functools import wraps, partial, reduce
from itertools import product
from typing import Type, Optional, Union, Any, Dict, Hashable, Sequence, Callable

import marshmallow as ma
import networkx as nx

from statey import NS, exc


LRU_MAX_SIZE = 100

STRING_TYPES = (str, bytes)


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
    """
    Extract a single inner annotation from an outer annotation
    """
    res = extract_inner_annotations(annotation)
    if not res:
        return None
    return res[0]


def extract_inner_annotations(annotation: Any) -> Any:
    """
    Extract a all inner annotations from `annotation`
    """
    if not getattr(annotation, "__args__", None):
        return None
    return annotation.__args__


def is_sequence_annotation(annotation: Any) -> bool:
    """
    Indicates if the given annotation might be a Sequence[type]
    """
    if not hasattr(annotation, "mro"):
        return False
    if not callable(annotation.mro):
        return False
    if collections.abc.Sequence not in annotation.mro():
        return False
    return True


def is_mapping_annotation(annotation: Any) -> bool:
    """
    Indicates if the given annotation might be a Dict[type] or
    Mapping[type] annotation
    """
    if not hasattr(annotation, "mro"):
        return False
    if not callable(annotation.mro):
        return False
    if not (dict in annotation.mro() or collections.abc.Mapping in annotation.mro()):
        return False
    return True


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

    def __init__(
        self,
        field: ma.fields.Field,
        type: "Type",
        registry: "Registry",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.field = field
        self.type = type
        self.registry = registry

    def _serialize(self, value: Any, attr: str, obj: str, **kwargs) -> Any:
        from statey.syms import Object

        # Just pass this through when serializing
        if isinstance(value, Object):
            return value
        self.field.name = self.name
        return self.field._serialize(value, attr, obj, **kwargs)

    def _deserialize(
        self,
        value: Any,
        attr: Optional[str] = None,
        data: Optional[Any] = None,
        **kwargs,
    ) -> Any:
        from statey.syms import types, Object

        self.field.name = self.name
        if not isinstance(value, Object):
            return self.field._deserialize(value, attr, data, **kwargs)

        if self.type != value._type:
            try:
                caster = self.registry.get_caster(value._type, self.type)
            except exc.NoCasterFound as err:
                raise ma.ValidationError(
                    f"Invalid symbol type (expected {self.type}, got {value._type}).",
                    field_name=self.name
                ) from err
            return caster.cast(value)
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
        and not isinstance(obj, STRING_TYPES)
        and obj
        and reduce(lambda x, y: x is y, map(type, obj))
    ):
        return Sequence[infer_annotation(obj[0])]
    return obj_type


def function_type(
    sig: inspect.Signature,
    return_type: "Type" = MISSING,
    registry: "Registry" = MISSING,
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

    if is_missing(return_type):
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
        type = function_type(inspect.signature(input), registry=registry)
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


def trace_func(func: Callable[[Any], Any], cb: Callable[[Any, Any, Any], None]) -> None:
    @wraps(func)
    def wrapper(*args, **kwargs):
        prev = sys.gettrace()
        trace_frame = sys._getframe(0)
        call_frame = None

        def tracer(frame, event, arg):
            nonlocal call_frame
            # Only immediate child frame should be the function call
            if frame.f_back is not trace_frame:
                return
            call_frame = frame
            cb(frame, event, arg)
            # 'done' is a fake event that we fire just to finalize
            # the namespace, so we don't want to pass it to the previous
            # tracer function
            if prev is not None and event != "done":
                prev(frame, event, arg)
            return tracer

        sys.settrace(tracer)
        try:
            return func(*args, **kwargs)
        finally:
            sys.settrace(prev)
            if call_frame is not None:
                tracer(call_frame, "done", None)

    return wrapper


def locals_history(frame):
    """
    Yield a function that, each time it is called, will return the "updated"
    members of the scope of the given frame vs. the previous call
    """
    current_locals = frame.f_locals.copy()

    def diff():
        nonlocal current_locals

        new_locals = frame.f_locals.copy()

        updated = {
            key: loc
            for key, loc in new_locals.items()
            if key not in current_locals or loc is not current_locals[key]
        }
        deleted = {key for key in current_locals if key not in new_locals}
        current_locals = new_locals

        return updated, deleted

    diff.frame = frame

    return diff


def make_tracer(diff_cb: Callable[[Any, Any, Any], None]):
    """
    Wrap the given handler function as a tracing function
    """
    current_history = None

    def tracer(frame, event, arg):
        nonlocal current_history

        if current_history is None:
            current_history = locals_history(frame)

        diff_cb(frame, *current_history())

        return tracer

    return tracer


def locals_diff_tracer(diff_cb: Callable[[Any, Any, Any], None]):
    """
    Decorator to wrap the given function as a trace function
    """
    tracer = make_tracer(diff_cb)

    def dec(func):
        return trace_func(func, cb=tracer)

    return dec


def scope_update_handler(handler: Callable[[Any, str, Any], None]):
    """
    Handle updates to the locals scope using a tracing function
    """

    @locals_diff_tracer
    def differ(frame, updated, deleted):
        for key, val in updated.items():
            handler(frame, key, val)

    return differ


def check_dag(graph: nx.DiGraph) -> None:
    """
    Check if the given graph is a DAG, raising NotADagError otherwise
    """
    try:
        cycle = nx.find_cycle(graph)
    except nx.NetworkXNoCycle:
        return

    edges_left, edges_right = zip(*cycle)
    cycle_path = edges_left + edges_right[-1:]

    raise exc.NotADagError(graph, cycle_path)
