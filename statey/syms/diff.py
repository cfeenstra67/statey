import abc
import dataclasses as dc
import operator
from typing import (
    Sequence,
    Any,
    Iterator,
    Dict,
    Type as PyType,
    Tuple,
    Callable,
    Union,
    Optional,
)

import statey as st
from statey.syms import types, utils, impl, Object
from statey.syms.path import PathParser


PathLike = Union[Sequence[Any], str]


def get_path_from_pathlike(
    path_like: PathLike, path_parser: PathParser
) -> Sequence[Any]:
    if isinstance(path_like, str):
        path_like = path_parser.split(path_like)
    return path_like


@dc.dataclass
class DiffConfig(utils.Cloneable):
    """
    Simple class to customize behavior when diffing
    """

    path_comparisons: Dict[Sequence[Any], Callable[[Any, Any], bool]] = dc.field(
        default_factory=dict
    )
    path_parser: PathParser = dc.field(default_factory=PathParser)

    def set_comparison(self, path: PathLike, func: Callable[[Any, Any], bool]) -> None:
        """
        Modify this config in place, setting the comparison for the given path
        """
        self.path_comparisons[get_path_from_pathlike(path, self.path_parser)] = func

    def get_explicit_comparison(
        self, path: PathLike
    ) -> Optional[Callable[[Any, Any], bool]]:
        """
        Get the comparison function for a given path, _only_ if it has been explicitly
        set using set_comparison(). Otherwise, return None
        """
        path = get_path_from_pathlike(path, self.path_parser)
        if path in self.path_comparisons:
            return self.path_comparisons[path]
        return None

    def get_comparison(self, path: PathLike) -> Callable[[Any, Any], bool]:
        """
        Get the comparison function for a given path
        """
        comp = self.get_explicit_comparison(path)
        return operator.eq if comp is None else comp


@dc.dataclass(frozen=True)
class DiffComponent:
    """
    A diff represents the differences between two values. Either left or right can contain unknowns, 
    however neither should contain objects (i.e. they should be either fully resolve or the result of
    Session.resolve() with allow_unknowns=True)
    """

    left: Any
    right: Any
    differ: "Differ"
    path: Sequence[Any] = ()

    def __bool__(self) -> bool:
        try:
            next(self.flatten())
        except StopIteration:
            return True
        return False

    def left_is_unknown(self) -> bool:
        """
        Indicate whether the left side of this diff is an `Unknown` instance
        """
        return isinstance(self.left, Object) and isinstance(
            self.left._impl, impl.Unknown
        )

    def right_is_unknown(self) -> bool:
        """
        Indicate whether the right side of this diff is an `Unknown` instance
        """
        return isinstance(self.right, Object) and isinstance(
            self.right._impl, impl.Unknown
        )

    def flatten(self, config: Optional[DiffConfig] = None) -> Iterator["DiffComponent"]:
        """
        Flatten this diff into any non-empty sub-diffs
        """
        return self.differ.flatten(self, config)


@dc.dataclass(frozen=True)
class Diff:
    """
    A flattened out diff object, suitable for actual inspection
    """

    components: Sequence[DiffComponent]
    path_parser: PathParser

    def _get_path(self, path_like: PathLike) -> Sequence[Any]:
        if isinstance(path_like, str):
            path_like = self.path_parser.split(path_like)
        return path_like

    def __iter__(self) -> Iterator[DiffComponent]:
        """
        Iterate through component diffs
        """
        return iter(self.components)

    def __bool__(self) -> bool:
        """
        Indicate whether this flat diff is empty
        """
        return bool(self.components)

    def __getitem__(self, path_like: PathLike) -> DiffComponent:
        """
        Get a diff by path
        """
        comps = get_path_from_pathlike(path_like, self.path_parser)
        for diff in self.components:
            if comps == tuple(diff.path):
                return diff
        raise KeyError(comps)

    def __contains__(self, path_like: PathLike) -> bool:
        """
        Indicate whether a diff at the given path exists
        """
        comps = get_path_from_pathlike(path_like, self.path_parser)
        for diff in self.components:
            if comps == tuple(diff.path)[:len(comps)]:
                return True
        return False


class Differ(abc.ABC):
    """
    A differ provides a simple way to get a flat set of differences from a potentially
    complex nested object
    """

    type: types.Type

    def config(self) -> DiffConfig:
        """
        Helper function to grab a fresh
        """
        return DiffConfig()

    @abc.abstractmethod
    def flatten(
        self, diff: DiffComponent, config: Optional[DiffConfig] = None
    ) -> Iterator[DiffComponent]:
        """
        Flatten this diff into any non-empty sub-diffs
        """
        raise NotImplementedError

    def diff(
        self,
        left: Any,
        right: Any,
        session: Optional["Session"] = None,
        config: Optional[DiffConfig] = None,
    ) -> Diff:
        """
        Construct a Diff object from this differ and the given data
        """
        if session is not None:
            left_obj = Object(left, self.type, session.ns.registry)
            left = session.resolve(left_obj, decode=False, allow_unknowns=True)
            right_obj = Object(right, self.type, session.ns.registry)
            right = session.resolve(right_obj, decode=False, allow_unknowns=True)

        if config is None:
            config = self.config()

        diff = DiffComponent(left, right, self)
        diffs = list(diff.flatten(config))

        return Diff(diffs, config.path_parser)


@dc.dataclass(frozen=True)
class ValueDiffer(Differ):
    """
    Differ for a single value
    """

    type: types.Type

    def is_empty(self, diff: "Diff", config: DiffConfig) -> bool:
        if diff.left_is_unknown() or diff.right_is_unknown():
            return False
        comp = config.get_comparison(diff.path)
        return comp(diff.left, diff.right)

    def flatten(
        self, diff: DiffComponent, config: Optional[DiffConfig] = None
    ) -> Iterator[DiffComponent]:
        if config is None:
            config = self.config()

        if not self.is_empty(diff, config):
            yield diff

    @classmethod
    @st.hookimpl
    def get_differ(cls, type: types.Type, registry: st.Registry) -> "Differ":
        if not isinstance(
            type,
            (types.IntegerType, types.FloatType, types.BooleanType, types.StringType),
        ):
            return None
        return cls(type)


@dc.dataclass(frozen=True)
class ArrayDiffer(Differ):
    """
    Differ for arrays
    """

    type: types.Type
    element_differ: Differ

    def element_diffs(self, diff: DiffComponent) -> Sequence[DiffComponent]:
        """
        Construct diffs for each of the elements of the givn diff
        """
        out = []

        for idx, (left_item, right_item) in enumerate(zip(diff.left, diff.right)):
            sub_diff = DiffComponent(
                differ=self.element_differ,
                left=left_item,
                right=right_item,
                path=tuple(diff.path) + (idx,),
            )
            out.append(sub_diff)

        if len(diff.left) > len(diff.right):
            for idx, left_item in enumerate(diff.left[len(diff.right) :]):
                sub_diff = DiffComponent(
                    differ=self.element_differ,
                    left=left_item,
                    right=None,
                    path=tuple(diff.path) + (idx + len(diff.right),),
                )
                out.append(sub_diff)

        if len(diff.right) > len(diff.left):
            for idx, right_item in enumerate(diff.right[len(diff.left) :]):
                sub_diff = DiffComponent(
                    differ=self.element_differ,
                    left=None,
                    right=right_item,
                    path=tuple(diff.path) + (idx + len(diff.left),),
                )
                out.append(sub_diff)

        return out

    def flatten(
        self, diff: DiffComponent, config: Optional[DiffConfig] = None
    ) -> Iterator[DiffComponent]:
        """
        Yield the diffs for each element in left and right. This assumes these arrays are of the
        same length
        """
        if config is None:
            config = self.config()

        if diff.left_is_unknown() or diff.right_is_unknown():
            yield diff
            return

        explicit_comp = config.get_explicit_comparison(diff.path)
        if explicit_comp is not None:
            res = explicit_comp(diff.left, diff.right)
            if not res:
                yield diff
            if res is not NotImplemented:
                return

        if diff.left is None or diff.right is None:
            if diff.left is None and diff.right is None:
                return
            yield diff
            return

        if len(diff.left) != len(diff.right):
            yield diff
            return

        for diff in self.element_diffs(diff):
            yield from diff.flatten(config)

    @classmethod
    @st.hookimpl
    def get_differ(cls, type: types.Type, registry: st.Registry) -> "Differ":
        if not isinstance(type, types.ArrayType):
            return None
        element_differ = registry.get_differ(type.element_type)
        return cls(type, element_differ)


@dc.dataclass(frozen=True)
class StructDiffer(Differ):
    """
    Differ for arrays
    """

    type: types.Type
    field_differs: Dict[str, Differ]

    def field_diffs(self, diff: DiffComponent) -> Dict[str, DiffComponent]:
        """
        Construct diffs for each field given the input diff
        """
        out = {}

        for key, differ in self.field_differs.items():
            left_value = diff.left[key]
            right_value = diff.right[key]
            out[key] = DiffComponent(
                differ=differ,
                left=left_value,
                right=right_value,
                path=tuple(diff.path) + (key,),
            )

        return out

    def flatten(
        self, diff: DiffComponent, config: Optional[DiffConfig] = None
    ) -> Iterator[DiffComponent]:
        if config is None:
            config = self.config()

        if diff.left_is_unknown() or diff.right_is_unknown():
            yield diff
            return

        explicit_comp = config.get_explicit_comparison(diff.path)
        if explicit_comp is not None:
            res = explicit_comp(diff.left, diff.right)
            if not res:
                yield diff
            if res is not NotImplemented:
                return

        if diff.left is None or diff.right is None:
            if diff.left is None and diff.right is None:
                return
            yield diff
            return

        for diff in self.field_diffs(diff).values():
            yield from diff.flatten(config)

    @classmethod
    @st.hookimpl
    def get_differ(cls, type: types.Type, registry: st.Registry) -> "Differ":
        if not isinstance(type, types.StructType):
            return None
        field_differs = {}
        for field in type.fields:
            field_differs[field.name] = registry.get_differ(field.type)
        return cls(type, field_differs)


@dc.dataclass(frozen=True)
class MapDiffer(Differ):
    """
    Differ for arrays
    """

    type: types.Type
    value_differ: Differ

    def element_diffs(self, diff: DiffComponent) -> Sequence[DiffComponent]:
        """
        Construct diffs for each of the elements of the givn diff
        """
        out = []

        left_keys = set(diff.left)
        right_keys = set(diff.right)

        for key in left_keys - right_keys:
            sub_diff = DiffComponent(
                differ=self.value_differ,
                left=diff.left[key],
                right=None,
                path=tuple(diff.path) + (key,),
            )
            out.append(sub_diff)

        for key in right_keys - left_keys:
            sub_diff = DiffComponent(
                differ=self.value_differ,
                left=diff.right[key],
                right=None,
                path=tuple(diff.path) + (key,),
            )
            out.append(sub_diff)

        for key in left_keys & right_keys:
            sub_diff = DiffComponent(
                differ=self.value_differ,
                left=diff.left[key],
                right=diff.right[key],
                path=tuple(diff.path) + (key,),
            )
            out.append(sub_diff)

        return out

    def flatten(
        self, diff: DiffComponent, config: Optional[DiffConfig] = None
    ) -> Iterator[DiffComponent]:
        """
        Yield the diffs for each element in left and right. This assumes these arrays are of the
        same length
        """
        if config is None:
            config = self.config()

        if diff.left_is_unknown() or diff.right_is_unknown():
            yield diff
            return

        explicit_comp = config.get_explicit_comparison(diff.path)
        if explicit_comp is not None:
            res = explicit_comp(diff.left, diff.right)
            if not res:
                yield diff
            if res is not NotImplemented:
                return

        if diff.left is None or diff.right is None:
            if diff.left is None and diff.right is None:
                return
            yield diff
            return

        if len(diff.left) != len(diff.right):
            yield diff
            return

        for diff in self.element_diffs(diff):
            yield from diff.flatten(config)

    @classmethod
    @st.hookimpl
    def get_differ(cls, type: types.Type, registry: st.Registry) -> "Differ":
        if not isinstance(type, types.MapType):
            return None
        value_differ = registry.get_differ(type.value_type)
        return cls(type, value_differ)


DIFFER_HOOKS = [ValueDiffer, ArrayDiffer, StructDiffer, MapDiffer]


def register(registry: Optional["Registry"] = None) -> None:
    if registry is None:
        registry = st.registry

    for plugin in DIFFER_HOOKS:
        registry.register(plugin)
