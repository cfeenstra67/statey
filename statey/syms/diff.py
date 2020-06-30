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


@dc.dataclass
class DiffConfig(utils.Cloneable):
    """
    Simple class to customize behavior when diffing
    """

    path_comparisons: Dict[Sequence[Any], Callable[[Any, Any], bool]] = dc.field(
        default_factory=dict
    )
    path_parser: PathParser = dc.field(default_factory=PathParser)

    def _get_path(self, path_like: PathLike) -> Sequence[Any]:
        if isinstance(path_like, str):
            path_like = self.path_parser.split(path_like)
        return path_like

    def set_comparison(self, path: PathLike, func: Callable[[Any, Any], bool]) -> None:
        """
        Modify this config in place, setting the comparison for the given path
        """
        self.path_comparisons[self._get_path(path)] = func

    def get_comparison(self, path: PathLike) -> Callable[[Any, Any], bool]:
        """
        Get the comparison function for a given path
        """
        path = self._get_path(path)
        if path in self.path_comparisons:
            return self.path_comparisons[path]
        return operator.eq


@dc.dataclass(frozen=True)
class Diff:
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
        return isinstance(self.left, Object) and isinstance(self.left._impl, impl.Unknown)

    def right_is_unknown(self) -> bool:
        """
        Indicate whether the right side of this diff is an `Unknown` instance
        """
        return isinstance(self.right, Object) and isinstance(self.right._impl, impl.Unknown)

    def flatten(self, config: Optional[DiffConfig] = None) -> Iterator["Diff"]:
        """
        Flatten this diff into any non-empty sub-diffs
        """
        return self.differ.flatten(self, config)


class Differ(abc.ABC):
    """
    A differ provides a simple way to get a flat set of differences from a potentially
    complex nested object
    """

    def config(self) -> DiffConfig:
        """
        Helper function to grab a fresh
        """
        return DiffConfig()

    @abc.abstractmethod
    def flatten(
        self, diff: Diff, config: Optional[DiffConfig] = None
    ) -> Iterator[Diff]:
        """
        Flatten this diff into any non-empty sub-diffs
        """
        raise NotImplementedError

    def diff(self, left: Any, right: Any) -> Diff:
        """
        Construct a Diff object from this differ and the given data
        """
        return Diff(left, right, self)


@dc.dataclass(frozen=True)
class ValueDiffer(Differ):
    """
    Differ for a single value
    """

    def is_empty(self, diff: "Diff", config: DiffConfig) -> bool:
        if diff.left_is_unknown() or diff.right_is_unknown():
            return False
        comp = config.get_comparison(diff.path)
        return comp(diff.left, diff.right)

    def flatten(
        self, diff: Diff, config: Optional[DiffConfig] = None
    ) -> Iterator[Diff]:
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
        return cls()


@dc.dataclass(frozen=True)
class ArrayDiffer(Differ):
    """
    Differ for arrays
    """

    element_differ: Differ

    def element_diffs(self, diff: Diff) -> Sequence[Diff]:
        """
        Construct diffs for each of the elements of the givn diff
        """
        out = []

        for idx, (left_item, right_item) in enumerate(zip(diff.left, diff.right)):
            sub_diff = Diff(
                differ=self.element_differ,
                left=left_item,
                right=right_item,
                path=tuple(diff.path) + (idx,),
            )
            out.append(sub_diff)

        if len(diff.left) > len(diff.right):
            for idx, left_item in enumerate(diff.left[len(diff.right) :]):
                sub_diff = Diff(
                    differ=self.element_differ,
                    left=left_item,
                    right=utils.MISSING,
                    path=tuple(diff.path) + (idx + len(diff.right),),
                )
                out.append(sub_diff)

        if len(diff.right) > len(diff.left):
            for idx, right_item in enumerate(diff.right[len(diff.left) :]):
                sub_diff = Diff(
                    differ=self.element_differ,
                    left=utils.MISSING,
                    right=right_item,
                    path=tuple(diff.path) + (idx + len(diff.left),),
                )
                out.append(sub_diff)

        return out

    def flatten(
        self, diff: Diff, config: Optional[DiffConfig] = None
    ) -> Iterator[Diff]:
        """
        Yield the diffs for each element in left and right. This assumes these arrays are of the
        same length
        """
        if config is None:
            config = self.config()

        if diff.left_is_unknown() or diff.right_is_unknown():
            yield diff
            return

        if diff.left is None or diff.right is None:
            if diff.left is None and diff.right is None:
                return
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
        return cls(element_differ)


@dc.dataclass(frozen=True)
class StructDiffer(Differ):
    """
    Differ for arrays
    """

    field_differs: Dict[str, Differ]

    def field_diffs(self, diff: Diff) -> Dict[str, Diff]:
        """
        Construct diffs for each field given the input diff
        """
        out = {}

        for key, differ in self.field_differs.items():
            left_value = diff.left[key]
            right_value = diff.right[key]
            out[key] = Diff(
                differ=differ,
                left=left_value,
                right=right_value,
                path=tuple(diff.path) + (key,),
            )

        return out

    def flatten(
        self, diff: "Diff", config: Optional[DiffConfig] = None
    ) -> Iterator[Diff]:
        if config is None:
            config = self.config()

        if diff.left_is_unknown() or diff.right_is_unknown():
            yield diff
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
        return cls(field_differs)


DIFFER_HOOKS = [ValueDiffer, ArrayDiffer, StructDiffer]


def register() -> None:
    for plugin in DIFFER_HOOKS:
        st.registry.pm.register(plugin)
