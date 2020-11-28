import abc
import dataclasses as dc
import math
from itertools import chain
from typing import Tuple, Sequence, Any

from statey.syms import utils


class BasePathParser(abc.ABC):
    """
    Defines the interfact that path parsers must implement
    """

    @abc.abstractmethod
    def validate_name(self, name: str) -> None:
        """
        Validate that the given name can be allocated with this path parser.
        Throw an exception if not.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def split(self, path: str) -> Sequence[Any]:
        """
        Split a path into its native-typed components
        """
        raise NotImplementedError

    @abc.abstractmethod
    def join(self, components: Sequence[Any]) -> str:
        """
        Join the natively-typed components into a path string
        """
        raise NotImplementedError


class PathParser(BasePathParser):
    """
    A path parser joins path components into a string and back again

    Paths like a[:1]/b/cdefa[*]/g

    supports encoding/decoding of utils.EXPLODE, strings, integers and slices
    """

    pathsep: str = "."
    explode_key: str = "*"
    idx_wrap: Tuple[str, str] = ("[", "]")

    @property
    def idx_wrap_left(self) -> str:
        return self.idx_wrap[0]

    @property
    def idx_wrap_right(self) -> str:
        return self.idx_wrap[1]

    def validate_name(self, name: str) -> None:
        for reserved_string in [self.pathsep, *self.idx_wrap]:
            if reserved_string in name:
                raise ValueError(
                    f"Key {name} cannot contain reserved string: ({repr(reserved_string)})."
                )

    def split(self, path: str) -> Sequence[Any]:
        out = []
        # Find first index of pathsep or idx_wrap_left
        try:
            first_sep = path.index(self.pathsep)
        except ValueError:
            first_sep = float("inf")

        try:
            first_idx_wrap = path.index(self.idx_wrap_left)
            # Just checking it exists after the first one
            path.index(self.idx_wrap_right, first_idx_wrap)
        except ValueError:
            first_idx_wrap = float("inf")

        # Nothing left to be parsed
        if not math.isfinite(min(first_sep, first_idx_wrap)):
            return (path,)

        if first_sep < first_idx_wrap:
            start, tail = path.split(self.pathsep, 1)
            return (start,) + self.split(tail)

        pre, post = path.split(self.idx_wrap_left, 1)
        idx_str, post = post.split(self.idx_wrap_right, 1)
        idx_strings = [idx_str]

        # Add any additional index strings
        while post.startswith(self.idx_wrap_left):
            if self.idx_wrap_right not in post:
                break
            idx_str, post = post.split(self.idx_wrap_right, 1)
            idx_str = idx_str[len(self.idx_wrap_left) :]
            idx_strings.append(idx_str)

        if post and not post.startswith(self.pathsep):
            raise ValueError(f'Invalid key: "{path}".')
        # Strip leading pathsep off next component
        post = post[len(self.pathsep) :]

        out = [pre]
        for idx_str in idx_strings:
            # This is a slice
            if idx_str == self.explode_key:
                out.append(utils.EXPLODE)
            elif ":" in idx_str:
                comps = idx_str.split(":")
                if len(comps) == 2:
                    start, stop = comps
                    step = None
                else:
                    start, stop, step = comps

                start, stop, step = map(
                    lambda x: int(x) if x else None, [start, stop, step]
                )
                out.append(slice(start, stop, step))
            # Regular index
            else:
                out.append(int(idx_str))

        if not post:
            return tuple(out)
        return tuple(out) + self.split(post)

    def join(self, components: Sequence[Any]) -> str:
        out = []
        skip = False
        for item, next_item in zip(components, chain(components[1:], [object()])):
            if skip:
                skip = False
                continue
            # For array indices we'll join them here
            if isinstance(next_item, int):
                out.append(
                    "".join(
                        [item, self.idx_wrap_left, str(next_item), self.idx_wrap_right]
                    )
                )
                skip = True
            elif isinstance(next_item, slice):

                def string_value(x):
                    return "" if x is None else str(x)

                if next_item.step is not None:
                    slice_str = ":".join(
                        map(
                            string_value,
                            [next_item.start, next_item.stop, next_item.step],
                        )
                    )
                else:
                    slice_str = (
                        ":".join(map(string_value, [next_item.start, next_item.stop]))
                        or ":"
                    )
                out.append(
                    "".join([item, self.idx_wrap_left, slice_str, self.idx_wrap_right])
                )
                skip = True
            elif next_item is utils.EXPLODE:
                out.append(
                    "".join(
                        [
                            item,
                            self.idx_wrap_left,
                            self.explode_key,
                            self.idx_wrap_right,
                        ]
                    )
                )
                skip = True
            else:
                out.append(item)

        return self.pathsep.join(out)
