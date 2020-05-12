"""
Contains classes related to building resource paths
"""
from typing import Optional, Type

import dataclasses as dc

from .resource import Resource


@dc.dataclass(frozen=True)
class PathBuilder:
    """
	Default data class used to compute paths
	"""

    name: Optional[str] = dc.field(default=None)
    type_name: Optional[str] = dc.field(default=None)

    @classmethod
    def from_resource(cls, resource: Resource) -> "PathBuilder":
        """
        Create a PathBuilder instance from the given resource
        """
        return cls(name=resource.name, type_name=resource.type_name)

    def copy(self, **kwargs) -> "PathBuilder":
        """
        Make a copy of this PathBuilder with the given attributes replaces. Similar
        to the copy() method of scala case classes
        """
        kws = self.__dict__.copy()
        kws.update(kwargs)
        return type(self)(**kws)

    def path(self) -> str:
        """
        Return the path for this resource
        """
        return f'{self.type_name or ""}:/{self.name or ""}'


# pylint: disable=too-few-public-methods
class Registry:
    """
	Default statey path builder class. Can be subclassed and
	passed to the State() constructor if desired
	"""

    def __init__(self, path_builder_cls: Type[PathBuilder] = PathBuilder) -> None:
        self.path_builder_cls = path_builder_cls

    def get_path(self, resource: Resource) -> str:
        """
        Return the path for the given resource.
        """
        return self.path_builder_cls.from_resource(resource).path()
