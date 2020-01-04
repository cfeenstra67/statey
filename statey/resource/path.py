import copy
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

	def copy(self, **kwargs) -> 'PathBuilder':
		kws = self.__dict__.copy()
		kws.update(kwargs)
		return type(self)(**kws)

	def path(self) -> str:
		return f'urn:type:{self.type_name or ""}:path:/{self.name or ""}'


class Registry:
	"""
	Default statey path builder class. Can be subclassed and
	passed to the State() constructor if desired
	"""
	def __init__(self, path_builder_cls: Type[PathBuilder] = PathBuilder) -> None:
		self.path_builder_cls = path_builder_cls

	def get_path(self, resource: Resource) -> str:
		return self.path_builder_cls(
			name=resource.name,
			type_name=resource.type_name
		).path()
