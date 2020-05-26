import abc
import dataclasses as dc
from typing import Dict, Any, Optional

import pluggy

from statey.hooks import hookspec, create_plugin_manager
from statey.syms import types, utils, exc


class Registry(abc.ABC):
	"""
	A type registry is used to parse annotations into types
	"""
	@abc.abstractmethod
	def get_type(self, annotation: Any, meta: Optional[Dict[str, Any]] = None) -> types.Type:
		"""
		Parse the given annotation and return a Type. This will properly handle dataclasses
		similarly to how case classes are handled in spark encoding
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def infer_type(self, obj: Any) -> types.Type:
		"""
		Attempt to infer the type of `obj`, falling back on self.any_type
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def get_encoder(self, type: types.Type) -> 'Encoder':
		"""
		Given a type, return an Encoder instance to encode the type, raising an exc.NoEncoderFound to
		indicate failure
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def get_semantics(self, type: types.Type) -> 'Semantics':
		"""
		Given a type, get the semantics to use for symbols of that type.
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def get_type_serializer(self, type: types.Type) -> 'TypeSerializer':
		"""
		Given a type, get the type serializer to turn it into a JSON-serializable value.
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def get_type_serializer_from_data(self, data: Any) -> 'TypeSerializer':
		"""
		Given serialized data, get a type serializer that can decode a serialized type of
		that name into a native Type
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def register_resource(self, resource: 'Resource') -> None:
		"""
		Register the given resource
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def get_resource(self, name: str) -> 'Resource':
		"""
		Get the resource with the given name
		"""
		raise NotImplementedError


class RegistryHooks:
	"""
	Specifies hooks for handling different annotations and converting them to types
	"""
	@hookspec(firstresult=True)
	def get_type(self, annotation: Any, registry: Registry, meta: Dict[str, Any]) -> types.Type:
		"""
		Handle the given annotation and return a Type, or None to indicate it can't be handled by this hook
		"""

	@hookspec(firstresult=True)
	def infer_type(self, obj: Any, registry: Registry) -> types.Type:
		"""
		Infer a type given an object
		"""

	@hookspec(firstresult=True)
	def get_encoder(self, type: types.Type, registry: Registry) -> 'Encoder':
		"""
		Handle the given type and produce an Encoder instance that can encode values of that type
		"""

	@hookspec(firstresult=True)
	def get_semantics(self, type: types.Type, registry: Registry) -> 'Semantics':
		"""
		Handle the given type and produce a Semantics instance for symbols of that type
		"""

	@hookspec(firstresult=True)
	def get_type_serializer(self, type: types.Type, registry: Registry) -> 'TypeSerializer':
		"""
		Handle the given type and produce an TypeSerializer instance that can encode values of that type
		"""

	@hookspec(firstresult=True)
	def get_type_serializer_from_data(self, data: Any, registry: Registry) -> 'TypeSerializer':
		"""
		Handle the given type and produce a TypeSerializer instance for symbols of that type
		"""


def create_registry_plugin_manager():
	"""
	Factory function to create the default base plugin manager for DefaultTypeRegistry
	"""
	pm = create_plugin_manager()
	pm.add_hookspecs(RegistryHooks)
	return pm


@dc.dataclass(frozen=True)
class DefaultRegistry(Registry):

	pm: pluggy.PluginManager = dc.field(init=False, default_factory=create_registry_plugin_manager, compare=False, repr=False)

	def get_type(self, annotation: Any, meta: Optional[Dict[str, Any]] = None) -> types.Type:
		if meta is None:
			meta = {}
		handled = self.pm.hook.get_type(
			annotation=annotation,
			meta=meta,
			registry=self
		)
		if handled is None:
			raise exc.NoTypeFound(annotation)
		return handled

	def infer_type(self, obj: Any) -> types.Type:
		handled = self.pm.hook.infer_type(
			obj=obj,
			registry=self
		)
		if handled is not None:
			return handled
		annotation = utils.infer_annotation(obj)
		return self.get_type(annotation)

	def get_encoder(self, type: types.Type) -> 'Encoder':
		handled = self.pm.hook.get_encoder(
			type=type,
			registry=self
		)
		if handled is None:
			raise exc.NoEncoderFound(type)
		return handled

	def get_semantics(self, type: types.Type) -> 'Semantics':
		handled = self.pm.hook.get_semantics(
			type=type,
			registry=self
		)
		if handled is None:
			raise exc.NoSemanticsFound(type)
		return handled

	def get_type_serializer(self, type: types.Type) -> 'TypeSerializer':
		handled = self.pm.hook.get_type_serializer(
			type=type,
			registry=self
		)
		if handled is None:
			raise exc.NoTypeSerializerFoundForType(type)
		return handled

	def get_type_serializer_from_data(self, data: Any) -> 'TypeSerializer':
		handled = self.pm.hook.get_type_serializer_from_data(
			data=data,
			registry=self
		)
		if handled is None:
			raise exc.NoTypeSerializerFoundForData(data)
		return handled

	def _get_registered_resource_name(self, resource_name: str) -> str:
		return f'resource:{resource_name}'

	def register_resource(self, resource: 'Resource') -> None:
		self.pm.register(resource, name=self._get_registered_resource_name(resource.name))

	def get_resource(self, name: str) -> 'Resource':
		resource = self.pm.get_plugin(self._get_registered_resource_name(name))
		if resource is None:
			raise exc.NoResourceFound(name)
		return resource