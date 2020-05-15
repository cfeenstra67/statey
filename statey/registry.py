import abc
import dataclasses as dc
from typing import Dict, Any, Optional

import pluggy

import statey as st
from statey import create_plugin_manager
from statey.syms.encoders import Encoder
from statey.resource import Resource
from statey.syms import types, utils


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

	def infer_type(self, obj: Any) -> types.Type:
		"""
		Attempt to infer the type of `obj`, falling back on self.any_type
		"""
		annotation = utils.infer_annotation(obj)
		return self.get_type(annotation)

	@abc.abstractmethod
	def get_encoder(self, type: types.Type) -> Encoder:
		"""
		Given a type, return an Encoder instance to encode the type, raising an exc.NoEncoderFound to
		indicate failure
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def register_resource(self, resource: Resource) -> None:
		"""
		Register the given resource
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def get_resource(self, name: str) -> Resource:
		"""
		Get the resource with the given name
		"""
		raise NotImplementedError


class RegistryHooks:
	"""
	Specifies hooks for handling different annotations and converting them to types
	"""
	@st.hookspec(firstresult=True)
	def get_type(self, annotation: Any, registry: Registry, meta: Dict[str, Any]) -> types.Type:
		"""
		Handle the given annotation and return a Type, or None to indicate it can't be handled by this hook
		"""

	@st.hookspec(firstresult=True)
	def infer_type(self, obj: Any, registry: Registry) -> types.Type:
		"""
		Infer a type given an object
		"""

	@st.hookspec(firstresult=True)
	def get_encoder(self, type: types.Type, registry: Registry) -> Encoder:
		"""
		Handle the given type and produce an Encoder instance that can encode values of that type
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

	any_type: types.Type = types.AnyType()
	pm: pluggy.PluginManager = dc.field(init=False, default_factory=create_registry_plugin_manager, compare=False, repr=False)

	def get_type(self, annotation: Any, meta: Optional[Dict[str, Any]] = None) -> types.Type:
		"""
		Parse the given annotation and return a Type. This will properly handle dataclasses
		similarly to how case classes are handled in spark encoding
		"""
		if meta is None:
			meta = {}
		handled = self.pm.hook.get_type(
			annotation=annotation,
			meta=meta,
			registry=self
		)
		return self.any_type if handled is None else handled

	def infer_type(self, obj: Any) -> types.Type:
		"""
		Attempt to infer the type of `obj`, falling back on self.any_type
		"""
		handled = self.pm.hook.infer_type(
			obj=obj,
			registry=self
		)
		if handled is not None:
			return handled
		annotation = utils.infer_annotation(obj)
		return self.get_type(annotation)

	def get_encoder(self, type: types.Type) -> Encoder:
		"""
		Given a type, get an Encoder instance that can encoder it
		"""
		handled = self.pm.hook.get_encoder(
			type=type,
			registry=self
		)
		if handled is None:
			raise exc.NoEncoderFound(type)
		return handled

	def register_resource(self, resource: Resource) -> None:
		"""
		Register the given resource
		"""
		self.pm.register(resource, name=resource.name)

	def get_resource(self, name: str) -> Resource:
		"""
		Get the resource with the given name
		"""
		return self.pm.get_plugin(name)
