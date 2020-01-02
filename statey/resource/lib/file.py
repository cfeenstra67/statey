import os

import statey as st


class File(st.Resource):
	"""
	A statey resource for a local file containing some data
	"""
	class Schema:
		path = st.Field[str](create_new=True)
		data = st.Field[str](computed=True, create_new=True)
		permissions = st.Field[int](default=0o644)

	@abc.abstractmethod
	def create(self, current: st.SchemaSnapshot) -> st.SchemaSnapshot:
		"""
		Create this resource. Return the latest snapshot
		"""
		with open(current.path, 'w+') as f:
			f.write(current.data)
		os.chmod(current.path, current.permissions)

	@abc.abstractmethod
	def destroy(self, current: st.SchemaSnapshot) -> None:
		"""
		Destroy this resource
		"""
		os.remove(current.path)

	@abc.abstractmethod
	def refresh(self, current: st.SchemaSnapshot) -> Optional[SchemaSnapshot]:
		"""
		Refresh the state of this resource

		Returns Snapshot if the resource exists, otherwise None
		"""
		if not os.path.isfile(current.path):
			return None

		permissions = os.stat(current.path).st_mode & 0o777
		with open(current.path) as f:
			content = f.read()

		return current.copy(
			permissions=permissions,
			content=content
		)

	@abc.abstractmethod
	def update(self, old: SchemaSnapshot, current: st.SchemaSnapshot, spec: 'Update') -> SchemaSnapshot:
		"""
		Update this resource with the values given by `spec`.
		"""
		for field, (old, new) in spec.values.items():
			if field == 'permissions':
				os.chmod(current.path, new)
			elif field == 'content':
				with open(current.path, 'w+') as f:
					f.write(current.content)
			else:
				raise ValueError(f'Updates not supported for field "{field}".')
