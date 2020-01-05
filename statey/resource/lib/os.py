"""
Generic operating system-related resources like files
"""
import hashlib
import os
from typing import Optional

import aiofiles
import aiofiles.os as aios

import statey as st


def _get_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


class File(st.Resource):
    """
    A statey resource for a local file containing some data
    """

    type_name = "file"

    # pylint: disable=missing-docstring
    class Schema(st.Schema):
        path = st.Field[str](create_new=True)
        data = st.Field[str](store=False)
        data_sha256 = st.Field[str](
            input=False, factory=lambda resource: st.Func[str](_get_hash, resource.attrs.data),
        )
        permissions = st.Field[int](default=0o644)
        size_bytes = st.Field[int](computed=True)

    async def create(self, current: st.SchemaSnapshot) -> st.SchemaSnapshot:
        """
        Create this resource. Return the latest snapshot
        """
        dirname, _ = os.path.split(current.path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)

        async with aiofiles.open(current.path, mode="w+") as file:
            await file.write(current.data)
        size_bytes = os.path.getsize(current.path)
        os.chmod(current.path, current.permissions)
        return current.copy(size_bytes=size_bytes)

    async def destroy(self, current: st.SchemaSnapshot) -> None:
        """
        Destroy this resource
        """
        os.remove(current.path)

    async def refresh(self, current: st.SchemaSnapshot) -> Optional[st.SchemaSnapshot]:
        """
        Refresh the state of this resource

        Returns Snapshot if the resource exists, otherwise None
        """
        if not os.path.exists(current.path):
            return None

        size_bytes = os.path.getsize(current.path)
        stat = await aios.stat(current.path)
        permissions = stat.st_mode & 0o777
        async with aiofiles.open(current.path) as file:
            content = await file.read()

        return current.copy(permissions=permissions, data=content, size_bytes=size_bytes)

    async def update(
        self, old: st.SchemaSnapshot, current: st.SchemaSnapshot, spec: "Update"
    ) -> st.SchemaSnapshot:
        """
        Update this resource with the values given by `spec`.
        """
        new_values = {}
        for field in spec.fields:
            new_value = new_values[field] = current[field]
            if field == "permissions":
                os.chmod(current.path, new_value)
            elif field == "data_sha256":
                async with aiofiles.open(current.path, mode="w+") as file:
                    await file.write(current.data)
            else:
                raise ValueError(f'Updates not supported for field "{field}".')

        # Recalc size_bytes
        return await self.refresh(current.copy(**new_values))
