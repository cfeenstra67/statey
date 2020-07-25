import os
import shutil
from typing import Dict, Any

import statey as st
from statey import (
    Machine,
    transition,
    MachineResource,
    S,
    TaskSession,
    task,
    StateSnapshot,
    StateConfig,
)
from statey.syms import types, utils, Object, impl


FileConfigType = S.Struct["location" : S.String, "data" : S.String].t


StatSchema = S.Struct[
    "mode" : S.Integer,
    "ino" : S.Integer,
    "dev" : S.Integer,
    "nlink" : S.Integer,
    "uid" : S.Integer,
    "gid" : S.Integer,
    "size" : S.Integer,
    "atime" : S.Float,
    "mtime" : S.Float,
    "ctime" : S.Float,
].s


StatType = StatSchema.output_type


FileType = S.Struct["location" : S.String, "data" : S.String, "stat":StatSchema].t


class FileMachine(Machine):
    """
    Simple file state machine
    """

    UP = st.State("UP", FileConfigType, FileType)
    DOWN = st.NullState("DOWN")

    async def refresh(self, current: StateSnapshot) -> StateSnapshot:
        state = current.state.state
        if state == self.null_state.state:
            return current
        if not os.path.isfile(current.data["location"]):
            return StateSnapshot({}, self.null_state)
        data = self.get_file_info(current.data["location"])
        return StateSnapshot(data, current.state)

    async def finalize(self, current: StateSnapshot) -> StateSnapshot:
        return StateSnapshot(dict(current.data, data=""), current.state)

    @staticmethod
    def get_file_info(path: str) -> FileType:
        location = os.path.realpath(path)
        with open(location) as f:
            data = f.read()

        stat_info = os.stat(path)
        return {
            "location": location,
            "data": data,
            "stat": {
                "mode": stat_info.st_mode,
                "ino": stat_info.st_ino,
                "dev": stat_info.st_dev,
                "nlink": stat_info.st_nlink,
                "uid": stat_info.st_uid,
                "gid": stat_info.st_gid,
                "size": stat_info.st_size,
                "atime": stat_info.st_atime,
                "mtime": stat_info.st_mtime,
                "ctime": stat_info.st_ctime,
            },
        }

    @staticmethod
    def get_file_expected(config: st.StateConfig) -> Dict[str, Any]:
        data = config.data
        return {
            "location": st.map(os.path.realpath, data["location"]),
            "data": data["data"],
            "stat": st.Object(impl.Unknown(return_type=StatType)),
        }

    @task.new
    async def remove_file(self, path: str) -> types.EmptyType:
        """
        Delete the file
        """
        os.remove(path)
        return {}

    @task.new
    async def set_file(self, data: FileConfigType) -> FileType:
        """
        Set the file's contents
        """
        path = os.path.realpath(data["location"])
        with open(path, "w+") as f:
            f.write(data["data"])
        return self.get_file_info(path)

    @task.new
    async def rename_file(self, from_path: str, to_path: str) -> FileType:
        from_path = os.path.realpath(from_path)
        to_path = os.path.realpath(to_path)
        os.rename(from_path, to_path)
        return self.get_file_info(to_path)

    @transition("UP", "UP")
    def modify(
        self,
        current: StateSnapshot,
        config: StateConfig,
        session: TaskSession
    ) -> Object:

        differ = session.ns.registry.get_differ(current.state.input_type)
        diffconfig = differ.config()
        expected = self.get_file_expected(config)

        def compare_realpaths(x, y):
            return os.path.realpath(x) == os.path.realpath(y)

        diffconfig.set_comparison("location", compare_realpaths)

        diff = differ.diff(current.data, config.data)
        flat = list(diff.flatten(diffconfig))
        if not flat:
            return current.obj

        paths = {d.path for d in flat}
        loc_changed = ("location",) in paths
        data_changed = ("data",) in paths

        # If location changes only we can just rename the file
        if loc_changed and not data_changed:
            return session["rename_file"] << (
                self.rename_file(current.obj.location, config.ref.location) >> expected
            )

        if loc_changed:
            session["delete_file"] << self.remove_file(current.obj.location)
            return session["create_file"] << (self.set_file(config.ref) >> expected)

        return session["update_file"] << (self.set_file(config.ref) >> expected)

    @transition("DOWN", "UP")
    def create(
        self,
        current: StateSnapshot,
        config: StateConfig,
        session: TaskSession
    ) -> Object:

        expected = self.get_file_expected(config)
        return session["create_file"] << (self.set_file(config.ref) >> expected)

    @transition("UP", "DOWN")
    def delete(
        self,
        current: StateSnapshot,
        config: StateConfig,
        session: TaskSession
    ) -> Object:

        session["delete_file"] << self.remove_file(current.obj.location)
        return st.Object({})


# Declaring global resources

file_resource = MachineResource("file", FileMachine)

# Resource state factory
File = file_resource.s


RESOURCES = [
    file_resource
]


def register() -> None:
    """
	Register default resources in this module
	"""
    for resource in RESOURCES:
        st.registry.register_resource(resource)
