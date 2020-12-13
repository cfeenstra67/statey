import os
import shutil
from typing import Dict, Any, Optional

import statey as st
from statey import (
    Machine,
    transition,
    TaskSession,
    task,
    StateSnapshot,
    StateConfig,
)
from statey.syms import types, utils, Object, impl


FileConfigType = st.Struct["location":str, "data":str]


StatType = st.Struct[
    "mode":int,
    "ino":int,
    "dev":int,
    "nlink":int,
    "uid":int,
    "gid":int,
    "size":int,
    "atime":float,
    "mtime":float,
    "ctime":float,
]


FileType = st.Struct["location":str, "data":str, "stat":StatType]


@st.function
def realpath(path: str) -> str:
    """
    Wrapper for os.path.realpath
    """
    return os.path.realpath(path)


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
        with_realpath = st.struct_replace(
            config.obj, location=realpath(config.obj.location)
        )
        return st.fill_unknowns(with_realpath, FileType)

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
    async def modify(
        self, current: StateSnapshot, config: StateConfig, session: TaskSession
    ) -> Object:

        differ = session.ns.registry.get_differ(current.state.input_type)
        diffconfig = differ.config()
        expected = self.get_file_expected(config)

        def compare_realpaths(x, y):
            return os.path.realpath(x) == os.path.realpath(y)

        diffconfig.set_comparison("location", compare_realpaths)

        current_as_config = st.filter_struct(current.obj, config.type)
        diff = differ.diff(current_as_config, config.obj, session, diffconfig)
        if not diff:
            return current.obj

        loc_changed = "location" in diff
        data_changed = "data" in diff

        # If location changes only we can just rename the file
        if loc_changed and not data_changed:
            return session["rename_file"] << (
                self.rename_file(current.obj.location, config.obj.location) >> expected
            )

        if loc_changed:
            raise st.exc.NullRequired

        return session["update_file"] << (self.set_file(config.obj) >> expected)

    @transition("DOWN", "UP")
    async def create(
        self, current: StateSnapshot, config: StateConfig, session: TaskSession
    ) -> Object:

        expected = self.get_file_expected(config)
        return session["create_file"] << (self.set_file(config.obj) >> expected)

    @transition("UP", "DOWN")
    async def delete(
        self, current: StateSnapshot, config: StateConfig, session: TaskSession
    ) -> Object:

        session["delete_file"] << self.remove_file(current.obj.location)
        return st.Object({})


# Declaring global resources

File = FileMachine("file")


RESOURCES = [File]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register default resources in this module
    """
    if registry is None:
        registry = st.registry

    for resource in RESOURCES:
        registry.register(resource)
