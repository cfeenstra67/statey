import asyncio
from typing import Dict, Any, Optional

import pylumi

import statey as st
from statey.ext.pulumi.constants import PULUMI_ID
from statey.ext.pulumi.exc import PulumiValidationError
from statey.ext.pulumi.helpers import object_to_pulumi_json


class PulumiResourceMachine(st.SingleStateMachine):
    """"""

    def make_output(
        self, data: Dict[str, Any], id: str, typ: st.Type
    ) -> Dict[str, Any]:
        """"""
        data = dict(data, **{PULUMI_ID: id})
        # Trim any extra fields
        data = {
            field.name: data[field.name] for field in typ.fields if field.name in data
        }
        # Add in default values for any required but missing fields
        for field in typ.fields:
            if field.name in data and data[field.name] is None:
                del data[field.name]

            if field.type.nullable or field.name in data:
                continue

            if isinstance(field.type, st.NumberType):
                data[field.name] = 0
            elif isinstance(field.type, st.StringType):
                data[field.name] = ""
            elif isinstance(field.type, st.BooleanType):
                data[field.name] = False
            elif isinstance(field.type, st.ArrayType):
                data[field.name] = []
            elif isinstance(field.type, (st.MapType, st.StructType)):
                data[field.name] = {}

        encoder = st.registry.get_encoder(typ)
        return encoder.encode(data)

    def make_expected_output(
        self, data: Dict[str, Any], config: st.StateConfig, typ: st.Type
    ) -> st.Object:
        """"""
        stripped = {key: val for key, val in data.items() if val is not None}

        drop_fields = []
        for field in typ.fields:
            if field.name not in stripped:
                drop_fields.append(field.name)

        sub_type = st.struct_drop(typ, *drop_fields)
        data_obj = st.Object(stripped, sub_type)

        unknown = st.Unknown[typ]
        config_type = config.obj._type
        config_fields = {field.name: field for field in config_type.fields}
        registry = config.obj._registry

        def get_value(key):
            this_field = typ[key]
            if key in config_fields:
                config_field = config_fields[key]

                try:
                    caster = registry.get_caster(config_field.type, this_field.type)
                except st.exc.NoCasterFound:
                    pass
                else:
                    return caster.cast(config.obj[key])

                non_nullable_config = config_field.type.with_nullable(False)
                try:
                    caster = registry.get_caster(non_nullable_config, this_field.type)
                except st.exc.NoCasterFound:
                    pass
                else:
                    return caster.cast(st.ifnull(config.obj[key], unknown[key]))

            return unknown[key]

        return st.fill(data_obj, typ, get_value)

    def clean_check_resp(
        self, resp: Dict[str, Any], type: st.Type, registry: st.Registry
    ) -> Dict[str, Any]:
        """
        Clean the check() response
        """
        if isinstance(resp, pylumi.UnknownValue):
            return st.Unknown[type]

        if isinstance(resp, list) and isinstance(type, st.ArrayType):
            return [
                self.clean_check_resp(item, type.element_type, registry)
                for item in resp
            ]

        if not isinstance(resp, dict):
            return resp

        out = {}

        semantics = registry.get_semantics(type)
        for key, val in resp.items():
            if key in {"__defaults"}:
                continue
            val_semantics = semantics.attr_semantics(key)
            out[key] = self.clean_check_resp(val, val_semantics.type, registry)

        return out

    async def get_action(
        self,
        current: st.StateSnapshot,
        config: st.StateConfig,
        session: st.TaskSession,
    ) -> st.ModificationAction:

        loop = asyncio.get_running_loop()

        check_resp, errs = await loop.run_in_executor(
            self.provider.thread_pool,
            self.provider.pulumi_provider.check,
            pylumi.URN(self.name),
            current.data,
            object_to_pulumi_json(config.obj, session),
        )
        check_resp = self.clean_check_resp(
            check_resp, self.UP.input_type, session.ns.registry
        )
        if errs:
            raise PulumiValidationError(errs)

        current_data = current.data.copy()
        pulumi_id = current_data.pop(PULUMI_ID)
        diff_resp = await loop.run_in_executor(
            self.provider.thread_pool,
            self.provider.pulumi_provider.diff,
            pylumi.URN(self.name, pulumi_id),
            pulumi_id,
            current_data,
            check_resp,
        )

        if not diff_resp["DetailedDiff"]:
            return st.ModificationAction.NONE

        replace_kinds = {
            pylumi.DiffKind.ADD_REPLACE,
            pylumi.DiffKind.DELETE_REPLACE,
            pylumi.DiffKind.UPDATE_REPLACE,
        }

        for key, val in diff_resp["DetailedDiff"].items():
            kind = pylumi.DiffKind(val["Kind"])
            if kind in replace_kinds:
                return st.ModificationAction.DELETE_AND_RECREATE

        return st.ModificationAction.MODIFY

    async def get_expected(
        self, current: st.StateSnapshot, config: st.StateConfig, session: st.TaskSession
    ) -> Any:

        loop = asyncio.get_running_loop()

        current_is_up = current.state.name == "UP"
        config_is_up = config.state.name == "UP"

        if not config_is_up:
            return {}

        config_json = object_to_pulumi_json(config.obj, session)
        check_resp, errs = await loop.run_in_executor(
            self.provider.thread_pool,
            self.provider.pulumi_provider.check,
            pylumi.URN(self.name),
            current.data,
            config_json,
            True,
        )
        check_resp = self.clean_check_resp(
            check_resp, self.UP.input_type, session.ns.registry
        )
        check_resp = object_to_pulumi_json(
            st.Object(check_resp, self.UP.input_type), session
        )
        if errs:
            raise PulumiValidationError(errs)

        if not current_is_up:
            resp = await loop.run_in_executor(
                self.provider.thread_pool,
                self.provider.pulumi_provider.create,
                pylumi.URN(self.name),
                check_resp,
                self.provider.operation_timeout,
                True,
            )
            props = self.clean_check_resp(
                resp["Properties"], self.UP.output_type, session.ns.registry
            )
            return self.make_expected_output(props, config, self.UP.output_type)

        current_data = current.data.copy()
        pulumi_id = current_data.pop(PULUMI_ID)
        diff_resp = await loop.run_in_executor(
            self.provider.thread_pool,
            self.provider.pulumi_provider.diff,
            pylumi.URN(self.name, pulumi_id),
            pulumi_id,
            current_data,
            check_resp,
        )

        output = current.data.copy()
        for key in diff_resp["ChangedKeys"]:
            output.pop(key, None)

        return self.make_expected_output(output, config, self.UP.output_type)

    async def refresh_state(self, data: Any) -> Optional[Any]:
        loop = asyncio.get_running_loop()

        data = data.copy()
        pulumi_id = data.pop(PULUMI_ID)
        resp = await loop.run_in_executor(
            self.provider.thread_pool,
            self.provider.pulumi_provider.read,
            pylumi.URN(self.name, pulumi_id),
            pulumi_id,
            data,
            data,
        )
        if not resp["Outputs"]:
            return None
        return self.make_output(resp["Outputs"], pulumi_id, self.UP.output_type)

    async def create(
        self, session: st.TaskSession, config: st.StateConfig
    ) -> st.Object:
        current = st.StateSnapshot({}, self.null_state.state)
        expected = await self.get_expected(current, config, session)
        return session["create"] << (
            st.task.new(self.create_task)(config.obj) >> expected
        )

    @property
    def create_task(self) -> Any:

        input_type = self.UP.input_type
        output_type = self.UP.output_type

        async def create_task(config: input_type) -> output_type:
            loop = asyncio.get_running_loop()

            check_resp, errs = await loop.run_in_executor(
                self.provider.thread_pool,
                self.provider.pulumi_provider.check,
                pylumi.URN(self.name),
                {},
                config,
            )
            check_resp = self.clean_check_resp(
                check_resp, self.UP.input_type, st.registry
            )
            if errs:
                raise PulumiValidationError(errs)
            resp = await loop.run_in_executor(
                self.provider.thread_pool,
                self.provider.pulumi_provider.create,
                pylumi.URN(self.name),
                check_resp,
                self.provider.operation_timeout,
            )
            return self.make_output(resp["Properties"], resp["ID"], output_type)

        return create_task

    async def modify(
        self,
        session: st.TaskSession,
        current: st.StateSnapshot,
        config: st.StateConfig,
    ) -> st.Object:
        expected = await self.get_expected(current, config, session)
        return session["modify"] << (
            st.task.new(self.modify_task)(current.obj, config.obj) >> expected
        )

    @property
    def modify_task(self) -> Any:

        input_type = self.UP.input_type
        output_type = self.UP.output_type

        async def modify_task(current: output_type, config: input_type) -> output_type:
            loop = asyncio.get_running_loop()
            current = current.copy()
            pulumi_id = current.pop(PULUMI_ID)

            check_resp, errs = await loop.run_in_executor(
                self.provider.thread_pool,
                self.provider.pulumi_provider.check,
                pylumi.URN(self.name, pulumi_id),
                current,
                config,
            )
            check_resp = self.clean_check_resp(
                check_resp, self.UP.input_type, st.registry
            )
            if errs:
                raise PulumiValidationError(errs)

            resp = await loop.run_in_executor(
                self.provider.thread_pool,
                self.provider.pulumi_provider.update,
                pylumi.URN(self.name, pulumi_id),
                pulumi_id,
                current,
                check_resp,
                self.provider.operation_timeout,
            )
            return self.make_output(resp["Properties"], resp["ID"], output_type)

        return modify_task

    async def delete(
        self, session: st.TaskSession, current: st.StateSnapshot
    ) -> st.Object:
        ref = session["delete"] << st.task.new(self.delete_task)(current.obj)
        return st.join(st.Object({}, st.EmptyType, session.ns.registry), ref)

    @property
    def delete_task(self) -> Any:

        null_type = self.DOWN.output_type
        output_type = self.UP.output_type

        async def delete_task(current: output_type) -> null_type:
            loop = asyncio.get_running_loop()
            current = current.copy()
            pulumi_id = current.pop(PULUMI_ID)

            resp = await loop.run_in_executor(
                self.provider.thread_pool,
                self.provider.pulumi_provider.delete,
                pylumi.URN(self.name, pulumi_id),
                pulumi_id,
                current,
                self.provider.operation_timeout,
            )
            return {}

        return delete_task

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name})"
