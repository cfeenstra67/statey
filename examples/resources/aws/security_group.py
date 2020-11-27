import asyncio
import contextlib
from typing import Any, Dict, Optional, Sequence

import aioboto3
import botocore

import statey as st
from statey.lib.aws import utils


SecurityGroupRuleType = st.Struct[
    # Optional args
    "cidr_blocks" : ~st.Array[str],
    "ipv6_cidr_blocks" : ~st.Array[str],
    "from_port" : Optional[int],
    "protocol" : st.String(default="tcp"),
    "to_port" : Optional[int],
]

SecurityGroupConfigType = st.Struct[
    "name":str,
    "description":str,
    # Optional args
    "ingress" : st.Array[SecurityGroupRuleType](default=()),
    "egress" : st.Array[SecurityGroupRuleType](default=()),
    "vpc_id" : Optional[str],
]

SecurityGroupType = st.Struct[
    "name":str,
    "description":str,
    "ingress" : st.Array[SecurityGroupRuleType],
    "egress" : st.Array[SecurityGroupRuleType],
    "vpc_id" : Optional[str],
    "id":str,
    "owner_id":str,
]


class SecurityGroupMachine(st.SimpleMachine):
    """
    Machine for a security group
    """

    UP = st.State("UP", SecurityGroupConfigType, SecurityGroupType)

    @contextlib.asynccontextmanager
    async def resource_ctx(self):
        async with aioboto3.resource("ec2") as ec2:
            yield ec2

    @contextlib.asynccontextmanager
    async def client_ctx(self):
        async with aioboto3.client("ec2") as client:
            yield client

    def get_diff(
        self,
        current: st.StateSnapshot,
        config: st.StateConfig,
        session: st.TaskSession,
    ) -> st.Diff:

        differ = session.ns.registry.get_differ(config.state.input_type)
        diffconfig = differ.config()

        def compare_unordered(arr1, arr2):
            if arr1 is None or arr2 is None:
                return arr1 == arr2
            return all(el1 in arr2 for el1 in arr1) and all(el2 in arr1 for el2 in arr2)

        diffconfig.set_comparison("ingress", compare_unordered)
        diffconfig.set_comparison("egress", compare_unordered)

        current_as_config = st.filter_struct(current.obj, config.type)
        out_diff = differ.diff(current_as_config, config.obj, session, diffconfig)
        return out_diff

    def get_action(self, diff: st.Diff) -> st.ModificationAction:
        if not diff:
            return st.ModificationAction.NONE
        if "name" in diff or "vpc_id" in diff or "description" in diff:
            return st.ModificationAction.DELETE_AND_RECREATE
        return st.ModificationAction.MODIFY

    async def convert_instance(self, instance: "SecurityGroup") -> Dict[str, Any]:
        out = {"id": instance.id}
        (
            out["name"],
            out["description"],
            ingress_perms,
            egress_perms,
            out["vpc_id"],
            out["owner_id"],
        ) = await asyncio.gather(
            instance.group_name,
            instance.description,
            instance.ip_permissions,
            instance.ip_permissions_egress,
            instance.vpc_id,
            instance.owner_id,
        )
        out["ingress"] = list(map(self.convert_rule, ingress_perms))
        out["egress"] = list(map(self.convert_rule, egress_perms))
        return out

    @staticmethod
    def convert_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "cidr_blocks": [rang["CidrIp"] for rang in rule["IpRanges"]],
            "ipv6_cidr_blocks": [rang["CidrIpv6"] for rang in rule["Ipv6Ranges"]],
            "from_port": rule.get("FromPort"),
            "to_port": rule.get("ToPort"),
            "protocol": rule["IpProtocol"],
        }

    async def refresh_state(self, data: Any) -> Optional[Any]:
        async with self.resource_ctx() as ec2:
            sg = await ec2.SecurityGroup(data["id"])
            try:
                await sg.load()
            except botocore.exceptions.ClientError:
                return None
            return await self.convert_instance(sg)

    async def refresh_config(self, config: st.Object) -> st.Object:
        async with self.client_ctx() as client:
            default_vpc = await utils.get_default_vpc(client)
            return st.struct_replace(
                config, vpc_id=st.ifnull(config.vpc_id, default_vpc["VpcId"])
            )

    async def create_task(self, config: SecurityGroupConfigType) -> SecurityGroupType:
        """
        Create a security group resource
        """
        async with self.resource_ctx() as ec2:
            kws = {"Description": config["description"], "GroupName": config["name"]}
            if config["vpc_id"] is not None:
                kws["VpcId"] = config["vpc_id"]
            group = await ec2.create_security_group(**kws)
            current = await self.convert_instance(group)
            yield current
            yield await self.update_rules(current, config)

    async def modify_task(
        self, diff: st.Diff, current: SecurityGroupType, config: SecurityGroupConfigType
    ) -> SecurityGroupType:
        """
        Modify the security group
        """
        async with self.resource_ctx() as ec2:
            sg = await ec2.SecurityGroup(current["id"])
            if "ingress" in diff or "egress" in diff:
                await self.update_rules(current, config)
                await sg.load()
                yield await self.convert_instance(sg)

    async def update_rules(
        self, current: SecurityGroupType, config: SecurityGroupConfigType
    ) -> SecurityGroupType:
        """
        Update security group rules
        """
        ingress_to_add = []
        ingress_to_remove = []
        egress_to_add = []
        egress_to_remove = []

        for name, to_add, to_remove, same in [
            ("ingress", ingress_to_add, ingress_to_remove, []),
            ("egress", egress_to_add, egress_to_remove, []),
        ]:

            for rule in config[name]:
                if rule in current[name]:
                    same.append(rule)
                else:
                    to_add.append(rule)

            for rule in current[name]:
                if rule not in same:
                    to_remove.append(rule)

        async with self.resource_ctx() as ec2:
            coros = []
            instance = await ec2.SecurityGroup(current["id"])

            def rule_params(rule):
                kws = {
                    "IpRanges": [
                        {"CidrIp": block} for block in (rule["cidr_blocks"] or [])
                    ],
                    "Ipv6Ranges": [
                        {"CidrIpv6": block}
                        for block in (rule["ipv6_cidr_blocks"] or [])
                    ],
                    "IpProtocol": rule["protocol"],
                }
                if rule["from_port"] is not None:
                    kws["FromPort"] = rule["from_port"]
                if rule["to_port"] is not None:
                    kws["ToPort"] = rule["to_port"]

                return {"IpPermissions": [kws]}

            for rule in ingress_to_add:
                coros.append(instance.authorize_ingress(**rule_params(rule)))

            for rule in ingress_to_remove:
                coros.append(instance.revoke_ingress(**rule_params(rule)))

            for rule in egress_to_add:
                coros.append(instance.authorize_egress(**rule_params(rule)))

            for rule in egress_to_remove:
                coros.append(instance.revoke_egress(**rule_params(rule)))

            await asyncio.gather(*coros)

        out = current.copy()
        out["ingress"] = config["ingress"]
        out["egress"] = config["egress"]

        return out

    async def delete_task(self, current: SecurityGroupType) -> st.EmptyType:
        """
        Delete a security group
        """
        async with self.resource_ctx() as ec2:
            group = await ec2.SecurityGroup(current["id"])
            await group.delete()
            return {}


security_group_resource = st.MachineResource("aws_security_group", SecurityGroupMachine)

# Resource State Factory
SecurityGroup = security_group_resource.s


RESOURCES = [security_group_resource]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register resources in this module
    """
    if registry is None:
        registry = st.registry

    for resource in RESOURCES:
        registry.register(resource)
