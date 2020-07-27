import asyncio
import contextlib
from typing import Any, Dict, Optional

import aioboto3
import botocore

import statey as st


SecurityGroupRuleType = st.S.Struct[
    "cidr_blocks" : ~st.S.Array[st.S.String],
    "ipv6_cidr_blocks" : ~st.S.Array[st.S.String],
    "from_port" : ~st.S.Integer,
    "protocol" : st.S.String,
    "to_port" : ~st.S.Integer,
].t

SecurityGroupConfigType = st.S.Struct[
    "name" : st.S.String,
    "description" : st.S.String,
    "ingress" : ~st.S.Array[SecurityGroupRuleType],
    "egress" : ~st.S.Array[SecurityGroupRuleType],
    "vpc_id" : ~st.S.String,
].t

SecurityGroupType = st.S.Struct[
    "name" : st.S.String,
    "description" : st.S.String,
    "ingress" : st.S.Array[SecurityGroupRuleType],
    "egress" : st.S.Array[SecurityGroupRuleType],
    "vpc_id" : ~st.S.String,
    "id" : st.S.String,
    "owner_id" : st.S.String,
].t


class SecurityGroupMachine(st.SingleStateMachine):
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

    @staticmethod
    async def get_expected(config: st.StateConfig) -> Dict[str, Any]:
        return st.fill_unknowns(config.obj, SecurityGroupType)

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
            vpcs_resp = await client.describe_vpcs(
                Filters=[{"Name": "isDefault", "Values": ["true"]}]
            )
            vpcs = vpcs_resp["Vpcs"]
            default_vpc = vpcs[0]["VpcId"] if vpcs else None

            @st.function
            def get_vpc(vpc: Optional[str]) -> Optional[str]:
                return vpc if vpc is not None else default_vpc

            return st.replace(config, vpc_id=get_vpc(config.vpc_id))

    @st.task.new
    async def create_sg(self, config: SecurityGroupConfigType) -> SecurityGroupType:
        """
        Create a security group resource
        """
        async with self.resource_ctx() as ec2:
            kws = {"Description": config["description"], "GroupName": config["name"]}
            if config["vpc_id"] is not None:
                kws["VpcId"] = config["vpc_id"]
            group = await ec2.create_security_group(**kws)
            return await self.convert_instance(group)

    @st.task.new
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

    @st.task.new
    async def delete_sg(self, current: SecurityGroupType) -> st.EmptyType:
        """
        Delete a security group
        """
        async with self.resource_ctx() as ec2:
            group = await ec2.SecurityGroup(current["id"])
            await group.delete()
            return {}

    async def create(
        self, session: st.TaskSession, config: st.StateConfig
    ) -> st.Object:
        expected = await self.get_expected(config)
        without_rules = session["create_security_group"] << self.create_sg(config.obj)
        without_rules_cp = session[
            "create_security_group_checkpoint"
        ] << self.UP.snapshot(without_rules)
        return session["update_rules"] << (
            self.update_rules(without_rules_cp, config.obj) >> expected
        )

    async def delete(
        self, session: st.TaskSession, current: st.StateSnapshot
    ) -> st.Object:
        ref = session["delete_security_group"] << self.delete_sg(current.obj)
        return st.join(st.Object({}, st.EmptyType, session.ns.registry), ref)

    async def modify(
        self, session: st.TaskSession, current: st.StateSnapshot, config: st.StateConfig
    ) -> st.Object:
        expected = await self.get_expected(config)
        ref = await self.delete(session, current)
        snapshotted = session["post_delete_snapshot"] << self.DOWN.snapshot(ref)
        joined_config = config.clone(obj=st.join(config.obj, snapshotted))
        return await self.create(session, joined_config) >> expected


security_group_resource = st.MachineResource("security_group", SecurityGroupMachine)

# Resource State Factory
SecurityGroup = security_group_resource.s


RESOURCES = [security_group_resource]


def register() -> None:
    """
    Register resources in this module
    """
    for resource in RESOURCES:
        st.registry.register_resource(resource)
