import asyncio
import contextlib
from typing import Any, Dict, Optional

import aioboto3

import statey as st


IngressRuleSchema = EgressRuleSchema = st.S.Struct[
    "cidr_blocks": ~st.S.Array[st.S.String],
    "ipv6_cidr_blocks": ~st.S.Array[st.S.String],
    "from_port": st.S.Integer,
    "protocol": st.S.String,
    "to_port": st.S.Integer
].s

IngressRuleType = IngressRuleSchema.output_type

EgressRuleType = EgressRuleSchema.output_type

SecurityGroupConfigType = st.S.Struct[
    "name": ~st.S.String(default=None),
    "description": ~st.S.String(default=None),
    "ingress": ~st.S.Array[IngressRuleSchema](default=None),
    "egress": ~st.S.Array[EgressRuleSchema](default=None),
    "vpc_id": ~st.S.Integer(default=None),
    "tags": ~st.S.Array[st.S.String](default=None)
].t

SecurityGroupType = st.S.Struct[
    "name": st.S.String,
    "description": st.S.String,
    "ingress": st.S.Array[IngressRuleSchema],
    "egress": st.S.Array[EgressRuleSchema],
    "vpc_id": st.S.Integer,
    "tags": st.S.Array[st.S.String],
    "id": st.S.String,
    "owner_id": st.S.String,
].t


class SecurityGroupMachine(st.SingleStateMachine):
    """
    Machine for a security group
    """
    UP = st.State("UP", SecurityGroupConfigType, SecurityGroupType)

    @contextlib.asynccontextmanager
    async def resource_ctx(self):
        async with aioboto3.resource('ec2') as ec2:
            yield ec2

    @staticmethod
    def get_expected(config: st.StateSnapshot) -> Dict[str, Any]:
        unknown = st.Object(st.Unknown(return_type=SecurityGroupType))
        return {
            'name': config.data['name'],
            'description': config.data['description'],
            'ingress': config.data['ingress'],
            'egress': config.data['egress'],
            'vpc_id': config.data['vpc_id'],
            'tags': config.data['tags'],
            'id': unknown.id,
            'arn': unknown.arn,
            'owner_id': unknown.owner_id
        }

    async def convert_instance(self, instance: "SecurityGroup") -> Dict[str, Any]:
        out = {'id': instance.id}
        (
            out['name'],
            out['description'],
            out['ingress'],
            ingress_perms,
            egress_perms,
            out['vpc_id'],
            out['tags'],
            out['owner_id']
        ) = await asyncio.gather(
            out.group_name,
            out.description,
            out.ip_permissions,
            out.ip_permissions_egress,
            out.vpc_id,
            out.tags,
            out.owner_id
        )
        out['ingress'] = list(map(self.convert_rule, ingress_perms))
        out['egress'] = list(map(self.convert_rule, egress_perms))
        return out

    @staticmethod
    def convert_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'cidr_blocks': [rang['CidrIp'] for rang in rule['IpRanges']],
            'ipv6_cidr_blocks': [rang['CidrIpv6'] for rang in rule['Ipv6Ranges']],
            'from_port': rule['FromPort'],
            'to_port': rule['ToPort'],
            'protocol': rule['IpProtocol']
        }

    async def refresh_state(self, data: Any) -> Optional[Any]:
        async with self.resource_ctx() as ec2:
            sg = await ec2.SecurityGroup(data['id'])
            try:
                await instance.load()
            except botocore.exceptions.ClientError:
                return None
            return await self.convert_instance(instance)

    @st.task.new
    async def create_sg(self, config: SecurityGroupConfigType) -> SecurityGroupType:
        """
        Create a security group resource
        """
        async with self.resource_ctx() as ec2:
            group = await ec2.create_security_group(
                Description=config['description'],
                GroupName=config['name'],
                VpcId=config['vpc_id'],
                TagSpecifications=[
                    {
                        'ResourceType': 'security-group',
                        'Tags': config['tags']
                    }
                ]
            )
            return await self.convert_instance(group)

    @st.task.new
    async def update_rules(self, current: SecurityGroupType, config: SecurityGroupConfigType) -> SecurityGroupType:
        """
        Update security group rules
        """
        ingress_to_add = []
        ingress_to_remove = []
        egress_to_add = []
        egress_to_remove = []

        for name, to_add, to_remove, same in [
            ('ingress', ingress_to_add, ingress_to_remove, []),
            ('egress', egress_to_add, egress_to_remove, [])
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
            instance = await ec2.SecurityGroup(current['id'])

            def rule_params(rule):
                return {
                    'GroupName': rule['name'],
                    'IpPermissions': [
                        {
                            'FromPort': rule['from_port'],
                            'ToPort': rule['to_port'],
                            'IpRanges': [
                                {
                                    'CidrIp': block
                                } for block in (rule['cidr_blocks'] or [])
                            ],
                            'Ipv6Ranges': [
                                {
                                    'CidrIpv6': block
                                } for block in (rule['ipv6_cidr_blocks'] or [])
                            ],
                            'IpProtocol': rule['protocol']
                        }
                    ]
                }

            for rule in ingress_to_add:
                coros.append(instance.authorize_ingress(**rule_params(rule)))

            for rule in ingress_to_remove:
                coros.append(instance.revoke_ingress(**rule_params(rule)))

            for rule in egress_to_add:
                coros.append(instance.authorize_egress(**rule_params(rule)))

            for rule in egress_to_remove:
                coros.append(instance.revoke_egress(**rule_params(rule)))

            await asyncio.wait(coros)

        out = current.copy()
        out['ingress'] = config['ingress']
        out['egress'] = config['egress']

        return out

    @st.task.new
    async def delete_sg(self, current: SecurityGroupType) -> st.EmptyType:
        """
        Delete a security group
        """
        async with self.resource_ctx() as ec2:
            group = await ec2.SecurityGroup(current['id'])
            await group.delete()
            return {}

    def create(self, session: st.TaskSession, config: st.StateConfig) -> st.Object:
        expected = self.get_expected(config)
        without_rules = session['create_security_group'] << self.create_sg(config.ref)
        without_rules_cp = session['create_security_group_checkpoint'] << self.UP.snapshot(without_rules)
        return session['update_rules'] << (self.update_rules(without_rules_cp, config.ref) >> expected)

    def delete(self, session: st.TaskSession, current: st.StateSnapshot) -> st.Object:
        ref = session['delete_security_group'] << self.delete_sg(current.obj)
        return st.join(st.Object({}), ref)

    def modify(self, session: st.TaskSession, current: st.StateSnapshot, config: st.StateConfig) -> st.Object:
        ref = self.delete(session, current)
        snapshotted = session['post_delete_snapshot'] << self.DOWN.snapshot(ref)
        joined_config = config.clone(ref=st.join(config.ref, snapshotted))
        return self.create(session, joined_config)


security_group_resource = st.MachineResource("security_group", SecurityGroupMachine)

# Resource State Factory
SecurityGroup = security_group_resource.s


RESOURCES = [
    security_group_resource
]


def register() -> None:
    """
    Register resources in this module
    """
    for resource in RESOURCES:
        st.registry.register_resource(resource)
