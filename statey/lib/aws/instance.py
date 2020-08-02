import asyncio
from typing import Dict, Any, Optional

import botocore

import statey as st
from statey.lib.aws.base import AWSMachine


InstanceConfigType = st.Struct[
    # Required args
    "ami" : st.String,
    "instance_type" : st.String,
    "key_name" : st.String,
    # Optional args
    "vpc_security_group_ids": ~st.Array[st.String],
    "subnet_id": ~st.String,
    # "availability_zone": ~st.String,
    # "placement_group": ~st.String,
    # "tenancy": ~st.String,
    # "host_id": ~st.String,
    # "cpu_core_count": ~st.Integer,
    # "cpu_threads_per_core": st.Integer(default=2),
    ## Seems like there is some complexity here
    # "ebs_optimized": ~st.Integer,
    # "disable_api_termination": st.Boolean(default=False),
    # "get_password_data": st.Boolean(default=False),
    ## This one seems complex too
    # "monitoring": st.Boolean(default=False),
]


InstanceType = st.Struct[
    "ami" : str,
    "instance_type" : str,
    "key_name" : str,
    # Exported
    "id" : str,
    "ebs_optimized" : bool,
    "placement_group" : str,
    "public_ip" : Optional[str],
    "public_dns" : str,
    "private_ip" : str,
    "private_dns" : str,
    "vpc_security_group_ids": st.Array[st.String],
    "subnet_id": st.String,
]


class InstanceMachine(st.SimpleMachine, AWSMachine):
    """
    Machine for an EC2 Instance
    """
    service: str = "ec2"

    UP = st.State("UP", InstanceConfigType, InstanceType)

    @staticmethod
    async def convert_instance(instance: "Instance") -> Dict[str, Any]:
        out = {"id": instance.id}
        (
            out["ami"],
            out["ebs_optimized"],
            out["instance_type"],
            placement,
            out["private_ip"],
            out["private_dns"],
            out["public_ip"],
            out["public_dns"],
            out["key_name"],
            security_groups,
            out['subnet_id']
        ) = await asyncio.gather(
            instance.image_id,
            instance.ebs_optimized,
            instance.instance_type,
            instance.placement,
            instance.private_ip_address,
            instance.private_dns_name,
            instance.public_ip_address,
            instance.public_dns_name,
            instance.key_name,
            instance.security_groups,
            instance.subnet_id
        )
        out["placement_group"] = placement["GroupName"]
        out['vpc_security_group_ids'] = [group['GroupId'] for group in security_groups]
        return out

    async def refresh_state(self, data: Any) -> Optional[Any]:
        async with self.resource_ctx() as ec2:
            instance = await ec2.Instance(data["id"])
            try:
                await instance.load()
            except botocore.exceptions.ClientError:
                return None
            return await self.convert_instance(instance)

    def get_diff(
        self,
        current: st.StateSnapshot,
        config: st.StateConfig,
        session: st.TaskSession,
    ) -> st.Diff:

        differ = session.ns.registry.get_differ(config.state.input_type)
        diffconfig = differ.config()

        def ignore_if_new_none(old, new):
            if new is None:
                return True
            # Pass it along to the element-level logic
            return NotImplemented

        diffconfig.set_comparison('subnet_id', ignore_if_new_none)
        diffconfig.set_comparison('vpc_security_group_ids', ignore_if_new_none)

        current_as_config = st.filter_struct(current.obj, config.type)
        out_diff = differ.diff(current_as_config, config.obj, session, diffconfig)
        return out_diff

    async def get_expected(self, config: st.StateConfig) -> Any:
        replaced = st.replace(
            config.obj, False,
            subnet_id=st.ifnull(config.obj.subnet_id, st.Unknown[st.String]),
            vpc_security_group_ids=st.ifnull(
                config.obj.vpc_security_group_ids,
                st.Unknown[st.Array[st.String]]
            )
        )
        return st.fill_unknowns(replaced, config.state.output_type)

    async def create_task(self, config: InstanceConfigType) -> InstanceType:
        """
        Create a new EC2 Instance
        """
        async with self.resource_ctx() as ec2:
            kws = {
                'ImageId': config['ami'],
                'InstanceType': config['instance_type'],
                'KeyName': config['key_name'],
                'MinCount': 1,
                'MaxCount': 1
            }
            if config['vpc_security_group_ids'] is not None:
                kws['SecurityGroupIds'] = config['vpc_security_group_ids']
            if config['subnet_id'] is not None:
                kws['SubnetId'] = config['subnet_id']

            (instance,) = await ec2.create_instances(**kws)
            # Checkpoint after creation
            yield await self.convert_instance(instance)
            await instance.wait_until_running()
            await instance.load()
            yield await self.convert_instance(instance)

    async def delete_task(self, current: InstanceType) -> st.EmptyType:
        """
        Delete the EC2 Instance
        """
        async with self.resource_ctx() as ec2:
            instance = await ec2.Instance(current["id"])
            await instance.terminate()
            yield {}
            await instance.wait_until_terminated()


instance_resource = st.MachineResource("aws_instance", InstanceMachine)

# Resource state factory
Instance = instance_resource.s


RESOURCES = [instance_resource]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register resources in this module
    """
    if registry is None:
        registry = st.registry

    for resource in RESOURCES:
        registry.register_resource(resource)
