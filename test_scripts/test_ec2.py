import json
import textwrap as tw

import statey as st

from statey.lib import sos, aws


@st.function
def encode_instance(instance: aws.InstanceType) -> str:
    return json.dumps(instance, indent=2, sort_keys=True)


@st.function
def render_inventory(public_dns: str) -> str:
    """

    """
    return tw.dedent(f'''
    groups:
    - name: hosts
      targets:
        - uri: {public_dns}
          name: target
      config:
        transport: ssh
        ssh:
          run-as: root
          run-as-command:
            - sudo
            - -u
          user: ec2-user
          private-key: ~/.ssh/sylph.pem
          host-key-check: false
    ''').strip()


@st.declarative
def resources(session):
    # my_vpc = aws.Vpc(
    #   cidr_block='10.0.0.0/16',
    #   instance_tenancy='default',
    #   assign_generated_ipv6_cidr_block=False
    # )
    my_sg = aws.SecurityGroup(
        name='sg_test_1',
        description='My Description',
        ingress=[
            {
                'cidr_blocks': ['0.0.0.0/0'],
                'ipv6_cidr_blocks': [],
                'from_port': 22,
                'to_port': 22
            },
            {
                'cidr_blocks': ['0.0.0.0/0'],
                'ipv6_cidr_blocks': [],
                'from_port': 80,
                'to_port': 80
            }
        ],
        egress=[
            {
                'cidr_blocks': ['0.0.0.0/0'],
                'ipv6_cidr_blocks': [],
                'protocol': '-1'
            }
        ]
        # vpc_id=my_vpc.id
    )
    my_instance = aws.Instance(
        instance_type='t2.micro',
        ami='ami-0d9a6b6c74aafb9d7',
        key_name='sylph',
        vpc_security_group_ids=[my_sg.id],
        tags={'source': 'statey'},
        availability_zone='us-east-2b',
        # subnet_id=my_subnet.id
    )
    bolt_inventory = sos.File(
        location='./bolt_project/inventory.yaml',
        data=render_inventory(my_instance.public_dns)
    )


def session():
    session = st.create_resource_session()
    resources(session)
    return session
