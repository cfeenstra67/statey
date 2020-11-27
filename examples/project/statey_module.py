import json
import os
import textwrap as tw

import statey as st
from statey.ext.pulumi.providers import aws


@st.declarative
def module(session):
    """
    This defines the resource configuration for this module.
    """
    bucket1 = aws.s3.Bucket(
        bucket='clf-misc-3'
    )
    object1 = aws.s3.BucketObject(
        bucket=bucket1.bucket,
        key='pulumi-test-1',
        content='blah blah blah'
    )
    object2 = aws.s3.BucketObject(
        bucket=bucket1.bucket,
        key='pulumi-test-2',
        content=st.ifnull(object1.content, "") + '\n(again)\n'
    )
    security_group1 = aws.ec2.SecurityGroup(
        name='sg_test_1',
        description='My description',
        ingress=[
            {
                'cidrBlocks': ['0.0.0.0/0'],
                'ipv6CidrBlocks': [],
                'fromPort': 22,
                'toPort': 22,
                'protocol': 'tcp'
            },
            {
                'cidrBlocks': ['0.0.0.0/0'],
                'ipv6CidrBlocks': [],
                'fromPort': 80,
                'toPort': 80,
                'protocol': 'tcp'
            }
        ],
        egress=[
            {
                'cidrBlocks': ['0.0.0.0/0'],
                'ipv6CidrBlocks': [],
                'protocol': '-1',
                'fromPort': 0,
                'toPort': 0
            }
        ]
    )
    instance1 = aws.ec2.Instance(
        instanceType='t2.micro',
        ami='ami-0d9a6b6c74aafb9d7',
        keyName='sylph',
        vpcSecurityGroupIds=[security_group1.id]
    )
    object3 = aws.s3.BucketObject(
        bucket=bucket1.bucket,
        key=instance1.id,
        source='schema.json',
        contentType='application/json'
    )
