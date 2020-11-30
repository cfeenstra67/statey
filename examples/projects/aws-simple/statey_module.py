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
        key='statey-test-1',
        content='blah blah blah'
    )
    object2 = aws.s3.BucketObject(
        bucket=bucket1.bucket,
        key='statey-test-2',
        content=st.ifnull(object1.content, "") + '\n(again)\n'
    )
    object3 = aws.s3.BucketObject(
        bucket=bucket1.bucket,
        key='statey-test-3',
        source='statey_module.py'
    )
