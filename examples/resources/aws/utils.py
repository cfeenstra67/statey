from typing import Any, Optional, Dict


async def get_default_vpc(ec2_client: "Client") -> Optional[Dict[str, Any]]:
    """
    Get the default VPC in the current region
    """
    vpcs_resp = await ec2_client.describe_vpcs(
        Filters=[{"Name": "isDefault", "Values": ["true"]}]
    )
    vpcs = vpcs_resp["Vpcs"]
    return vpcs[0] if vpcs else None
