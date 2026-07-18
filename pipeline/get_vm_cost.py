"""
pipeline/get_vm_cost.py
-----------------------
Fetches on-demand EC2 instance pricing from AWS Price List API.
Returns monthly cost estimate (730 hours/month).

Usage:
    python -m pipeline.get_vm_cost --instance r6i.xlarge --region us-east-1
    python -m pipeline.get_vm_cost --instance m5.8xlarge --region us-east-1

Requires: boto3, AWS credentials configured (read-only pricing access)
Note: AWS Pricing API is only available in us-east-1.
"""

import argparse
import json

import boto3

REGION_NAMES = {
    "us-east-1":      "US East (N. Virginia)",
    "us-east-2":      "US East (Ohio)",
    "us-west-1":      "US West (N. California)",
    "us-west-2":      "US West (Oregon)",
    "eu-west-1":      "Europe (Ireland)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
}

HOURS_PER_MONTH = 730


def get_ec2_price(instance_type: str, region: str = "us-east-1") -> dict:
    """
    Returns:
        {
            instance_type: str,
            region: str,
            price_per_hour_usd: float,
            price_per_month_usd: float,
            vcpu: str,
            memory: str,
            as_of: str,
        }
    """
    client = boto3.client("pricing", region_name="us-east-1")

    location = REGION_NAMES.get(region)
    if not location:
        raise ValueError(f"Unknown region: {region}. Supported: {list(REGION_NAMES)}")

    response = client.get_products(
        ServiceCode="AmazonEC2",
        Filters=[
            {"Type": "TERM_MATCH", "Field": "instanceType",      "Value": instance_type},
            {"Type": "TERM_MATCH", "Field": "operatingSystem",   "Value": "Linux"},
            {"Type": "TERM_MATCH", "Field": "tenancy",           "Value": "Shared"},
            {"Type": "TERM_MATCH", "Field": "preInstalledSw",    "Value": "NA"},
            {"Type": "TERM_MATCH", "Field": "capacitystatus",    "Value": "Used"},
            {"Type": "TERM_MATCH", "Field": "location",          "Value": location},
        ],
        FormatVersion="aws_v1",
    )

    products = response.get("PriceList", [])
    if not products:
        raise ValueError(f"No pricing found for {instance_type} in {region}")

    product  = json.loads(products[0])
    attrs    = product["product"]["attributes"]
    terms    = product["terms"]["OnDemand"]
    price_dim = next(iter(next(iter(terms.values()))["priceDimensions"].values()))
    price_per_hour = float(price_dim["pricePerUnit"]["USD"])

    return {
        "instance_type":        instance_type,
        "region":               region,
        "price_per_hour_usd":   price_per_hour,
        "price_per_month_usd":  round(price_per_hour * HOURS_PER_MONTH, 2),
        "vcpu":                 attrs.get("vcpu", "?"),
        "memory":               attrs.get("memory", "?"),
        "as_of":                price_dim.get("effectiveDateStart", "?"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance", required=True, help="EC2 instance type e.g. r6i.xlarge")
    parser.add_argument("--region",   default="us-east-1")
    args = parser.parse_args()

    result = get_ec2_price(args.instance, args.region)

    print(f"\nEC2 Pricing: {result['instance_type']} in {result['region']}")
    print(f"  vCPU:            {result['vcpu']}")
    print(f"  Memory:          {result['memory']}")
    print(f"  Price/hour:      ${result['price_per_hour_usd']:.4f}")
    print(f"  Price/month:     ${result['price_per_month_usd']:.2f} ({HOURS_PER_MONTH}h)")
    print(f"  Pricing date:    {result['as_of']}")


if __name__ == "__main__":
    main()
