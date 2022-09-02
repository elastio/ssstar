#!/bin/bash
#
# Launches an EC2 instance for testing `ssstar`
set -euo pipefail

scripts_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
default_instance_type="m5.2xlarge"
instance_type="$default_instance_type"
key_name=""
instance_name="test instance for $(whoami)"
instance_market_options="--instance-market-options MarketType=spot"
security_group="launch-instance.sh-sg"

while getopts ":hk:i:n:g:o" opt; do
    case ${opt} in
        h )
            echo "Usage: $0 [options] [-k <key_name>] [-i <instance_type>] [-n <instance name>] [ -o ]"
            echo ""
            echo "Options:"
            echo "  -k <key_name> - Use a specified key name (Required)"
            echo "  -i <instance_type> - Use a specified instance type instead of the default $instance_type"
            echo "  -n <instance_name> - Give this instance a name to help you identify it in instance lists (prefix: $instance_name)"
            echo "  -g <security group> - Use this security group (default: $security_group)"
            echo "  -o - Create an on-demand instance instead of the default spot instance"
            exit 0
            ;;
        k )
            key_name=$OPTARG
            ;;

        i )
            instance_type=$OPTARG
            ;;

        n )
            instance_name="$instance_name:$OPTARG"
            ;;

        o)
            instance_market_options=""
            ;;

        \? )
            echo "Invalid option: $OPTARG" 1>&2
            exit 0
            ;;

        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            exit 0
            ;;
    esac
done

if [[ -z "$key_name" ]]; then
    echo "Error: -k <key_name> is required"
    exit -1
fi

ami=$(aws ssm get-parameters --names /aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2 --output json | jq ".Parameters[].Value" -r)
. $(dirname "${0}")/utils.sh

# Create the security group if it doesn't exist
if aws ec2 describe-security-groups --group-names "$security_group" 2>&1 > /dev/null; then
    echo "Security group $security_group already exists; no need to create"
else
    echo "Creating security group $security_group"

    security_group_id=$(aws ec2 create-security-group \
        --description "Auto-generated security group produced by ${0}" \
        --group-name "$security_group" \
        --vpc-id $(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --output json | jq -r  ".Vpcs[].VpcId") \
        | jq -r ".GroupId")

    aws ec2 authorize-security-group-ingress --group-id "$security_group_id" --protocol tcp --port 22 --cidr 0.0.0.0/0
fi

# 'envsubst` will expand placeholders in the YAML file with the values of the below env vars

# Build the cloud-init script including some env vars
user_data=$(envsubst < $scripts_dir/s0-bootstrap.yml)

echo "Launching \"$instance_name\" (instance type $instance_type) with AMI $ami"

instance_json=$(aws ec2 run-instances \
    --image-id "$ami" \
    --instance-type "$instance_type" \
    $instance_market_options \
    --key-name "$key_name" \
    --ebs-optimized \
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=40,VolumeType=gp3}" \
    --user-data "$user_data" \
    --security-groups "$security_group" \
    --tag-specifications \
    "ResourceType=instance,Tags=[{Key=Name,Value=$instance_name}]" \
    --output json)

instance_id=$(echo $instance_json | jq ".Instances[].InstanceId" -r)
echo "Launched EC2 instance id $instance_id"

echo "Querying instance info for public DNS..."
instance_info=$(aws ec2 describe-instances --instance-ids $instance_id --output json)
#echo $instance_info | jq "."
instance_dns=$(echo $instance_info | jq ".Reservations[].Instances[].PublicDnsName" -r)
echo "SSH into the instance with ssh ec2-user@$instance_dns using key $key_name"



# NEXT STEP
# Another script that copies the `elastio` source tree up to the spawned EC2 instance for convenient building
