#!/bin/bash
#
# Rsync a directory (by default the `ssstar` project in its entirety) to a remote host.
#
# It's assumed the remote host has SSH and rsync installed, and is an Amazon Linux 2 EC2 instance.
set -euo pipefail

scripts_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
src_dir="$(realpath $scripts_dir/../)"
dest_username="ec2-user"
dest_host=""

usage() {
    echo "Usage: $0 [-s <source_dir>] [-u <destination_username>] -d <destination_hostname>"
    echo "Default source dir: $src_dir"
    echo "Default destination username: $dest_username"
}

while getopts ":hs:u:d:" opt; do
    case ${opt} in
        h )
            usage
            exit 0
            ;;
        s )
            src_dir=$OPTARG
            ;;

        u )
            dest_username=$OPTARG
            ;;

        d )
            dest_host=$OPTARG
            ;;

        \? )
            echo "Invalid option: $OPTARG" 1>&2
            usage
            exit 1
            ;;

        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z $dest_host ]]; then
    echo "A destination hostname is required" 1>&2
    usage
    exit 1
fi

# By default, use the name of the source dir (without its entire path), and copy it into the
# home directory on the destination host
src_dir_path=$(realpath $src_dir)
src_dir_name=$(basename $src_dir_path)
dest_path="/home/$dest_username"

echo Copying $src_dir_path to $dest_path on $dest_host

# Use rsync to copy changed files, excluding anything that's ignored by `.gitignore`
rsync --info=progress2 -azzhe ssh \
    --filter=":- .gitignore" \
    $src_dir_path \
    $dest_username@$dest_host:$dest_path
