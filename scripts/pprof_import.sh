#!/usr/bin/env bash

set -euo pipefail

: ${PROFEFE_COLLECTOR:="http://localhost:10100"}

usuage() {
    echo "$0 --service <service> --type (cpu|heap) [--label foo=bar] -- <file...>"
}

service=
labels=
prof_id="imported-profile"
prof_type=

while true
do
    case "$1" in
        --service)
            service="$2"
            shift 2
            ;;
        --type)
            prof_type="$2"
            shift 2
            ;;
        --label)
            if [ -n "$labels" ]; then
                labels="${labels},"
            fi
            labels="${labels}${2}"
            shift 2
            ;;
        -h|--help)
            usuage
            exit 1
            ;;
        --)
            shift
            break
            ;;
        -*)
            echo 1>&2 "$0: bad option $1"
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

if [ -z "$prof_type" ] || [ -z "$service" ]; then
    echo 1>&2 "$0: service and profile type must be set"
    usuage
    exit 1
fi

if [ $# -eq 0 ]; then
    echo 1>&2 "$0: no prof files to upload"
    exit 1
fi

api_profile_url="$PROFEFE_COLLECTOR/api/0/profile"

create_profile() {
    curl -s -XPUT "$api_profile_url?id=$prof_id&service=$service&labels=$labels" | jq -r ".token"
}

update_profile() {
    local token="$1"
    local file_path="$2"
    echo -n "uploading ${file_path}..."
    curl -s -XPOST "$api_profile_url?id=$prof_id&token=$token&type=$prof_type" --data-binary "@$file_path" >/dev/null
    echo "OK"
}

token=$(create_profile)
if [ -z "$token" ]; then
    echo 1>&2 "$0: unable to receive profile token"
    exit 3
fi

for prof_file in $*;
do
    if [ ! -r "$prof_file" ]; then
        echo 1>&2 "$0: can't read prof file $prof_file"
        exit 1
    fi
    update_profile "$token" "$prof_file"
done
