#!/usr/bin/env bash

set -euo pipefail

: ${PROFEFE_COLLECTOR:="http://localhost:10100"}

usuage() {
    echo "$0 --service <service> --type (cpu|heap) [--label foo=bar] -- <file...>"
}

service=
labels=
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

api_profile_url="$PROFEFE_COLLECTOR/api/0/profiles"

create_profile() {
    local file_path="$1"
    echo -n "uploading ${file_path}..."
    curl -s -XPOST "${api_profile_url}?service=${service}&type=${prof_type}&labels=${labels}" --data-binary "@${file_path}" >/dev/null
    echo "OK"
}

for prof_file in "$@";
do
    if [ ! -r "$prof_file" ]; then
        echo 1>&2 "$0: can't read prof file $prof_file"
        exit 1
    fi
    create_profile "$prof_file"
done
