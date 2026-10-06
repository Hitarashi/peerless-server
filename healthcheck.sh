#!/bin/bash

set -o pipefail

host=127.0.0.1
port="${STREAM_SERVER_PORT:-4444}"
path=/api/v1/health

exec 3<>"/dev/tcp/${host}/${port}" || exit 1

printf 'GET %s HTTP/1.1\r\nhost: %s\r\nConnection: close\r\n\r\n' "$path" "$host" >&3

IFS= read -r -t 5 status_line <&3
exec 3<&-

printf '%s\n' "$status_line"
[[ $status_line == *" 200 "* ]]
