#!/bin/bash
# Liveness probe for the streaming server.
#
# The runtime image is debian:bookworm-slim: no curl, no wget. The obvious
# replacement -- a raw TCP connect through bash's /dev/tcp -- is what this does,
# with a real HTTP request so we validate the response code rather than just the
# socket. Deliberately written with bash builtins only (no grep, no timeout), so
# it works on a base image with essentially nothing installed.
#
# Note /dev/tcp is a bash feature: /bin/sh here is dash, so this must run under
# bash rather than being invoked as a plain shell script.

set -o pipefail

host=127.0.0.1
port="${STREAM_SERVER_PORT:-4444}"
path=/api/v1/health

exec 3<>"/dev/tcp/${host}/${port}" || exit 1

printf 'GET %s HTTP/1.1\r\nhost: %s\r\nConnection: close\r\n\r\n' "$path" "$host" >&3

# Read the status line only; the body is irrelevant to a health check.
IFS= read -r -t 5 status_line <&3
exec 3<&-

printf '%s\n' "$status_line"
[[ $status_line == *" 200 "* ]]
