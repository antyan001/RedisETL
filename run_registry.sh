#!/bin/bash

resp="$(curl -i -f http://localhost:8003/register_replicas 2>/dev/null 1>./registry_status | grep -oP -- '"registry":\s?\K"[a-z]+"' registry_status)"
#resp="$(!curl -i -f -s http://localhost:8003/register_replicas | \
#        python3 -c $'import io,sys\nfor line in sys.stdin: res = line\nprint(res)' | xargs)"
status="$?"

if [[ "$resp" == *"success"* ]]; then
    echo "Successfull Replica's Registration"
    curl -i -f http://localhost:8003/stop_calling_registry
    exit $status
else
    echo "Got some failure in registration"
    echo $resp
    exit 1
fi

#if [ "$resp" -eq 0 ]; then
#    echo "Successfull Replica's Registration"
#    curl -i -f http://localhost:8002/stop_calling_registry
#    exit $status
#  else
#    echo "Got some failure in registration"
#    exit $status
#fi