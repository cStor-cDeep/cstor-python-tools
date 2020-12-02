#!/bin/bash

echo "ext_process V1.0.0"
echo "Arguments are: $@"

sleep 1

echo "!~Ready"

while read req_id file; do
    test -z "$req_id" && break

    sleep 1

    echo "!~Error $req_id TESTING MODE"
done

echo "!~Bye"