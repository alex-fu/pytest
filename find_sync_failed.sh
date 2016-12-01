#!/bin/bash

if [ -z $1 ]; then
    echo "Usage: $0 <error num>"
    exit
fi

sed -n 's/.*Retrieve HFQ day kline for \(.*\) from 201.-01-01 to 201.-12-31.*/\1/p' log | sort | uniq > log1
sed -n 's/.*Store .* klines for \(.*\) from 201.-01-01 to 201.-12-31.*/\1/p' log | sort | uniq > log2
diff -u log1 log2 | grep '^-' | head -n $1 | tail -n +2 | cut -d'-' -f2
