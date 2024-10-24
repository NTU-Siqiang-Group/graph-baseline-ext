#!/bin/bash
prefix=~/AsterDB/dataset/
graphs=(freebase_large ldbc)
paths=(freebase_large.json2 ldbc.json2)

for i in 0 1
do
  graph_name=${graphs[$i]}
  datapath="${prefix}${paths[$i]}"
  echo "Loading $graph_name..."
  ./build/nebula/nebula_rw --data_path=$datapath --graph_name=$graph_name --command=load_property > log/nebula/load_property.log 2>&1
done