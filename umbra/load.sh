#!/bin/bash
# docker run -v umbra-db:/var/db -p 5432:5432 --ulimit nofile=1048576:1048576 --ulimit memlock=8388608:8388608 umbradb/umbra:latest
datasets=("wikitalk")
vcounts=(2394385)
paths=("/home/junfeng/wiki-Talk/wiki-Talk.e")

for i in 0
do
  dataset=${datasets[$i]}
  vcount=${vcounts[$i]}
  path=${paths[$i]}
  build/umbra/umbra_test --command=load --dataset=$dataset --vertex_count=$vcount --path=$path
done