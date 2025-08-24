#!/bin/bash

datasets=("wikitalk", "dblp")

for i in 0 1
do
  dataset="${datasets[$i]}"
  build/umbra/umbra_test --command=test --dataset=$dataset --read_ratio=0.5
done