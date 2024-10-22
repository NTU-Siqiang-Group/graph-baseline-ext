#!/bin/bash

datasets=("wikitalk")

for i in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9
do
  dataset=${datasets[0]}
  build/umbra/umbra_test --command=test --dataset=$dataset --read_ratio=$i
done