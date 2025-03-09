#!/bin/bash

cd src/tools/scripts

python3 ./generate_graph.py --dataset citpatents --binary --in-memory
# python3 ./generate_graph.py --dataset citpatents --binary --in-memory --weighted

python3 ./rebalance_input.py --dataset citpatents
# python3 ./rebalance_input.py --dataset citpatents-weighted

python3 ./run_tiles_indexer.py --dataset citpatents --debug
# python3 ./run_tiles_indexer.py --dataset citpatents-weighted

# echo "Testing pagerank..."
# time ./run_mosaic.py --dataset citpatents --algorithm pagerank --debug --max-iteration 10 > /dev/null 2>&1
echo "Testing cc..."
time ./run_mosaic.py --dataset citpatents --algorithm cc --debug --max-iteration 10
# echo "Testing bfs..."
# time ./run_mosaic.py --dataset citpatents --algorithm bfs --debug --max-iteration 10 > /dev/null 2>&1
# echo "Testing sssp..."
# time ./run_mosaic.py --dataset citpatents-weighted --debug --algorithm sssp > /dev/null 2>&1