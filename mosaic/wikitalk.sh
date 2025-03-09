#!/bin/bash

cd src/tools/scripts

python3 ./generate_graph.py --dataset wikitalk --binary --in-memory
# python3 ./generate_graph.py --dataset wikitalk --binary --in-memory --weighted

python3 ./rebalance_input.py --dataset wikitalk
# python3 ./rebalance_input.py --dataset wikitalk-weighted

python3 ./run_tiles_indexer.py --dataset wikitalk
# python3 ./run_tiles_indexer.py --dataset wikitalk-weighted

# echo "Testing pagerank..."
# time ./run_mosaic.py --dataset wikitalk --algorithm pagerank --max-iteration --debug 10 > /dev/null 2>&1
echo "Testing cc..."
time ./run_mosaic.py --dataset wikitalk --algorithm cc --debug --max-iteration 10
# echo "Testing bfs..."
# time ./run_mosaic.py --dataset wikitalk --algorithm bfs --max-iteration 10 > /dev/null 2>&1
# echo "Testing sssp..."
# time ./run_mosaic.py --dataset wikitalk-weighted --algorithm sssp > /dev/null 2>&1