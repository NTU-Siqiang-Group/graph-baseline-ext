#!/bin/bash

# ./bin/preprocess -i ../data/wikitalk -o ./data/wikitalk_grid -v 2394385 -p 4 -t 0
# echo "bfs on wikitalk_grid..."
# ./bin/bfs data/wikitalk_grid 6765 | grep seconds
# echo "pagerank on wikitalk_grid..."
# ./bin/pagerank data/wikitalk_grid 10 | grep seconds
# echo "wcc on wikitalk_grid..."
# ./bin/wcc data/wikitalk_grid | grep seconds
# echo "sssp on wikitalk_grid..."
# ./bin/sssp data/wikitalk_grid 32822 33 | grep seconds
# echo "cdlp on wikitalk_grid..."
# ./bin/cdlp data/wikitalk_grid

./bin/preprocess -i ../data/citpatents -o ./data/citpatents_grid -v 3774769 -p 4 -t 0
echo "bfs on citpatents_grid..."
./bin/bfs data/citpatents_grid 3494505 | grep seconds
echo "pagerank on citpatents_grid..."
./bin/pagerank data/citpatents_grid 10 | grep seconds
echo "wcc on citpatents_grid..."
./bin/wcc data/citpatents_grid | grep seconds
echo "sssp on citpatents_grid..."
./bin/sssp data/citpatents_grid 3494505 754148 | grep seconds
echo "cdlp on citpatents_grid..."
./bin/cdlp data/citpatents_grid