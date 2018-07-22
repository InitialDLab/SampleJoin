# SampleJoin

Source code for paper ``Random Sampling over Joins Revisited'' (SIGMOD '18).

The main memory implementation is located in mmJoin.  Create makefiles using
cmake.  The code is written for a C++11 compiler.

## Build
1. cd mmJoin
2. install cmake, make, gcc, g++ (>= 4.8)
3. mkdir build
4. cd build
5. cmake -DCMAKE_BUILD_TYPE=Release .. (or Debug if you want to debug it using gdb, also need to change the -O2 to -O0 in CMakeList.txt)
6. make

## Prepare Data
1. TPC-H. Place generated *.tbl in mmJoin/build/src/${scaleFactor}x/. Rename lineitem.tbl to lineitem_skew.tbl (or change the file path in mmJoin/src/generic_sample_test.cpp). Symbolic links also work.
2. Twitter graph data from [SNAP](http://snap.stanford.edu/data/). // TODO upload the data preparation tools


## run
1. cd mmJoin/build/src
2. ./generic_sample_test [args]
    Use --help and refer to mmJoin/src/generic_sample_test.cpp for help.
    Note: TPC-H queries are named TCPX (X='3', 'X', 'Y') in the source file due to a misspelling.


## licensing
This repository is shared for academic purpose only. Any one can use,
copy, modify and distribute the software, provided this notice is also
included. We disclaim any
warranty for any particular purpose nor shall we be liable to any one
for using the code.


