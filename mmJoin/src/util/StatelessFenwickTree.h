// an implementation of the fenwick tree that follows C style, rather then
// C++ standard.
// The code for this implementation is based on the topcode tutorial
//
// all state needed to perform an algorithm are provided in the function call


#pragma once
#include <vector>
#include <algorithm>
#include <cstdint>

/**
 * The Fenwick tree is built on data[0..size - 2] (inclusive on both ends)
 * where size is the size of the data array. The prefix sum of idx is
 * sum_{i = 0}^{idx - 1}data[i]. Note that the last element is lost, which
 * should be stored somewhere else.
 */
namespace StatelessFenwickTree
{
    std::uint32_t log2(int32_t v);

    void initialize(int64_t *data_start, size_t size);

    // read the value of the prefix sum of a given index
    // we make the assumption that idx < size.  Because of this assumption
    // it is not necessary to state what size is
    int64_t read_value(int64_t *data_start, size_t idx);

    // read the frequency at idx
    int64_t readSingle_value(int64_t *data_start, size_t idx);

    // adds the given value to the fenwick tree.
    // data_start = the beginning pointer of the fenwick tree
    // size = size of the fenwick tree (number of items in the tree)
    // idx = the index of the value to update
    // value = the value to use to update
    void update_add_value(int64_t *data_start, size_t size, size_t idx, int64_t value);

    // scale up all values in the fenwick tree by a multiplier
    void scale_up(int64_t *data_start, size_t size, int64_t value);

    void scale_down(int64_t *data_start, size_t size, int64_t value);

    // read a frequency.
    // data_start = the beginning pointer of the fenwick tree
    // idx = the index of the value to read (we assume idx < size)
    int64_t readSingle(int64_t *data_start, size_t idx);

    size_t findG(int64_t *data_start, size_t size, int64_t cumFreq);

    // find the index which has the specified cumulative frequency
    // returns the index.  cumulative frequency will change to be
    // the remaining frequency after the search finishes.
    size_t findG_mod(int64_t *data_start, size_t size, int64_t &cumFreq);

    size_t findL(int64_t *data_start, size_t size, int64_t cumFreq);
}
