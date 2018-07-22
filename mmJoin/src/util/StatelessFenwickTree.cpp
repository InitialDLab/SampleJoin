//

#include "StatelessFenwickTree.h"

#include <assert.h>

using namespace StatelessFenwickTree;

uint32_t StatelessFenwickTree::log2(int32_t v)
{
    static const int MultiplyDeBruijnBitPosition[32] =
    {
        0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
        8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
    };

    v |= v >> 1; // first round down to one less than a power of 2 
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;

    return MultiplyDeBruijnBitPosition[(uint32_t)(v * 0x07C4ACDDU) >> 27];
}

void StatelessFenwickTree::initialize(int64_t *data_start, size_t size)
{
    std::vector<int64_t> sums;
    sums.resize(log2(size) + 1, 0);

    for (int i = 0; i < size; ++i) {
        int index = StatelessFenwickTree::log2(i&(-i));
        int64_t value = data_start[i];

        // we only support doing prefix sum of positive numbers
        assert(value > 0);

        data_start[i] = sums[index];

        // if we are taking a sum of positive numbers we should always output
        // something greater then or equal to zero.
        assert(data_start[i] >= 0);

        std::fill(sums.begin(), sums.begin() + index + 1, 0);
        std::transform(sums.begin(), sums.end(), sums.begin(), [value](int64_t v) { return value + v;});
    }
}

int64_t StatelessFenwickTree::read_value(int64_t *data_start, size_t idx)
{
    int64_t sum = 0;
    while (idx > 0) {
        sum += data_start[idx];
        idx -= (idx & -idx);
    }
    return sum;
}

int64_t StatelessFenwickTree::readSingle_value(int64_t *data_start, size_t idx)
{
    int64_t sum = data_start[idx];
    if (idx > 0) {
        int z = idx - (idx & -idx);
        --idx;
        while (idx != z) {
            sum -= data_start[idx];
            idx -= (idx & -idx);
        }
    }
    return sum;
}

void StatelessFenwickTree::update_add_value(int64_t *data_start, size_t size, size_t idx, int64_t value) {
    if (value == 0)
        return;
    while (idx < size)
    {
        data_start[idx] += value;
        idx += (idx & -idx);
    }
}

void StatelessFenwickTree::scale_up(int64_t *data_start, size_t size, int64_t value) {
    for (int i = 1; i < size; ++i)
        StatelessFenwickTree::update_add_value(data_start, size, i, -(value - 1) * StatelessFenwickTree::readSingle_value(data_start, i) / value);
}

void StatelessFenwickTree::scale_down(int64_t *data_start, size_t size, int64_t value) {
    for (int i = 1; i < size; ++i)
        data_start[i] = data_start[i] / value;
}

int64_t StatelessFenwickTree::readSingle(int64_t *data_start, size_t idx)
{
    int64_t sum = data_start[idx];
    if (idx > 0) {
        int z = idx - (idx & -idx);
        --idx;
        while (idx != z) {
            sum -= data_start[idx];
            idx -= (idx & -idx);
        }
    }
    return sum;
}

size_t StatelessFenwickTree::findG(int64_t *data_start, size_t size, int64_t cumFreq)
{
    // adjust for off by one?
    //--size;

    int idx = 0;
    uint32_t bitMask{ 1u << StatelessFenwickTree::log2(size) };

    while (bitMask != 0) {
        int tIdx = idx + bitMask;
        if (tIdx < size && cumFreq >= data_start[tIdx]) {
            idx = tIdx;
            cumFreq -= data_start[tIdx];
        }
        bitMask >>= 1;
    }

    assert(cumFreq >= 0);
    return idx;
}

size_t StatelessFenwickTree::findG_mod(int64_t * data_start, size_t size, int64_t & cumFreq)
{
    // adjust for off by one?
    //--size;

    int idx = 0;
    uint32_t bitMask{ 1u << StatelessFenwickTree::log2(size) };

    while (bitMask != 0) {
        int tIdx = idx + bitMask;
        if (tIdx < size && cumFreq >= data_start[tIdx]) {
            idx = tIdx;
            cumFreq -= data_start[tIdx];
        }
        bitMask >>= 1;
    }

    assert(cumFreq >= 0);
    return idx;
}

size_t StatelessFenwickTree::findL(int64_t *data_start, size_t size, int64_t cumFreq)
{
    // TODO: it might be useful to optimize search for lower bound
    int64_t cumFreT{ cumFreq };
    int retVal = StatelessFenwickTree::findG(data_start, size, cumFreT);
    int64_t cumFre1{ cumFreq - cumFreT - 1 };
    int retVal1 = StatelessFenwickTree::findG(data_start, size, cumFreq);

    cumFreq = cumFreT;

    return retVal1 + (retVal1 != retVal);
}
