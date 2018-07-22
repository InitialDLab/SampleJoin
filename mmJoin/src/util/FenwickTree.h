#pragma once
#include <vector>
#include <algorithm>

// the code for this implementation is based on the topcoder tutorial.
// https://www.topcoder.com/community/data-science/data-science-tutorials/binary-indexed-trees/
class FenwickTree
{
private:
    int64_t *data_base;
    uint32_t m_size;
    std::vector<int64_t> m_tree;
    uint32_t m_mask;
public:
    FenwickTree(int size)
    {
        m_tree.resize(size + 1);
        m_mask = 1 << log2(m_tree.size());
        m_tree = std::move(pre_tree);
        m_tree.resize(m_tree.size() + 1);
        m_mask = 1 << log2(m_tree.size());

        std::vector<int64_t> sums;
        sums.resize(log2(m_tree.size()) + 1);

        for (int i = 0; i < m_tree.size(); ++i)
        {

            int index = log2(i&(-i));
            int64_t value = m_tree[i];
            m_tree[i] = sums[index];

            std::fill(sums.begin(), sums.begin() + index + 1, 0);
            std::transform(sums.begin(), sums.end(), sums.begin(), [value](int v) { return value + v;});
        }

        //m_tree.back() = sums.back();

    }

    ~FenwickTree()
    {};

    // read the cumulative frequency at idx
    int64_t read(int idx) {
        int64_t sum = 0;
        while (idx > 0) {
            sum += m_tree[idx];
            idx -= (idx & -idx);
        }
        return sum;
    };

    // change the frequency at some position and update tree
    void update(int idx, int val) {
        while (idx <= m_tree.size()) {
            m_tree[idx] += val;
            idx += (idx & -idx);
        }
    }

    // read the frequency at idx
    int64_t readSingle(int idx) {
        int64_t sum = m_tree[idx];
        if (idx > 0) {
            int z = idx - (idx & -idx);
            --idx;
            while (idx != z) {
                sum -= m_tree[idx];
                idx -= (idx & -idx);
            }
        }
        return sum;
    };

    // scale the entire tree by a constant factor
    void scale_up(int c) {
        for (int i = 1; i <= m_tree.size(); ++i)
            update(-(c - 1) * readSingle(i) / c, i);
    };

    // scale the entire tree by a constant factor
    void scale_down(int c) {
        for (int i = 1; i <= m_tree.size(); ++i)
            m_tree[i] = m_tree[i] / c;
    };

    size_t find(int64_t &cumFre) {
        int idx = 0; // this var is result of function
        uint32_t bitMask{ m_mask };

        while ((bitMask != 0) && (idx < m_tree.size())) { // nobody likes overflow :)
            int tIdx = idx + bitMask;
            if (cumFre == m_tree[tIdx]) // if it is equal, we just return idx
                return tIdx;
            else if (cumFre > m_tree[tIdx]) {
                // if tree frequency "can fit" into cumFree,
                // then include it
                idx = tIdx; // update index
                cumFre -= m_tree[tIdx]; // set frequency for next loop
            }
            bitMask >>= 1; // half current interval
        }

        //if (cumFre != 0) // maybe given cumulative frequency doesn't exist
        //        return -1;
        //else
        return idx;
    };

    // if the tree has more than one index with the same
    // cumulative frequency, this procedure will return
    // the greatest one
    int findG(int64_t &cumFre) {
        int idx = 0;
        uint32_t bitMask{ m_mask };

        while ((bitMask != 0) && (idx < m_tree.size())) {
            int tIdx = idx + bitMask;
            if (cumFre >= m_tree[tIdx]) {
                // if current cumulative frequency is equal to cumFre,
                // we are still looking for higher index (if exists)
                idx = tIdx;
                cumFre -= m_tree[tIdx];
            }
            bitMask >>= 1;
        }
        return idx;
    };

    // if the tree has more than one index with the same
    // cumulative frequency, this procedure will return
    // the greatest one
    int findL(int64_t &cumFre) {
        // TODO optimize search for lower bound
        //int64_t cumFre1{ cumFre - 1 };
        int64_t cumFreT{ cumFre };
        //int retVal1 = findG(cumFre1);
        int retVal = findG(cumFreT);
        int64_t cumFre1{ cumFre - cumFreT - 1 };
        int retVal1 = findG(cumFre1);

        cumFre = cumFreT;

        // if they are not equal, increase by one.
        return retVal1 + (retVal1 != retVal);
    };

    size_t size()
    {
        return m_tree.size();
    }
    int64_t get_value(size_t idx)
    {
        return m_tree[idx];
    }
private:
    int log2(int32_t v)
    {
        int r;      // result goes here

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
};
