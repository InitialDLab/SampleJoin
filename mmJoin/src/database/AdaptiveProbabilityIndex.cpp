#include "AdaptiveProbabilityIndex.h"

#include <random>

#include "../util/StatelessFenwickTree.h"

AdaptiveProbabilityIndex::AdaptiveProbabilityIndex(std::shared_ptr<TableGeneric> Table1, std::shared_ptr<TableGeneric> Table2, std::shared_ptr<TableGeneric> Table3)
{
    m_data.resize(3);

    m_data[0] = Table1;
    m_data[1] = Table2;
    m_data[2] = Table3;

    // build the index
    m_probability_index.resize(3);
    m_data.resize(3);

    // we can build everything in reverse, so we can get an idea of how data
    // is laid out.

    // the last items will all have the same probability, exactly 1.  Because
    // of this we don't need to build an index.

    // we will set the array for now
    // TODO: fix the bounds here to make them more tight
    //m_probability_index[1].resize(Table2->get_size(), Table3->get_size());

    // what should our initial AGM bound be for the first item?  Doing AGM
    // bound graph for the join R--S--T:
    // |R||S|
    // |R||T|
    // |S||T|
    // For now, we will use the first two arguments to bound the initial index
    // notice that for each value of R we will have the frequency |S| or |T|,
    // as a result, the total AGM bound will be |R||S|

    //this->default_round2_AGM = (int64_t) Table2->get_size() * (int64_t) Table3->get_size();
    //this->default_round2_AGM = Table3->get_size();
    this->default_round2_AGM = (int64_t) 10000 * 10000;
    //this->initial_AGM = (int64_t) Table1->get_size() * (int64_t) Table2->get_size() * (int64_t) Table3->get_size();
    this->initial_AGM = (int64_t)10000 * 10000 * 10000;

    //m_probability_index[0].resize(Table1->get_size(), std::min(Table2->get_size(), Table3->get_size()));
    m_probability_index[0].resize(Table1->get_size(), (int64_t)Table2->get_size() * (int64_t)Table3->get_size());

    // build the stateless Fenwick tree on the initial group
    StatelessFenwickTree::initialize(&m_probability_index[0][0], m_probability_index[0].size());

    //this->default_round2_AGM = std::min(Table2->get_size(), Table3->get_size());
    //this->initial_AGM = this->default_round2_AGM * Table1->get_size();
}

SAMPLE_STATUS AdaptiveProbabilityIndex::sample_join(std::vector<int>& table_indexes, int64_t rvalue)
{
    int64_t pre_AGM = this->initial_AGM;

    last_p = 1;

    // generate random value to where to search in the graph for a join tuple.
    // the random number generated should include 0, but exclude the max (max AGM bound).
    std::uniform_int_distribution<int64_t> dist(0, initial_AGM - 1);
    int64_t max_output[3];
    if (rvalue == 0)
    {
        rvalue = dist(m_rgen);
    }
    else
    {
        // make sure we are not over the max.
        rvalue %= initial_AGM;
    }
    //int64_t rvalue = 10;
    int64_t remember_rvalue = rvalue;
    // the rejected item will keep track of when level we rejected
    int rejected = 0;
    SAMPLE_STATUS ret_val = GOOD_S;

    // find the index which corresponds to the random int generated
    auto index1 = StatelessFenwickTree::findG_mod(&m_probability_index[0][0], m_probability_index[0].size(), rvalue);
    //int64_t frequency1 = StatelessFenwickTree::read_value(&m_probability_index[0][0], index1);

    table_indexes[0] = index1;

    //rvalue -= frequency1;

    // see if we need to build a new node for the point we selected in the 'R' table
    auto value1 = m_data[0]->get_column2_value(index1);
    auto idx_S = m_maxFrequency.find(value1);
    // if it does equal the end, we must build (or it does not exist)
    if (idx_S == m_maxFrequency.end())
    {
        point_information new_point;

        new_point.value = value1;

        new_point.m_frequencies.resize(m_data[1]->count_cardinality(value1), this->default_round2_AGM);
        if(new_point.m_frequencies.size() > 0)
            StatelessFenwickTree::initialize(&new_point.m_frequencies[0], new_point.m_frequencies.size());

        new_point.max_frequency = this->default_round2_AGM * new_point.m_frequencies.size();

        //m_maxFrequency.insert_or_assign(value1, new_point);
        m_maxFrequency[value1] = new_point;

        idx_S = m_maxFrequency.find(value1);
    }

    // check if we need to reject
    if (rvalue >= idx_S->second.max_frequency) {
        rejected = 1;
        if (idx_S->second.max_frequency == 0)
            ret_val = FAIL_S;
        else
            ret_val = REJECT_S;
    }

    // we can find the tuple in 'S' we will select
    if (!rejected)
    {
        auto offset2 = StatelessFenwickTree::findG_mod(&idx_S->second.m_frequencies[0], idx_S->second.m_frequencies.size(), rvalue);
        //int64_t frequency2 = StatelessFenwickTree::read_value(&idx_S->second.m_frequencies[0], offset2);

        //rvalue -= frequency2;

        table_indexes[1] = m_data[1]->get_column1_index_offset(value1, offset2);

        // see if we need to build an entry for the last table
        auto value2 = m_data[1]->get_column2_value(table_indexes[1]);
        auto idx_T = m_lastFrequency.find(value2);
        if (idx_T == m_lastFrequency.end())
        {
            auto count = m_data[2]->count_cardinality(value2);
            //m_lastFrequency.insert_or_assign(value2, count);
            m_lastFrequency[value2] = count;
            idx_T = m_lastFrequency.find(value2);

            max_output[2] = count;
        }

        if (rvalue >= idx_T->second) {
            rejected = 2;
            if (idx_T->second == 0)
                ret_val = FAIL_S;
            else
                ret_val = REJECT_S;
        }

        if (!rejected)
        {
            // otherwise, select the last value (tuple from 'T') and we are good!
            table_indexes[2] = m_data[2]->get_column1_index_offset(value2, rvalue);
        }

        // update the frequency for the 'S' table.  Only update if it decreases
        // the weight.
        // if it is the last element in the array
        if (offset2 + 1 == idx_S->second.m_frequencies.size())
        {
            int64_t last_largest_weight{ 0 };
            if (idx_S->second.m_frequencies.size() > 1)
                last_largest_weight = StatelessFenwickTree::read_value(&idx_S->second.m_frequencies[0], idx_S->second.m_frequencies.size() - 1);

            idx_S->second.max_frequency = last_largest_weight + idx_T->second;
            // since we are using exclusive fenwick tree, we don't need to update the tree
        }
        else
        {
            int64_t weight2 = StatelessFenwickTree::readSingle_value(&idx_S->second.m_frequencies[0], offset2+1);
            StatelessFenwickTree::update_add_value(&idx_S->second.m_frequencies[0], idx_S->second.m_frequencies.size(), offset2 + 1, idx_T->second - weight2);
            idx_S->second.max_frequency += idx_T->second - weight2;
        }
    }

    // next, we need to update the frequency for 'R' table
    if (index1 + 1== m_probability_index[0].size())
    {
        int64_t last_largest_weight{ 0 };
        if (m_probability_index[0].size() > 1)
            last_largest_weight = StatelessFenwickTree::read_value(&m_probability_index[0][0], m_probability_index[0].size() - 1);

        this->initial_AGM = last_largest_weight + idx_S->second.max_frequency;
    }
    else
    {
        int64_t weight1 = StatelessFenwickTree::readSingle_value(&m_probability_index[0][0], index1+1);
        StatelessFenwickTree::update_add_value(&m_probability_index[0][0], m_probability_index[0].size(), index1+1, idx_S->second.max_frequency - weight1);

        this->initial_AGM += idx_S->second.max_frequency - weight1;
    }

    assert(pre_AGM >= this->initial_AGM);

    return ret_val;
}

int64_t AdaptiveProbabilityIndex::test_scan()
{
    // start from the beginning.  
    // * If the weight decreases, repeat.
    // * If the weight does not decrease but is accepted, increase seed.
    // * If the weight does not decrease but is rejected, we found a problem.

    int64_t trials = 0;
    int64_t idx = this->initial_AGM;
    std::vector<int> data(3);

    //while (idx < this->initial_AGM)
    while(idx > 0)
    {
        idx = std::min(idx, this->initial_AGM - 1);

        ++trials;

        if (trials % 10000 == 0)
            std::cout << "\r" << "                         " << '\r' << this->initial_AGM << std::flush;

        int64_t last_AGM = this->initial_AGM;
        bool last_accept = this->sample_join(data, idx);

        // we are converging, so continue with current idx
        if (last_AGM > this->initial_AGM)
        {
            continue;
        }
        else if (last_AGM < this->initial_AGM)
        {
            std::cout << "FOUND A PROBLEM -- bound did not decrease at idx=" << idx << std::endl;
            continue;
        }
        else if (/* weights are equal */ last_accept == true)
        {
            --idx;
            continue;
        }
        else
        {
            std::cout << "FOUND A PROBLEM -- rejected but not modified at idx=" << idx << std::endl;
            continue;
        }
    }

    return int64_t();
}
