#include "AdaptiveProbabilityIndexTriangle.h"

#include "../util/StatelessFenwickTree.h"
//

AdaptiveProbabilityIndexTriangle::AdaptiveProbabilityIndexTriangle(std::shared_ptr<TableGeneric> Table1, std::shared_ptr<TableGeneric> Table2, std::shared_ptr<TableGeneric> Table3)
{
    m_data.resize(3);

    m_data[0] = Table1;
    m_data[1] = Table2;
    m_data[2] = Table3;

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

    this->default_round2_AGM = (int64_t) Table2->get_size() * (int64_t) Table3->get_size();
    this->default_round2_AGM = 9000 * 9000; // TODO: fix this default value

    //this->initial_AGM = std::sqrt((int64_t)Table1->get_size() * (int64_t)Table2->get_size() * (int64_t)Table3->get_size());
    this->initial_AGM = (int64_t)Table1->get_size() * this->default_round2_AGM * 9000;

    //m_probability_index[0].resize(Table1->get_size(), std::min(Table2->get_size(), Table3->get_size()));
    m_start_probability_index.resize(Table1->get_size(), this->default_round2_AGM * 9000);

    // build the stateless Fenwick tree on the initial group
    StatelessFenwickTree::initialize(&m_start_probability_index[0], m_start_probability_index.size());
}

bool AdaptiveProbabilityIndexTriangle::sample_join(std::vector<int>& table_indexes, int64_t rvalue)
{
    int64_t pre_AGM = this->initial_AGM;

    // generate random value to where to search in the graph for a join tuple.
    // the random number generated should include 0, but exclude the max (max AGM bound).
    std::uniform_int_distribution<int64_t> dist(0, initial_AGM - 1);
    //int64_t max_output[3];
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

    // find the index which corresponds to the random int generated
    const auto index1 = StatelessFenwickTree::findG_mod(&m_start_probability_index[0], m_start_probability_index.size(), rvalue);
    //int64_t frequency1 = StatelessFenwickTree::read_value(&m_start_probability_index[0], index1);

    table_indexes[0] = index1;

    //rvalue -= frequency1;
    assert(rvalue >= 0);
    assert(rvalue < this->initial_AGM);

    // see if we need to build a new node for the point we selected in the 'R' table
    const auto value0 = m_data[0]->get_column1_value(table_indexes[0]);
    const auto value1 = m_data[0]->get_column2_value(table_indexes[0]);
    auto idx_S = m_maxFrequency.find(merge2_32bit(value0, value1));
    // if it does equal the end, we must build (or it does not exist)
    if (idx_S == m_maxFrequency.end())
    {
        point_information new_point;

        new_point.value = value1;

        new_point.m_frequencies.resize(m_data[1]->count_cardinality(value1), this->default_round2_AGM);
        if (new_point.m_frequencies.size() > 0)
            StatelessFenwickTree::initialize(&new_point.m_frequencies[0], new_point.m_frequencies.size());

        new_point.max_frequency = this->default_round2_AGM * new_point.m_frequencies.size();

        //m_maxFrequency.insert_or_assign(merge2_32bit(value0, value1), new_point);
        m_maxFrequency[merge2_32bit(value0, value1)] = new_point;

        idx_S = m_maxFrequency.find(merge2_32bit(value0, value1));
    }

    // check if we need to reject
    if (rvalue >= idx_S->second.max_frequency)
        rejected = 1;

    // we can find the tuple in 'S' we will select
    if (!rejected)
    {
        const auto offset2 = StatelessFenwickTree::findG_mod(&idx_S->second.m_frequencies[0], idx_S->second.m_frequencies.size(), rvalue);
        //const int64_t frequency2 = StatelessFenwickTree::read_value(&idx_S->second.m_frequencies[0], offset2);

        //rvalue -= frequency2;
        assert(rvalue >= 0);
        assert(rvalue < this->initial_AGM);

        table_indexes[1] = m_data[1]->get_column1_index_offset(value1, offset2);

        // see if we need to build an entry for the last table
        const auto value2 = m_data[1]->get_column2_value(table_indexes[1]);
        //auto idx_T = m_lastFrequency.find(value2);

        // For triangle join we must retain context of the prefix so we can update the join path appropriately
        auto idx_T = m_lastFrequency.find(merge2_32bit(value0, value2));
        if (idx_T == m_lastFrequency.end())
        {
            point_information new_point;

            auto count = m_data[2]->count_cardinality(value2);

            new_point.value = value2;
            // we default the weight to 1, because it will either accept or reject (but right now we don't know which it is).
            new_point.m_frequencies.resize(count, 1);
            if (new_point.m_frequencies.size() > 0)
                StatelessFenwickTree::initialize(&new_point.m_frequencies[0], new_point.m_frequencies.size());

            //                        the default weight is 1, so we set that as the multiplicative
            new_point.max_frequency = 1 * new_point.m_frequencies.size();

            //m_lastFrequency.insert_or_assign(merge2_32bit(value0, value2), new_point);
            m_maxFrequency[merge2_32bit(value0, value2)] = new_point;
            idx_T = m_lastFrequency.find(merge2_32bit(value0, value2));

            //max_output[2] = count;
        }

        if (rvalue >= idx_T->second.max_frequency)
            rejected = 2;

        if (!rejected)
        {
            const auto offset3 = StatelessFenwickTree::findG(&idx_T->second.m_frequencies[0], idx_T->second.m_frequencies.size(), rvalue);
            //const int64_t frequency3 = StatelessFenwickTree::read_value(&idx_T->second.m_frequencies[0], offset3);

            table_indexes[2] = m_data[2]->get_column1_index_offset(value2, rvalue);

            // validate the selected tuple is valid (T.A == S.A)
            if (value0 != m_data[2]->get_column2_value(table_indexes[2]))
            {
                rejected = 3;
            }
            else {
                // otherwise, we are accepted!
                //std::cout << "ACCEPTED\N";
            }


            // update the frequencies, if needed
            if (rejected)
            {
                if (offset3 + 1 == idx_T->second.m_frequencies.size())
                {
                    // since the weight default is 1, this should only come in here once,
                    // so we can just subtract the max by one
                    idx_T->second.max_frequency -= 1;
                    // now the weight should be equal to the total cumulative sum
                    assert(StatelessFenwickTree::readSingle_value(&idx_T->second.m_frequencies[0], idx_T->second.m_frequencies.size() - 1) == idx_T->second.max_frequency);
                }
                else
                {
                    assert(StatelessFenwickTree::readSingle_value(&idx_T->second.m_frequencies[0], offset3 + 1) == 1);
                    StatelessFenwickTree::update_add_value(&idx_T->second.m_frequencies[0], idx_T->second.m_frequencies.size(), offset3 + 1, -1);
                    idx_T->second.max_frequency -= 1;
                }
            }
        }

        // update the frequency for the 'S' table.  Only update if it decreases
        // the weight.
        // if it is the last element in the array
        if (offset2 + 1 == idx_S->second.m_frequencies.size())
        {
            int64_t last_largest_weight{ 0 };
            if (idx_S->second.m_frequencies.size() > 1)
                last_largest_weight = StatelessFenwickTree::read_value(&idx_S->second.m_frequencies[0], idx_S->second.m_frequencies.size() - 1);

            assert(last_largest_weight + idx_T->second.max_frequency <= idx_S->second.max_frequency);

            idx_S->second.max_frequency = last_largest_weight + idx_T->second.max_frequency;
            // since we are using exclusive fenwick tree, we don't need to update the tree
        }
        else
        {
            int64_t weight2 = StatelessFenwickTree::readSingle_value(&idx_S->second.m_frequencies[0], offset2 + 1);
            StatelessFenwickTree::update_add_value(&idx_S->second.m_frequencies[0], idx_S->second.m_frequencies.size(), offset2 + 1, idx_T->second.max_frequency - weight2);
            idx_S->second.max_frequency += idx_T->second.max_frequency - weight2;
            assert(idx_T->second.max_frequency - weight2 <= 0);
            assert(StatelessFenwickTree::readSingle_value(&idx_S->second.m_frequencies[0], offset2 + 1) == idx_T->second.max_frequency);
        }

        // if we will never go down this path in the future, lets do some cleanup
        // since we will never select this path in the future, it should be find to clean it up.
        if (idx_T->second.max_frequency == 0)
            m_lastFrequency.erase(idx_T);
    }

    // next, we need to update the frequency for 'R' table
    if (index1 + 1 == m_start_probability_index.size())
    {
        int64_t last_largest_weight{ 0 };
        if (m_start_probability_index.size() > 1)
            last_largest_weight = StatelessFenwickTree::read_value(&m_start_probability_index[0], m_start_probability_index.size() - 1);

        assert(last_largest_weight + idx_S->second.max_frequency <= this->initial_AGM);

        this->initial_AGM = last_largest_weight + idx_S->second.max_frequency;
    }
    else
    {
        int64_t weight1 = StatelessFenwickTree::readSingle_value(&m_start_probability_index[0], index1 + 1);
        StatelessFenwickTree::update_add_value(&m_start_probability_index[0], m_start_probability_index.size(), index1 + 1, idx_S->second.max_frequency - weight1);

        this->initial_AGM += idx_S->second.max_frequency - weight1;

        assert(idx_S->second.max_frequency - weight1 <= 0);
    }

    // if we will never go down this path in the future, lets do some cleanup
    // since we will never select this path in the future, it should be find to clean it up.
    if (idx_S->second.max_frequency == 0) {
        m_maxFrequency.erase(idx_S);
    }

    assert(pre_AGM >= this->initial_AGM);

    return rejected == 0;
}