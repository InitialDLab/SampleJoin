#pragma once
#include <algorithm>
#include <vector>
#include <memory>
#include <assert.h>
#include <random>
#include <chrono>

#include "TableGeneric.h"

class olkenIndex {
public:
    olkenIndex(size_t join_length)
        : trials{ 0 }
    {
        m_tables.resize(join_length);
        m_maxOutdegree.resize(join_length);

        //r_gen.seed(std::chrono::system_clock::now().time_since_epoch().count());
    }

    virtual ~olkenIndex()
    {}

    void add_Table(int index, std::shared_ptr<TableGeneric> table, int64_t maxOutdegree)
    {
        m_tables[index] = table;
        m_maxOutdegree[index] = maxOutdegree;

        max_p = 1.0;
        for (auto i = m_maxOutdegree.begin(); i != m_maxOutdegree.end(); ++i)
            max_p *= *i;
    }

    bool sample_join(std::vector<jefastKey_t> &output)
    {
        output.clear();
        output.resize(m_tables.size());

        //std::mt19937_64 r_gen;
        //std::random_device rd;
        r_gen.seed(std::chrono::steady_clock::now().time_since_epoch().count() * trials);
        ++trials;

        // select first record
        std::uniform_int_distribution<int> dist(0, m_tables[0]->get_size() - 0);
        
        jefastKey_t next_idx = dist(r_gen);
        output[0] = next_idx;
        jefastKey_t value = m_tables[0]->get_column2_value(next_idx);


        //double p = m_tables[0]->get_size();
        double p = 1.0;

        // select remaining records
        int current_level = 1;
        while (current_level < m_tables.size())
        {
            std::vector<jefastKey_t>* index = &m_tables[current_level]->m_column1_fast_index[value];
            int degree = m_tables[current_level]->count_cardinality(value, 1);
            //int degree = index->size();

            if (degree == 0)
            {
                // walk fails
                return false;
            }

            std::uniform_int_distribution<int> dist(0, degree - 1);
            next_idx = dist(r_gen);

            output[current_level] = m_tables[current_level]->get_column1_index_offset(value, next_idx);

            value = m_tables[current_level]->get_column2_value(output[current_level]);
            //value = m_tables[current_level]->get_column2_value((*index)[next_idx]);


            p *= degree;
            ++current_level;
        }


        // reject with appropriate probability
        std::uniform_real_distribution<double> dist_p(0.0, max_p);

        return dist_p(r_gen) < p;
    }


private:
    std::vector<std::shared_ptr<TableGeneric> > m_tables;
    std::vector<int64_t> m_maxOutdegree;

    std::mt19937_64 r_gen;

    double max_p;

    int trials = 0;
};
