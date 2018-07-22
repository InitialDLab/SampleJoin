#pragma once
#include <algorithm>
#include <assert.h>

#include "DynamicLevel.h"
#include "TableGeneric.h"

class DynamicIndex {
public:
    DynamicIndex(size_t join_length)
        : m_first_selection_set{ false }
        , m_last_min_value{ false }
        , wander_join_level{ 1 }
    {
        m_tables.resize(join_length);
        m_levels.resize(join_length);
        m_maxAGM.resize(join_length);

        m_rgen.seed(std::chrono::system_clock::now().time_since_epoch().count());
    };

    virtual ~DynamicIndex()
    {};

    void add_Table(int index, std::shared_ptr<TableGenericBase> table, int64_t max_AGM = 0)
    {
        if (max_AGM == 0)
            max_AGM = table->get_size();

        m_tables[index] = table;
        m_maxAGM[index] = max_AGM;
    }

    int initialize(int DP_level = -1)
    {
        if (DP_level == -1)
            DP_level = 1;

        wander_join_level = DP_level;

        //m_levels[0].reset(new DynamicInitialLevel(m_tables[0], m_maxAGM[0]));
        //m_levels[0].reset(nullptr);
        m_startlevel.reset(new DynamicInitialLevel(m_tables[0], m_maxAGM[0]));

        for (int i = 1; i < m_levels.size(); ++i)
        {
            m_levels[i].reset(new DynamicLevel(m_tables[i], m_maxAGM[i]));
        }

        return 0;
    }

    void warmup()
    {
        this->wander_join_warmup(this->wander_join_level);
    }
    
    bool sample_join(std::vector<int> &output)
    {
        std::vector<std::unordered_map<jfkey_t, std::shared_ptr<DynamicVertex> >::iterator> search_itr(m_tables.size());
        std::vector<int64_t> update_id(m_tables.size());
        std::vector<double> p_value(m_tables.size(), 1.0);
        //search_itr.resize(m_tables.size());
        //update_id.resize(m_tables.size());
        //p_value.resize(m_tables.size(), 1.0);

        //std::uniform_int_distribution<int64_t> dist(0, m_levels[0]->upper_bound() - 1);

        int64_t weight{ 0 };// = dist(m_rgen);

        int64_t value = 0;
        int64_t next_index = 0;

        WALK_STATUS status = GOOD;

        // we do initial lookup outside the for loop
        auto start_itr = m_startlevel->GetNextStep(value, weight, next_index, update_id[0], status, p_value[0]);
        output[0] = next_index;
        value = m_tables[0]->get_column2_value(next_index);

        if (m_first_selection_set) {
            if (value < m_first_min_value)
                status = FAIL;
        }

        int i = 1;
        // fill in the search iterators
        for ( ; i < search_itr.size() && !(status & FAIL); ++i)
        {
            search_itr[i] = m_levels[i]->get_vertex(value);

            // check if we need to do a DP scan of this region
            if (i == this->wander_join_level)
            {
                if (!search_itr[i]->second->is_DPset())
                {
                    //std::cout << '.';
                    this->DP_calculation(i, value);
                }
            }

            // do a weighted search
            status |= search_itr[i]->second->get_records_p(weight, next_index, update_id[i], p_value[i]);

            //search_itr[i] = m_levels[i]->GetNextStep(value, weight, next_index, update_id[i], status, p_value[i]);

            // see if the walk is valid
            if (search_itr[i] == m_levels[i]->m_data.end())
            {
                status |= FAIL;
                // in this case we fail
            }

            if (status & FAIL)
            {
                break;
            }

            // see if we accept or reject the walk
            //m_home->get_column2_value
            //output[i] = m_tables[i]->get_column2_value(next_index);
            output[i] = next_index;
            value = m_tables[i]->get_column2_value(next_index);
        }

        // do selection condition, if needed
        if (m_last_selection_set && ((status & FAIL) == GOOD))
        {
            if (value < m_last_min_value)
                status = FAIL;
        }

        i = i == m_levels.size() ? m_levels.size() - 1 : i;

        // this is the max value used for i (which would be valid to use i + 1 indexing)
        int max_i = i - 1;

        // go back through the points to update accepts
        // we can also use this time to update weights?
        //if (status & FAIL)
        //{
        //    --i;
        //}
        //else {
        //}

        // we can do the first round outside the loop, because it will only happen
        // up to one times
        for (int t = p_value.size() - 2; t >= 0; t--) {
            p_value[t] = p_value[t] * p_value[t + 1];
        }       
        
        // I think we only need to do weight adjustment and reporting for all < wander join limit

        i = std::min(i, this->wander_join_level - 1);
        for (; i >= 1; --i)
        {
            // this should probably never happen
            if ((status & FAIL) && max_i == i) {
                search_itr[i]->second->adjust_weight(0, update_id[i]);
            }
            else {
                search_itr[i]->second->adjust_weight(search_itr[i + 1]->second->get_max_weight(), update_id[i]);
            }

            search_itr[i]->second->report(p_value[i], (status & FAIL) == GOOD);
        }

        //if (i == m_levels.size() - 1
        //    && search_itr[i] != m_levels[i]->m_data.end()
        //    && i != 0) {
        //    search_itr[i]->second->adjust_weight((status & FAIL) == GOOD, update_id[i]);

        //    search_itr[i]->second->report(p_value[i], (status & FAIL) == GOOD);
        //}


        //for (--i /*because we already did first loop above*/; i >= 1; --i) {
        //    if ((status & FAIL) && max_i == i) {
        //        search_itr[i]->second->adjust_weight(0, update_id[i]);
        //    }
        //    else {
        //        //if(i==0)
        //        //    std::cout << search_itr[i + 1]->second->max_weight << '\n';
        //        search_itr[i]->second->adjust_weight(search_itr[i + 1]->second->get_max_weight(), update_id[i]);
        //    }

        //    search_itr[i]->second->report(p_value[i], (status & FAIL) == GOOD);
        //}

        //////////////////////////////
        // do initial update by itself
        if ((status & FAIL) && max_i == 0) {
            // this should probably never happen with our current setup
            start_itr->second->adjust_weight(0, update_id[0]);
        }
        else {
            start_itr->second->adjust_weight(search_itr[i + 1]->second->get_max_weight(), update_id[i]);
        }

        start_itr->second->report(p_value[i], (status & FAIL) == GOOD);

        if (status == GOOD)
            m_stats.good_walks++;
        if (status &= FAIL)
            m_stats.failed_walks++;
        if (status &= REJECT)
            m_stats.rejected_walks++;

        return status == GOOD;
    }

    // do recursive calculations to get DP value
    int64_t DP_calculation(int level, int64_t key)
    {
        // if we are at the end of the join...
        if (level >= this->m_levels.size())
            return 1;

        if (level != 0) {

            auto vertex = m_levels[level]->get_vertex(key);

                // if we already have the DP information for this vertex, just return it.
            if (vertex->second->is_DPset())
                return vertex->second->get_weight();

            // otherwise, we need to fill in the DP calculations for the vertex
            int i = 0;
            for (auto idx = m_tables[level]->m_column1_fast_index[key].begin();
                idx != m_tables[level]->m_column1_fast_index[key].end();
                ++idx)
            {
                vertex->second->adjust_DP_weight(DP_calculation(level + 1, m_tables[level]->get_column2_value(*idx)), i++);
            }
            assert(vertex->second->is_DPset());
            return vertex->second->get_weight();

        }
        else {
            auto vertex = m_startlevel->get_vertex();

            if (vertex->second->is_DPset())
                return vertex->second->get_weight();

            for (auto idx = 0; idx < m_tables[0]->get_size(); ++idx)
            {
                vertex->second->adjust_DP_weight(DP_calculation(level + 1, m_tables[0]->get_column2_value(idx)), idx);
            }
            assert(vertex->second->is_DPset());
            return vertex->second->get_weight();
        }
    }

    void wander_join_warmup(int level)
    {
        // each item will need to run the algorithm.. If we do the same one twice that is okay
        // because it will be detected quickly
        //for (auto idx = 0; idx < m_tables[level]->get_size(); ++idx)
        for(auto uniq_itr = m_tables[level]->m_uniquecolumn1.begin(); uniq_itr != (m_tables[level]->m_uniquecolumn1.end()); ++uniq_itr)
        {
            do_wanderjoin(m_levels[level]->get_vertex(*uniq_itr)->second, level);
            // find vertex to run wander join on
        }

        // push back the weights to the source
        for (int c_level = level - 1; c_level > 0; --c_level)
        {
            for (auto uniq_itr = m_tables[c_level]->m_uniquecolumn1.begin(); uniq_itr != (m_tables[c_level]->m_uniquecolumn1.end()); ++uniq_itr)
            {
                // for each item in the vertex, push the estimator and weights up
                int i_val = 0;
                auto update_vertex = m_levels[c_level]->get_vertex(*uniq_itr)->second;
                double fanout = m_tables[c_level]->m_column1_fast_index[*uniq_itr].size();
                auto end_i = m_tables[c_level]->m_column1_fast_index[*uniq_itr].end();
                for (auto key = m_tables[c_level]->m_column1_fast_index[*uniq_itr].begin(); key != end_i; ++key)
                {
                    auto trial_vertex = m_levels[c_level + 1]->get_vertex(m_tables[c_level]->get_column2_value(*key))->second;

                    update_vertex->S_sum += (trial_vertex->S_sum * fanout);
                    update_vertex->V_sum += (trial_vertex->V_sum * (fanout * fanout));
                    update_vertex->successful_walks += trial_vertex->successful_walks;
                    update_vertex->walk_count += trial_vertex->walk_count;

                    update_vertex->adjust_weight(trial_vertex->get_max_weight(), i_val);
                    
                    ++i_val;
                }
            }
        }

        // do source separate
        {
            auto update_vertex = m_startlevel->get_vertex()->second;
            double fanout = m_tables[0]->get_size();
            for (auto source_idx = 0; source_idx < m_tables[0]->get_size(); ++source_idx)
            {
                auto trial_vertex = m_levels[1]->get_vertex(m_tables[0]->get_column2_value(source_idx))->second;

                update_vertex->S_sum += (trial_vertex->S_sum * fanout);
                update_vertex->V_sum += (trial_vertex->V_sum * (fanout * fanout));
                update_vertex->successful_walks += trial_vertex->successful_walks;
                update_vertex->walk_count += trial_vertex->walk_count;

                update_vertex->adjust_weight(trial_vertex->get_max_weight(), source_idx);
            }
        }
    }

    int64_t do_wanderjoin(std::shared_ptr<DynamicVertex> vertex, int starting_level)
    {
        int loops = 0;

        while (!vertex->WJ_stats_good())
        {
            if (vertex->walk_count > 10000)
            {
                throw "BAD, did not get enough good samples";
            }

            // do a wander join round starting here
            int current_level = starting_level;
            jefastKey_t value = vertex->m_value;
            bool failed_walk = false;
            double p = 1.0;

            while(current_level < m_tables.size())
            {
                std::vector<jefastKey_t>* index = &m_tables[current_level]->m_column1_fast_index[value];
                int degree = index->size();
                //int degree = m_tables[current_level]->count_cardinaltiy_f(value);
                if (degree == 0)
                {
                    failed_walk = true;
                    break;
                }
                // randomly select which path to go down.
                std::uniform_int_distribution<int> dist(0, degree - 1);
                int next_idx = dist(m_rgen);

                value = m_tables[current_level]->get_column2_value((*index)[next_idx]);
                //value = m_tables[current_level]->get_column2_value(m_tables[current_level]->get_column1_index_offset2(value, next_idx));

                // update the probability we got here.
                p /= (double)degree;
                ++current_level;
            }

            vertex->report(p, !failed_walk);
            ++loops;
        }
        return loops;
    }

    void test_DP_set()
    {
        for (auto i = 0; i < m_tables[0]->get_size(); ++i)
            std::cout << "index= " << i << " DP= " << DP_calculation(1, m_tables[0]->get_column2_value(i)) << std::endl;
    }

    void set_final_selection_min_condition(int64_t min_value)
    {
        m_last_selection_set = true;
        m_last_min_value = min_value;
    }

    void set_first_selection_min_condition(int64_t min_value)
    {
        m_first_selection_set = true;
        m_first_min_value = min_value;
    }

    int64_t initial_upper_bound()
    {
        return m_startlevel->upper_bound();
    }

    int64_t size_est()
    {
        return m_startlevel->est_size();
    }

private:
    std::vector<int64_t> m_maxAGM;

    std::vector<std::shared_ptr<TableGenericBase> > m_tables;

    std::vector<std::shared_ptr<DynamicLevel> > m_levels;

    std::shared_ptr<DynamicInitialLevel> m_startlevel;

    std::mt19937_64 m_rgen;

    int wander_join_level;

    bool m_last_selection_set;
    int64_t m_last_min_value;

    bool m_first_selection_set;
    int64_t m_first_min_value;

    struct stats {
        int good_walks;
        int rejected_walks;
        int failed_walks;
    } m_stats;

};
