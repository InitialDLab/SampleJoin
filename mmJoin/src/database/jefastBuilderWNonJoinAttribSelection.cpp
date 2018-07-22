//#include "jefastBuilderWNonJoinAttribSelection.h"
//
//#include <chrono>
//#include <iostream>
//#include <algorithm>
//#include <cmath>
//
//std::shared_ptr<jefastIndexLinear> jefastBuilderWNonJoinAttribSelection::Build()
//{
//    // we fill the data backwards
//    std::shared_ptr<jefastIndexLinear> builder{ new jefastIndexLinear() };
//
//    builder->m_levels.resize(this->m_initialIndex->m_levels.size());
//
//    int start_rebuild_index;
//
//    // 1 - we use existing items from the end until we have a filtering condition
//    int idx = builder->m_levels.size() - 1;
//    builder->m_levels.at(idx) = this->m_initialIndex->m_levels.at(idx);
//
//    for (;idx > 0; --idx)
//    {
//        // we have to do an adjustment to get the table for this join
//        if (this->m_filters.at(idx + 1).size() != 0)
//            break;
//
//        builder->m_levels.at(idx) = this->m_initialIndex->m_levels.at(idx);
//    }
//
//    // TODO: copy back until filtering exists
//
//
//    // do a range query the first time.
//    bool first_loop = true;
//    start_rebuild_index = idx;
//
//    // do a first loop, iterating over items in 
//    //for (; idx >= 0; --idx)
//    {
//        builder->m_levels.at(idx).reset(new JefastLevel<jfkey_t>(this->m_initialIndex->m_levels.at(idx)->get_LHS_Table(),
//            this->m_initialIndex->m_levels.at(idx)->get_RHS_Table(),
//            false));
//        builder->m_levels.at(idx)->set_LHS_table_index(this->m_initialIndex->m_levels.at(idx)->get_LHS_table_index());
//        builder->m_levels.at(idx)->set_RHS_table_index(this->m_initialIndex->m_levels.at(idx)->get_RHS_table_index());
//
//
//        // NOTE: we are using the partition id to index the table we are intereted
//        //    in using.  Make sure you know the difference
//        auto i_enu = m_filters.at(idx + 1).front()->getEnumerator();
//
//        //std::vector<jfkey_t>::iterator current_table = builder->m_levels.at(idx + 1)->get_LHS_Table()->get_key_iterator(builder->m_levels.at(idx)->get_LHS_table_index());
//
//        int counter = 0;
//        jfkey_t max_rid = 0;
//        jfkey_t min_rid = 10000000;
//        while (i_enu->Step()) {
//            jfkey_t record_id = i_enu->getValue();
//
//
//
//            // validate other filters
//            bool valid = true;
//            for (int i = 1; i < m_filters.at(idx + 1).size(); ++i) {
//                if (!m_filters.at(idx + 1).at(i)->Validate(record_id)) {
//                    valid = false;
//                    break;
//                }
//            }
//            if (valid == false)
//                continue;
//
//            //jfkey_t r_value = builder->m_levels.at(idx + 1)->get_RHS_Table()->get_int64(record_id, builder->m_levels.at(idx + 1)->get_index_for_RHS_table());
//            jfkey_t l_value = builder->m_levels.at(idx)->get_RHS_Table()->get_int64(record_id, builder->m_levels.at(idx)->get_index_for_LHS_table());
//
//            max_rid = std::max(max_rid, l_value);
//            min_rid = std::min(min_rid, l_value);
//            //if (!first_loop)
//            //    builder->m_levels.at(idx + 1)->InsertLHSRecord(r_value, record_id);
//            builder->m_levels.at(idx)->InsertRHSRecord(l_value, record_id);
//            ++counter;
//        }
//        //std::cout << "counter=" << counter << std::endl;
//        //std::cout << "min=" << min_rid << " max=" << max_rid;
//        //first_loop = false;
//    }
//    --idx;
//
//    // continue back, checking for filtering conditions
//    for (; idx >= 0; --idx) {
//        builder->m_levels.at(idx).reset(new JefastLevel<jfkey_t>(this->m_initialIndex->m_levels.at(idx)->get_LHS_Table(),
//            this->m_initialIndex->m_levels.at(idx)->get_RHS_Table(),
//            false));
//        builder->m_levels.at(idx)->set_LHS_table_index(this->m_initialIndex->m_levels.at(idx)->get_LHS_table_index());
//        builder->m_levels.at(idx)->set_RHS_table_index(this->m_initialIndex->m_levels.at(idx)->get_RHS_table_index());
//
//        auto i_idx = builder->m_levels.at(idx + 1)->m_data.begin();
//        auto e_idx = builder->m_levels.at(idx + 1)->m_data.end();
//
//        //std::vector<jfkey_t>::iterator current_table = builder->m_levels.at(idx + 1)->mp_LHS_Table->get_key_iterator(builder->m_levels.at(idx)->get_LHS_table_index());
//        std::vector<jfkey_t>::iterator current_table = builder->m_levels.at(idx)->mp_RHS_Table->get_key_iterator(builder->m_levels.at(idx)->get_index_for_LHS_table());
//
//        auto search_index = &this->m_initialIndex->m_levels.at(idx + 1)->m_data;
//
//        for (; i_idx != e_idx; ++i_idx) {
//            // search for this item in the original join graph
//            auto v_tx = search_index->find(i_idx->first);
//
//            if (v_tx == search_index->end())
//                continue;
//
//            // for each element in the 'rhs' push back
//            auto v_enu = v_tx->second->getLHSEnumerator();
//
//            while (v_enu->Step()) {
//                // validate the records with filters (if any exist)
//                bool valid = true;
//                for (int i = 0; i < m_filters.at(idx + 1).size(); ++i) {
//                    if (!m_filters.at(idx + 1).at(i)->Validate(v_enu->getRecordId())) {
//                        valid = false;
//                        break;
//                    }
//                }
//                if (valid == false)
//                    continue;
//
//                //jfkey_t n_value = builder->m_levels.at(idx)->mp_RHS_Table->get_int64(v_enu->getRecordId(), builder->m_levels.at(idx)->get_index_for_LHS_table()); // ERROR HERE
//                jfkey_t n_value = *(current_table + v_enu->getRecordId());
//
//
//                builder->m_levels.at(idx + 1)->InsertLHSRecord(v_tx->first, v_enu->getRecordId());
//                builder->m_levels.at(idx)->InsertRHSRecord(n_value, v_enu->getRecordId());
//
//                //std::cout << "RHS= " << n_value << " LHS= " << v_tx->first << " id=" << v_enu->getRecordId() << '\n';
//
//            }
//        }
//    }
//
//    // do the final loop
//    {
//        auto i_idx = builder->m_levels.at(0)->m_data.begin();
//        auto e_idx = builder->m_levels.at(0)->m_data.end();
//
//        for (; i_idx != e_idx; ++i_idx) {
//            auto v_tx = this->m_initialIndex->m_levels.at(idx + 1)->m_data.find(i_idx->first);
//
//            // for each element, verify and push in the graph if it is valid
//            auto v_enu = v_tx->second->getLHSEnumerator();
//            while (v_enu->Step()) {
//                bool valid = true;
//                for (int i = 0; i < m_filters.at(0).size(); ++i) {
//                    if (!m_filters.at(0).at(i)->Validate(v_enu->getRecordId())) {
//                        valid = false;
//                        break;
//                    }
//                }
//                if (valid == false)
//                    continue;
//
//                builder->m_levels.at(0)->InsertLHSRecord(v_tx->first, v_enu->getRecordId());
//            }
//        }
//    }
//
//
//    // Fill in all weights and optimize
//    for (int level_i = start_rebuild_index + 1; level_i > 0; --level_i)
//    {
//        //std::cout << "level_i=" << level_i << std::endl;
//        // make sure we only push back weights, not subset sum weights
//        builder->m_levels.at(level_i - 1)->fill_weight(builder->m_levels.at(level_i), builder->m_levels.at(level_i)->get_LHS_table_index());
//    }
//
//    for (int level_i = 0; level_i <= start_rebuild_index; level_i++)
//    {
//        //std::cout << "level_i=" << level_i << std::endl;
//        builder->m_levels.at(level_i)->optimize();
//    }
//
//
//    //std::cout << "builder has " << builder->m_levels.size() << " levels" << std::endl;
//    //for (int i = 0; i < builder->m_levels.size(); ++i)
//    //    std::cout << "level " << i << " has " << builder->m_levels.at(i)->GetLevelWeight() << " vs " << this->m_initialIndex->m_levels.at(i)->GetLevelWeight() << std::endl;
//
//
//    builder->start_weight = builder->m_levels[0]->GetLevelWeight();
//    builder->m_distribution = std::uniform_int_distribution<weight_t>(0, builder->start_weight);
//    std::chrono::high_resolution_clock::duration d = std::chrono::high_resolution_clock::now().time_since_epoch();
//    builder->m_generator.seed(d.count());
//
//    builder->m_levels.front()->build_starting();
//
//    return builder;
//}
//
//void jefastBuilderWNonJoinAttribSelection::AddFilter(std::shared_ptr<jefastFilter> filter, int tableNumber)
//{
//    m_filters.at(tableNumber).push_back(filter);
//}
