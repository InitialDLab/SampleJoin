//#include "jefastBuilderWJoinAttribSelection.h"
//
//#include <random>
//#include <chrono>
//#include <iostream>
//#include <algorithm>
//
//std::shared_ptr<jefastIndexLinear> jefastBuilderWJoinAttribSelection::Build()
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
//
//    for (;idx > 0; --idx)
//    {
//        // we have to do an adjustment to get the table for this join
//        if (this->m_filters.at(idx + 1).set != 0)
//            break;
//
//        builder->m_levels.at(idx) = this->m_initialIndex->m_levels.at(idx);
//    }
//
//    // TODO: copy back until filtering is needed
//    //for (; (idx > 1); --idx)
//    //    builder->m_levels.at(idx) = this->m_initialIndex->m_levels.at(idx);
//
//    // 3 - we continue to push back.  we check filtering conditions as we continue
//    // to push back, until we reach the beginning
//
//    // we do a ranged query the first time.  Otherwise we can just go to the first and end
//    bool first_loop = true;
//    start_rebuild_index = idx;
//
//    for (; idx >= 0; --idx) {
//        builder->m_levels.at(idx).reset(new JefastLevel<jfkey_t>(this->m_initialIndex->m_levels.at(idx)->get_LHS_Table(),
//            this->m_initialIndex->m_levels.at(idx)->get_RHS_Table(),
//            false));
//        builder->m_levels.at(idx)->set_LHS_table_index(this->m_initialIndex->m_levels.at(idx)->get_LHS_table_index());
//        builder->m_levels.at(idx)->set_RHS_table_index(this->m_initialIndex->m_levels.at(idx)->get_RHS_table_index());
//
//        // traverse the section of interest in the prev level?
//
//        auto i_idx = first_loop ? builder->m_levels.at(idx + 1)->m_data.lower_bound(this->m_filters.at(idx + 1).min)
//                                   : builder->m_levels.at(idx + 1)->m_data.begin();
//        auto e_idx = first_loop ? builder->m_levels.at(idx + 1)->m_data.upper_bound(this->m_filters.at(idx + 1).max)
//                                   : builder->m_levels.at(idx + 1)->m_data.end();
//        //ranged_query = false;
//
//        std::vector<jfkey_t>::iterator current_table = builder->m_levels.at(idx + 1)->mp_LHS_Table->get_key_iterator(builder->m_levels.at(idx)->get_LHS_table_index());
//
//        for (; i_idx != e_idx; ++i_idx) {
//            //search for the vertex in the original join graph, so we can look at the 'rhs' values
//            //std::cout << "looking for " << i_idx->first << std::endl;
//            auto v_tx = this->m_initialIndex->m_levels.at(idx + 1)->m_data.find(i_idx->first);
//            //std::cout << "found it, with weight = " << i_idx->second.getWeight();
//
//            // for each element in the 'rhs' push back
//            auto v_enu = v_tx->second->getLHSEnumerator();
//
//            //int cnt = 0;
//            while (v_enu->Step()) {
//                //++cnt;
//
//                //std::cout << "id=" << v_enu->getRecordId() << std::endl;
//                //jfkey_t n_value = *(current_table + v_enu->getRecordId());
//                //jfkey_t n_value = builder->m_levels.at(idx + 1)->mp_LHS_Table->get_int64(v_enu->getRecordId(), builder->m_levels.at(idx + 1)->get_LHS_table_index());
//                jfkey_t n_value = builder->m_levels.at(idx)->mp_RHS_Table->get_int64(v_enu->getRecordId(), builder->m_levels.at(idx)->get_index_for_LHS_table());
//                if (n_value >= this->m_filters.at(idx).min && n_value <= this->m_filters.at(idx).max) {
//                    if(!first_loop)
//                        builder->m_levels.at(idx + 1)->InsertLHSRecord(v_tx->first, v_enu->getRecordId());
//                    builder->m_levels.at(idx)->InsertRHSRecord(n_value, v_enu->getRecordId());
//                    // should we also set the weight?
//                }
//            }
//        }
//
//        first_loop = false;
//    }
//
//    // do a final loop, to just copy the incoming values for the first group.
//    {
//        auto i_idx = first_loop ? builder->m_levels.at(0)->m_data.lower_bound(this->m_filters.at(0).min)
//            : builder->m_levels.at(0)->m_data.begin();
//        auto e_idx = first_loop ? builder->m_levels.at(0)->m_data.upper_bound(this->m_filters.at(0).max)
//            : builder->m_levels.at(0)->m_data.end();
//
//        for (; i_idx != e_idx; ++i_idx) {
//            //search for the vertex in the original join graph, so we can look at the 'rhs' values
//            auto v_tx = this->m_initialIndex->m_levels.at(idx + 1)->m_data.find(i_idx->first);
//
//            // for each element in the 'rhs' push back
//            auto v_enu = v_tx->second->getLHSEnumerator();
//            while (v_enu->Step()) {
//               builder->m_levels.at(0)->InsertLHSRecord(v_tx->first, v_enu->getRecordId());
//            }
//        }
//    }
//
//    // 4 - fill in all weights and optimize.
//    for (int level_i = start_rebuild_index + 1; level_i > 0; --level_i)
//    //for (size_t level_i = builder->m_levels.size() - 1; level_i > 0; --level_i)
//    {
//        // make sure we only push back weights, not subset sum weights
//        builder->m_levels.at(level_i - 1)->fill_weight(builder->m_levels.at(level_i), builder->m_levels.at(level_i)->get_LHS_table_index());
//    }
//
//    for (int level_i = 0; level_i <= start_rebuild_index; level_i++)
//    //for (int level_i = 0; level_i < builder->m_levels.size(); level_i++)
//    {
//        builder->m_levels.at(level_i)->optimize();
//    }
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
//void jefastBuilderWJoinAttribSelection::AddFilter(jfkey_t min, jfkey_t max, int tableNumber)
//{
//    m_filters.at(tableNumber).min = min;
//    m_filters.at(tableNumber).max = max;
//    m_filters.at(tableNumber).set = true;
//}
