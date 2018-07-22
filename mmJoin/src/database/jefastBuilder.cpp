// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Implements a builder for the jefast graph

#include "jefastBuilder.h"
#include "jefastLevel.h"
#include "jefastIndex.h"

#include <random>
#include <chrono>
#include <iostream>
#include <algorithm>

JefastBuilder::JefastBuilder()
{
}

int JefastBuilder::AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex, int tableNumber){
    m_joinedTables.push_back(table);
    m_LHSJoinIndex.push_back(LHSIndex);
    m_RHSJoinIndex.push_back(RHSIndex);
    m_filters.push_back(std::vector<std::shared_ptr<jefastFilter> >());

    return int(m_joinedTables.size());
}

int JefastBuilder::AddFilter(std::shared_ptr<jefastFilter> filter, int tableNumber)
{
    m_filters.at(tableNumber).push_back(filter);
    return int(m_filters.at(tableNumber).size());
}

int JefastBuilder::AppendBuildSuggestion(int table, BuilderSuggestion::side buildSide)
{
    m_buildOrder.emplace_back(table, buildSide);
    return int(m_buildOrder.size());
}

std::shared_ptr<jefastIndexLinear> JefastBuilder::Build()
{
    std::shared_ptr<jefastIndexLinear> builder{ new jefastIndexLinear() };
    // this will track which tables we have scanned and submitted to the builder.
    std::vector<bool> scanned_table;
    scanned_table.resize(m_joinedTables.size());

    // build the level graph.
    for (int i = 0; i < m_joinedTables.size() - 1; i++) {
        std::shared_ptr<JefastLevel<jfkey_t> > value(new JefastLevel<jfkey_t>(m_joinedTables.at(i), m_joinedTables.at(i + 1), (i == m_joinedTables.size()-2)));

        
        value->set_LHS_table_index(m_LHSJoinIndex[i]);
        //if (i > 0)
        value->set_RHS_table_index(m_RHSJoinIndex[i + 1]);
            //value->set_RHS_table_index(m_RHSJoinIndex[i - 1]);

        //std::shared_ptr<JefastLevel<jfkey_t> > value(new JefastLevel<jfkey_t>());
        builder->m_levels.push_back(value);
    }

    // fill in links which have a filter in the graph (naively)
    for (int i = 0; i < m_filters.size(); i++) {
        if(m_filters.at(i).size() != 0)
            throw "Filtering on initial build unsupported";
    }

    // build items with suggestions (using indexes)
    // fill in links which have a filter in the graph (naively)
    //for (int i = 0; i < m_buildOrder.size(); i++) {
    //    if (scanned_table.at(m_buildOrder.at(i).table_id) == true)
    //        continue;

    //    int table_id = m_buildOrder.at(i).table_id;

    //    // if the side we are building from is not filled out, we ignore this request
    //    if ((m_buildOrder.at(i).build_side == BuilderSuggestion::LEFT  && !builder->m_levels[table_id    ]->isLockedVertex())
    //     || (m_buildOrder.at(i).build_side == BuilderSuggestion::RIGHT && !builder->m_levels[table_id - 1]->isLockedVertex()))
    //        continue;

    //    scanned_table.at(table_id) = true;

    //    std::unique_ptr<jefastEnumerator> enu;
    //    if (m_buildOrder.at(i).build_side == BuilderSuggestion::LEFT)
    //        enu = builder->m_levels[table_id]->GetVertexValueEnumerator();
    //    else if (m_buildOrder.at(i).build_side == BuilderSuggestion::RIGHT)
    //        enu = builder->m_levels[table_id - 1]->GetVertexValueEnumerator();

    //    std::shared_ptr< int_index > table_index;
    //    if (m_buildOrder.at(i).build_side == BuilderSuggestion::LEFT)
    //        table_index = this->m_joinedTables.at(table_id)->get_int_index(this->m_LHSJoinIndex.at(table_id));
    //    else if (m_buildOrder.at(i).build_side == BuilderSuggestion::RIGHT)
    //        table_index = this->m_joinedTables.at(table_id)->get_int_index(this->m_RHSJoinIndex.at(table_id));

    //    int LHS_column = m_LHSJoinIndex[table_id];
    //    int RHS_column = m_RHSJoinIndex[table_id];
    //    bool RHS_locked = true;
    //    bool LHS_locked = true;
    //    if (LHS_column != -1)
    //        LHS_locked = builder->m_levels[table_id]->isLockedVertex();
    //    if (RHS_column != -1)
    //        RHS_locked = builder->m_levels[table_id - 1]->isLockedVertex();
    //    // if we are filling in the last column, we can insert the weights right now.
    //    //int64_t default_weight = LHS_column == -1 ? 1 : 0;
    //    while (enu->Step()) {
    //        int64_t vertexValue = enu->getValue();
    //        auto r_range = table_index->equal_range(vertexValue);
    //        for (auto itm = r_range.first; itm != r_range.second; ++itm)
    //        {
    //            int64_t LHS_value = 0;
    //            int64_t RHS_value = 0;

    //            if (RHS_column != -1) {
    //                // if this is true we will fill in the LHS part of the join
    //                RHS_value = m_joinedTables.at(table_id)->get_int64(itm->second, RHS_column);
    //            }
    //            if (LHS_column != -1) {
    //                LHS_value = m_joinedTables.at(table_id)->get_int64(itm->second, LHS_column);
    //            }
    //            /////////////
    //            // if we can't insert new vertexes, make sure the vertexes exist before trying to create
    //            // a new link.
    //            //if (RHS_column != -1 && RHS_locked && !builder->m_levels[table_id - 1]->DoesVertexExist(RHS_value))
    //            //    continue;
    //            //if (LHS_column != -1 && LHS_locked && !builder->m_levels[table_id    ]->DoesVertexExist(LHS_value))
    //            //    continue;

    //            if (RHS_column != -1) {
    //                //builder->m_levels[table_id - 1]->InsertRHSRecord(RHS_value, itm->second, LHS_value, default_weight);
    //                builder->m_levels[table_id - 1]->InsertRHSRecord(RHS_value, itm->second);
    //            }
    //            if (LHS_column != -1) {
    //                //builder->m_levels[table_id    ]->InsertLHSRecord(LHS_value, itm->second, RHS_value);
    //                builder->m_levels[table_id]->InsertLHSRecord(LHS_value, itm->second);
    //            }
    //        }
    //    }

    //    // if we have filled out the items, we can lock vertex creation for the levels
    //    if (!LHS_locked)
    //        builder->m_levels[table_id]->LockNewVertex();
    //    if (!RHS_locked)
    //        builder->m_levels[table_id - 1]->LockNewVertex();
    //}


    // build the remaining links
    // TODO: We need to determine the best way to scan the table (using an index or
    //       doing a table scan)
    // For now we are doing a full table scan to pull the elements from the table
    //  (we assume no indexes)
    for (int i = 0; i < m_filters.size(); i++) {
        if (scanned_table.at(i) == true)
            continue;

        scanned_table.at(i) = true;

        int64_t LHS_column = m_LHSJoinIndex[i];
        int64_t RHS_column = m_RHSJoinIndex[i];

        // this section of code will eliminate building LHS edges
        //if (i != 0)
        //    LHS_column = -1;

        auto LHS_column_itr = LHS_column != -1 ? m_joinedTables.at(i)->get_key_iterator(m_LHSJoinIndex[i]) : std::vector<jfkey_t>::iterator();
        auto RHS_column_itr = RHS_column != -1 ? m_joinedTables.at(i)->get_key_iterator(m_RHSJoinIndex[i]) : std::vector<jfkey_t>::iterator();

        bool RHS_locked = true;
        bool LHS_locked = true;
        if (LHS_column != -1)
            LHS_locked = builder->m_levels[i]->isLockedVertex();
        if (RHS_column != -1)
            RHS_locked = builder->m_levels[i - 1]->isLockedVertex();
        
        int64_t row_count = m_joinedTables.at(i)->row_count();
        for (int64_t t = 0; t < row_count; ++t) {
            int64_t LHS_value = 0;
            int64_t RHS_value = 0;

            if (RHS_column != -1) {
                RHS_value = *RHS_column_itr;

                builder->m_levels[i - 1]->InsertRHSRecord(RHS_value, t);
                ++RHS_column_itr; 
            }

            if (LHS_column != -1) {
                LHS_value = *LHS_column_itr;

                builder->m_levels[i]->InsertLHSRecord(LHS_value, t);
                ++LHS_column_itr;
            }
        }
        if (!LHS_locked)
            builder->m_levels[i]->LockNewVertex();
        if (!RHS_locked)
            builder->m_levels[i - 1]->LockNewVertex();
    }

    for (size_t level_i = builder->m_levels.size() - 1; level_i > 0; --level_i)
    {
        builder->m_levels.at(level_i - 1)->fill_weight(builder->m_levels.at(level_i), builder->m_levels.at(level_i)->get_LHS_table_index());
    }

    // do an optimize phase
    for (int level_i = 0; level_i < builder->m_levels.size() - 1; level_i++)
    {
        builder->m_levels.at(level_i)->optimize();
    }

    builder->start_weight = builder->m_levels[0]->GetLevelWeight();
    builder->m_distribution = std::uniform_int_distribution<weight_t>(0, builder->start_weight);
    std::chrono::high_resolution_clock::duration d = std::chrono::high_resolution_clock::now().time_since_epoch();
    builder->m_generator.seed(d.count());

    builder->m_levels.front()->build_starting();

    return builder;
}
