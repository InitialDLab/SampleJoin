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

JefastBuilder::JefastBuilder():
    m_has_fork(false)
{
}

int JefastBuilder::AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex, int /* unused */ ){
    if (m_has_fork) {
        return -1;
    }

    m_joinedTables.push_back(table);
    m_LHSJoinIndex.push_back(LHSIndex);
    m_RHSJoinIndex.push_back(RHSIndex);
    m_filters.push_back(std::vector<std::shared_ptr<jefastFilter> >());

    return int(m_joinedTables.size()) - 1;
}

int JefastBuilder::AddTableToFork(
    std::shared_ptr<Table> table,
    int thisTableJoinColIndex,
    int prevTableJoinColIndex,
    int prevTableNumber) {
    if (!m_has_fork) {
        if (m_joinedTables.size() > 0) {
            // Fill in the parent table number vec for
            // all previously added tables.
            // Also need to right shift LHSJoinIndex by 1 because
            // the lhs join index is now associated with the child
            // table.
            assert(m_parentTableNumber.empty());
            m_parentTableNumber.resize(m_joinedTables.size());
            for (unsigned i = m_joinedTables.size() - 1; i > 0; --i) {
                m_LHSJoinIndex[i] = m_LHSJoinIndex[i - 1]; 
                m_parentTableNumber[i] = i - 1;
            }
            m_LHSJoinIndex[0] = -1;
            m_RHSJoinIndex[0] = -1;
            m_parentTableNumber[0] = -1;
        }
        m_has_fork = true;            
    }
    
    int thisTableNumber = (int) m_joinedTables.size();
    if (thisTableNumber >= 1 && prevTableNumber >= thisTableNumber) {
        return -1;
    }
    m_joinedTables.push_back(table);
    m_LHSJoinIndex.push_back(prevTableJoinColIndex);
    m_RHSJoinIndex.push_back(thisTableJoinColIndex);
    m_parentTableNumber.push_back(prevTableNumber);
    return thisTableNumber;
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
    if (m_has_fork) return nullptr; 

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
    builder->m_distribution = std::uniform_int_distribution<weight_t>(0, builder->start_weight - 1);
    std::chrono::high_resolution_clock::duration d = std::chrono::high_resolution_clock::now().time_since_epoch();
    builder->m_generator.seed(d.count());

    builder->m_levels.front()->build_starting();

    return builder;
}

std::shared_ptr<jefastIndexFork> JefastBuilder::BuildFork() {
    if (!m_has_fork) return nullptr;
    
    auto index = std::make_shared<jefastIndexFork>();
    
    // TODO filters are not supported yet
    if (std::any_of(m_filters.begin(),m_filters.end(),
        [](const std::vector<std::shared_ptr<jefastFilter>> &v) -> bool {
            return !v.empty();
        })) {
        throw "Filtering on initial build unsupported";
    }
    
    // calculate child table numbers
    std::vector<std::vector<int>> child_table_numbers(
        m_joinedTables.size());
    for (unsigned i = 1; i < m_joinedTables.size(); ++i) {
        child_table_numbers[m_parentTableNumber[i]].push_back(i);
    }

    // We need a virtual level that connects one tuple to
    // all the tuples in the first table if the first table has
    // more than one child table. Upon query, we should call
    // JefastLevel::GetNextStep() to find the first record ID.
    // Otherwise, we should build just one fewer level than
    // the number of tables in the query and call
    // JefastLevel::GetStartPairStep() to set up the first
    // two records at once.
    bool has_virtual_level = child_table_numbers[0].size() > 1;
    
    // create the level graphs
    if (has_virtual_level) {
        auto level = std::make_shared<JefastLevel<jfkey_t>>(
            nullptr,
            m_joinedTables[0],
            child_table_numbers[0].empty());
        index->m_levels.push_back(level);
    } else {
        index->m_levels.push_back(nullptr);
    }
    for (unsigned i = 1; i < m_joinedTables.size(); ++i) {
        int lhs_table_number = m_parentTableNumber[i];
        auto level = std::make_shared<JefastLevel<jfkey_t>>(
            m_joinedTables[lhs_table_number], /* LHS_table */
            m_joinedTables[i], /* RHS_table */
            child_table_numbers[i].empty() /* useDefaultWeigtht */ );
        level->set_LHS_table_index(m_LHSJoinIndex[i]);
        level->set_RHS_table_index(m_RHSJoinIndex[i]);

        index->m_levels.push_back(level);
    }

    // insert the records into the levels
    // We only insert the rhs into the levels as we don't
    // need to handle insert/delete in the index.
    if (has_virtual_level) {
        auto level = index->m_levels[0];
        assert(level.get());

        int64_t row_count =  m_joinedTables[0]->row_count();
        for (int64_t t = 0; t < row_count; ++t) {
            // We use the same key in the virtual level because
            // there is not an actual join pred for the virutal level.
            level->InsertRHSRecord(virtual_key, t);
        }
        // Add this fake record to ensure GetLevelWeight() works
        // correctly.
        level->InsertLHSRecord(virtual_key, 0);
    }
    for (unsigned i = 1; i < m_joinedTables.size(); ++i) {
        auto level = index->m_levels[i];
        int64_t rhs_index = m_RHSJoinIndex[i];
        int rhs_table_number = i;
        auto rhs_column_iter = m_joinedTables[rhs_table_number]
            ->get_key_iterator(rhs_index);
        
        int64_t row_count = m_joinedTables[rhs_table_number]->row_count();
        for (int64_t t = 0; t < row_count; ++t) {
            level->InsertRHSRecord(*rhs_column_iter, t);
            ++rhs_column_iter;
        }
        
        if (!has_virtual_level && i == 1) {
            // Don't copy the lhs column as it is not needed anyway
            // without insert/delete, unless this is level 1 and
            // there is no virtual level, where we need the lhs records
            // for build_starting() to work;
            int lhs_table_number = 0;
            int64_t lhs_index = m_LHSJoinIndex[1];
            auto lhs_column_iter = m_joinedTables[lhs_table_number]
                ->get_key_iterator(lhs_index);

            int64_t row_count = m_joinedTables[lhs_table_number]->row_count();
            for (int64_t t = 0; t < row_count; ++t) {
                level->InsertLHSRecord(*lhs_column_iter, t);
                ++lhs_column_iter;
            }
        }
    }

    // build weights
    for (size_t i = m_joinedTables.size(); i > 0;) {
        if (!index->m_levels[--i].get()) {
            // only happens when we don't have a virtual level
            // and i == 0
            continue;
        }
        if (child_table_numbers[i].size() == 0) {
            // a leaf in the query graph, which has default weights
            continue;
        }
        
        std::vector<std::shared_ptr<JefastLevel<jfkey_t>>> nextLevels;
        std::vector<int> nextLevelIndexes;
        nextLevels.reserve(child_table_numbers.size());
        nextLevelIndexes.reserve(child_table_numbers.size());
        for (int child_table_number : child_table_numbers[i]) {
            nextLevels.push_back(index->m_levels[child_table_number]);
            nextLevelIndexes.push_back(m_LHSJoinIndex[child_table_number]);
        }
        
        index->m_levels[i]->fill_weight_fork(
            nextLevels,
            nextLevelIndexes);
    }
    
    // optimize phase.
    //
    // Those with default weights shouldn't (and can't) be optimized
    // and that condition is now added in JefastLevel.
    if (has_virtual_level) {
        index->m_levels[0]->optimize<false>();
    }
    for (size_t i = 1; i < m_joinedTables.size(); ++i) {
            index->m_levels[i]->optimize();
    }
    
    index->m_start_weight =
        index->m_levels[(has_virtual_level) ? 0 : 1]
            ->GetLevelWeight();
    index->m_distribution = std::uniform_int_distribution<weight_t>(
            0, index->m_start_weight - 1);
    std::chrono::high_resolution_clock::duration d =
        std::chrono::high_resolution_clock::now().time_since_epoch();
    index->m_generator.seed(d.count());
    index->m_parent_tables = m_parentTableNumber;
    
    // index->m_is_last_child[0] will never be referenced so we
    // don't bother to set it.
    index->m_is_last_child.resize(m_joinedTables.size(), false);
    for (size_t i = 1; i < m_joinedTables.size(); ++i) {
        if (child_table_numbers[m_parentTableNumber[i]].back() == i) {
            index->m_is_last_child[i] = true; 
        }
    }

    if (!has_virtual_level) {
        index->m_levels[1]->build_starting();
    }

    return index;
}

