// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Implements the jefast index

#include "jefastIndex.h"
#include "jefastLevel.h"

#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <random>
#include <queue>

weight_t jefastIndexLinear::GetTotal() {
    return start_weight;
}

void jefastIndexLinear::GetJoinNumber(weight_t joinNumber, std::vector<int64_t> &out) {
    // check if it is out of bounds?
    //if (joinNumber < start_weight)
    //    throw "Out of bounds";
    assert(joinNumber < start_weight);

    out.resize(this->GetNumberOfLevels());

    // find the first level item
    weight_t current_weight = joinNumber;
    //auto first_phase = this->m_levels.at(0)->GetLHSEnumerator();
    //first_phase->Step();
    //while (current_weight >= first_phase->getWeight()) {
    //    current_weight -= first_phase->getWeight();
    //    first_phase->Step();
    //}

    this->m_levels.at(0)->GetStartPairStep(current_weight, out[0], out[1]);

    //out.at(0) = first_phase->getRecordId();
    //auto value = first_phase->getVertexValue();

    // in this case, we already have selected the items needed.
    if (this->m_levels.size() == 1)
        return;


    auto value = this->m_levels.at(0)->get_RHS_Table()->get_int64(out[1], this->m_levels.at(1)->get_LHS_table_index());

    // step though each other point.
    for (int i = 1; i < this->m_levels.size(); ++i) {
        //this->m_levels.at(i)->GetNextStep(value, current_weight, out[i + 1], value);
        this->m_levels.at(i)->GetNextStep(value, current_weight, out[i + 1]);
        if(i+1 < this->m_levels.size())
            value = this->m_levels.at(i)->get_RHS_Table()->get_int64(out[i + 1], this->m_levels.at(i + 1)->get_LHS_table_index());
    }
}

std::vector<weight_t> jefastIndexLinear::GetJoinNumberWithWeights(weight_t joinNumber, std::vector<int64_t> &out) {
    // check if it is out of bounds?
    //if (joinNumber < start_weight)
    //    throw "Out of bounds";
    assert(joinNumber < start_weight);

    out.resize(this->GetNumberOfLevels());
    std::vector<weight_t> join_weights(this->GetNumberOfLevels());

    // find the first level item
    weight_t current_weight = joinNumber;
    //auto first_phase = this->m_levels.at(0)->GetLHSEnumerator();
    //first_phase->Step();
    //while (current_weight >= first_phase->getWeight()) {
    //    current_weight -= first_phase->getWeight();
    //    first_phase->Step();
    //}

    this->m_levels.at(0)->GetStartPairStep(current_weight, out[0], out[1], {0, &join_weights[0]});

    //out.at(0) = first_phase->getRecordId();
    //auto value = first_phase->getVertexValue();

    // in this case, we already have selected the items needed.
    if (this->m_levels.size() == 1)
        return join_weights;


    auto value = this->m_levels.at(0)->get_RHS_Table()->get_int64(out[1], this->m_levels.at(1)->get_LHS_table_index());

    // step though each other point.
    for (int i = 1; i < this->m_levels.size(); ++i) {
        //this->m_levels.at(i)->GetNextStep(value, current_weight, out[i + 1], value);
        this->m_levels.at(i)->GetNextStep(value, current_weight, out[i + 1], &join_weights[i]);
        if(i+1 < this->m_levels.size())
            value = this->m_levels.at(i)->get_RHS_Table()->get_int64(out[i + 1], this->m_levels.at(i + 1)->get_LHS_table_index());
    }
    return join_weights;
}

void jefastIndexLinear::GetRandomJoin(std::vector<int64_t> &out) {
    weight_t random_join_number = m_distribution(m_generator);

    return this->GetJoinNumber(random_join_number, out);
}

std::vector<weight_t> jefastIndexLinear::GetRandomJoinWithWeights(std::vector<int64_t> &out) {
    weight_t random_join_number = m_distribution(m_generator);
    std::cerr << "inside linear!!!!" << std::endl;
    return this->GetJoinNumberWithWeights(random_join_number, out);
}

std::pair<std::vector<std::vector<int64_t>>, std::vector<std::vector<uint64_t>>> jefastIndexLinear::GenerateData(size_t count)
// Generate `count` samples, along with the weights of the tuples.
{
    return {};
}

int jefastIndexLinear::GetNumberOfLevels()
{
    return int(this->m_levels.size()) + 1;
}

void jefastIndexLinear::Insert(int table_id, jefastKey_t record_id)
{
    // find the last level which we will need to adjust
    // the table will be the same as the table_id
    int level_to_edit = table_id;

    weight_t new_weight = 1;

    struct work_item
    {
        work_item(jfkey_t id, weight_t weight)
            : next_id{ id }
            , new_weight{ weight }
        {};
        //jfkey_t vertex_label;
        jfkey_t next_id;
        weight_t new_weight;
    };

    // setup work queues
    std::unique_ptr<std::queue<work_item> > current_work(new std::queue<work_item>());
    std::unique_ptr<std::queue<work_item> > next_work(new std::queue<work_item>());

    // we have to check to make sure a level exists to insert the LHS edge
    if (level_to_edit < m_levels.size())
    {
        jfkey_t LHS_value = m_levels.at(level_to_edit)->get_LHS_Table()->get_int64(record_id, m_levels.at(level_to_edit)->get_LHS_table_index());
        auto vtx = m_levels.at(level_to_edit)->getVertex(LHS_value);
        vtx->insert_lhs_record_ids(record_id);
        new_weight = vtx->getWeight();
    }
    // insert the new RHS edge and a new LHS edge between the correct levels
    if (level_to_edit > 0)
    {
        jfkey_t RHS_value = m_levels.at(level_to_edit - 1)->get_RHS_Table()->get_int64(record_id, m_levels.at(level_to_edit-1)->get_RHS_table_index());
        auto vtx = m_levels.at(level_to_edit - 1)->getVertex(RHS_value);
        weight_t next_weight = vtx->insert_rhs_record_weight_with_sum(record_id, new_weight);
        auto enu = vtx->getLHSEnumerator();
        while (enu->Step())
        {
            next_work->emplace(enu->getRecordId(), next_weight);
        }
    }

    // now we continue to step back, updating edge weight along the way
    for (int i = level_to_edit - 2; i >= 0; --i) {
        std::swap(current_work, next_work);
        while (!current_work->empty()) {
            work_item current_item = current_work->front();
            current_work->pop();

            jfkey_t RHS_value = m_levels.at(i)->get_RHS_Table()->get_int64(current_item.next_id, m_levels.at(i)->get_RHS_table_index());
            auto vtx = m_levels.at(i)->getVertex(RHS_value);
            weight_t next_weight = vtx->adjust_rhs_record_weight_with_sum(current_item.next_id, current_item.new_weight);
            auto enu = vtx->getLHSEnumerator();
            while (enu->Step())
            {
                next_work->emplace(enu->getRecordId(), next_weight);
            }
        }
    }

    // update the initial search index
    if (!postpone_rebuild) {
        std::swap(current_work, next_work);
        while (!current_work->empty()) {
            work_item current_item = current_work->front();
            current_work->pop();

            m_levels.at(0)->update_search_weight(current_item.next_id, current_item.new_weight);
        }

        start_weight = m_levels[0]->GetLevelWeight();
    }
    // and we are done!
}

void jefastIndexLinear::Delete(int table_id, jefastKey_t record_id)
{
    {
        // find the last level which we will need to adjust
        // the table will be the same as the table_id
        int level_to_edit = table_id;

        struct work_item
        {
            work_item(jfkey_t id, weight_t weight)
                : next_id{ id }
                , new_weight{ weight }
            {};
            //jfkey_t vertex_label;
            jfkey_t next_id;
            weight_t new_weight;
        };

        // setup work queues
        std::unique_ptr<std::queue<work_item> > current_work(new std::queue<work_item>());
        std::unique_ptr<std::queue<work_item> > next_work(new std::queue<work_item>());

        // we have to check to make sure a level exists to insert the LHS edge
        if (level_to_edit < m_levels.size())
        {
            jfkey_t LHS_value = m_levels.at(level_to_edit)->get_LHS_Table()->get_int64(record_id, m_levels.at(level_to_edit)->get_LHS_table_index());
            std::shared_ptr<JefastVertex> vtx = m_levels.at(level_to_edit)->getVertex(LHS_value);
            vtx->delete_lhs_record_ids(record_id);
        }
        // insert the new RHS edge and a new LHS edge between the correct levels
        if (level_to_edit > 0)
        {
            jfkey_t RHS_value = m_levels.at(level_to_edit - 1)->get_RHS_Table()->get_int64(record_id, m_levels.at(level_to_edit - 1)->get_RHS_table_index());
            auto vtx = m_levels.at(level_to_edit - 1)->getVertex(RHS_value);
            weight_t next_weight = vtx->delete_rhs_record_weight_with_sum(record_id);
            auto enu = vtx->getLHSEnumerator();
            while (enu->Step())
            {
                next_work->emplace(enu->getRecordId(), next_weight);
            }
        }

        // now we continue to step back, updating edge weight along the way
        for (int i = level_to_edit - 2; i >= 0; --i) {
            std::swap(current_work, next_work);
            while (!current_work->empty()) {
                work_item current_item = current_work->front();
                current_work->pop();

                jfkey_t RHS_value = m_levels.at(i)->get_RHS_Table()->get_int64(current_item.next_id, m_levels.at(i)->get_RHS_table_index());
                auto vtx = m_levels.at(i)->getVertex(RHS_value);
                weight_t next_weight = vtx->adjust_rhs_record_weight_with_sum(current_item.next_id, current_item.new_weight);
                auto enu = vtx->getLHSEnumerator();
                while (enu->Step())
                {
                    next_work->emplace(enu->getRecordId(), next_weight);
                }
            }
        }

        // update the initial search index
        if (!postpone_rebuild) {
            std::swap(current_work, next_work);
            while (!current_work->empty()) {
                work_item current_item = current_work->front();
                current_work->pop();

                m_levels.at(0)->update_search_weight(current_item.next_id, current_item.new_weight);
            }

            start_weight = m_levels[0]->GetLevelWeight();
        }
        // and we are done!
    }
}

std::vector<weight_t> jefastIndexLinear::MaxOutdegree()
{
    std::vector<weight_t> to_return;
    to_return.resize(this->m_levels.size());

    std::transform(this->m_levels.begin(), this->m_levels.end(), to_return.begin(), [](std::shared_ptr<JefastLevel<jfkey_t> > lvl) { return lvl->getMaxOutdegree();});

    //for (auto i : to_return)
    //    std::cout << i << std::endl;

    return to_return;
}

void jefastIndexLinear::rebuild_initial()
{
    start_weight = m_levels[0]->GetLevelWeight();
    m_levels[0]->build_starting();
}

//////////////////////////////////////////////////////
//// BELOW are implementation for jefastIndexFork ////
//////////////////////////////////////////////////////

void jefastIndexFork::GetJoinNumber(
    weight_t joinNumber,
    std::vector<int64_t> &out) {
    
    assert(joinNumber < m_start_weight);
    out.resize(GetNumberOfLevels());

    // Stores the remaining weights to be used in the subsequent levels
    // after we traverse through a fork.
    std::vector<weight_t> rem_weights(m_levels.size());
    
    // i is the next table to be sampled, set after the following
    // if.
    size_t i;
    if (m_levels[0].get()) {
        // We have a virtual level where there is just one
        // (virtual) key in it and the vertex contains all records
        // in table[0].
        rem_weights[0] = joinNumber;
        m_levels[0]->GetNextStep(
            virtual_key,
            rem_weights[0],
            out[0]);
        i = 1;
    } else {
        // We don't have a virtual level. Just use
        // GetStartPairStep() to set up the first two
        // rows simultaneously.
        rem_weights[1] = joinNumber;
        m_levels[1]->GetStartPairStep(
            rem_weights[1],
            out[0],
            out[1]);
        i = 2;
    }

    // continue through all the remaining tables
    for (; i < m_levels.size(); ++i) {
        int lhs_table_number = m_parent_tables[i];
        int lhs_column = m_levels[i]->get_LHS_table_index();
        jfkey_t value = m_levels[lhs_table_number]->get_RHS_Table()
            ->get_int64(out[lhs_table_number], lhs_column);
        if (m_is_last_child[i]) {
            // This is either a linear child or the last
            // child in a fork. Use GetNextStep() as usual,
            // which saves a modulo op.
            rem_weights[i] = rem_weights[lhs_table_number];
            m_levels[i]->GetNextStep(value, rem_weights[i], out[i]);
        } else {
            // This is some child other than the last in a fork.
            // Use GetNextStepThroughFork() to correctly set
            // the rem_weight of the parent table and the child table.
            m_levels[i]->GetNextStepThroughFork(
                value,
                rem_weights[lhs_table_number], /* parent_weight */
                rem_weights[i], /* my_weight */
                out[i]);
        }
    }
}

std::vector<weight_t> jefastIndexFork::GetJoinNumberWithWeights(
    weight_t joinNumber,
    std::vector<int64_t> &out) {
    
    std::cerr << "#levels=" << GetNumberOfLevels() << std::endl;

    assert(joinNumber < m_start_weight);
    out.resize(GetNumberOfLevels());
    std::vector<weight_t> join_weights(GetNumberOfLevels());
    std::cerr << "[GetJoinNumberWithWeights] join_weights.size()=" << join_weights.size() << std::endl;

    // Stores the remaining weights to be used in the subsequent levels
    // after we traverse through a fork.
    std::vector<weight_t> rem_weights(m_levels.size());
    
    // i is the next table to be sampled, set after the following
    // if.
    size_t i;
    // TODO: what to do with weight(0)? Or is the entire vector shifted by one?
    if (m_levels[0].get()) {
        std::cerr << "first case" << std::endl;
        // We have a virtual level where there is just one
        // (virtual) key in it and the vertex contains all records
        // in table[0].
        rem_weights[0] = joinNumber;
        m_levels[0]->GetNextStep(
            virtual_key,
            rem_weights[0],
            out[0],
            &join_weights[0]);
        i = 1;
    } else {
        std::cerr << "second case" << std::endl;
        // We don't have a virtual level. Just use
        // GetStartPairStep() to set up the first two
        // rows simultaneously.
        rem_weights[1] = joinNumber;
        m_levels[1]->GetStartPairStep(
            rem_weights[1],
            out[0],
            out[1],
            {1, &join_weights[1]});
        i = 2;
    }
    std::cerr << "i=" << i << " size=" << m_levels.size() << std::endl;
#if 1
    // continue through all the remaining tables
    for (; i < m_levels.size(); ++i) {
        std::cerr << "[GetJoinNumberWithWeights] main loop i=" << i << std::endl;
        std::cerr << "!!!!!!!!!!!!!!!!! insideeeee" << std::endl;
        int lhs_table_number = m_parent_tables[i];
        int lhs_column = m_levels[i]->get_LHS_table_index();
        jfkey_t value = m_levels[lhs_table_number]->get_RHS_Table()
            ->get_int64(out[lhs_table_number], lhs_column);
        if (m_is_last_child[i]) {
            std::cerr << "\t [Main Loop] first branch!" << std::endl;
            // This is either a linear child or the last
            // child in a fork. Use GetNextStep() as usual,
            // which saves a modulo op.
            rem_weights[i] = rem_weights[lhs_table_number];
            std::cerr << "\t[before GetNextStep]" << std::endl;
            m_levels[i]->GetNextStep(value, rem_weights[i], out[i], &join_weights[i]);
            std::cerr << "\t[after GetNextStep]" << std::endl;
            std::cerr << "i=" << i << " w=" << rem_weights[i] << std::endl;
        
            // TODO: is this the correct weight?
            // TODO: is it before `GetNextStep`?
            join_weights[i] = rem_weights[i];
        } else {
            std::cerr << "\t [Main Loop] second branch!" << std::endl;
            // This is some child other than the last in a fork.
            // Use GetNextStepThroughFork() to correctly set
            // the rem_weight of the parent table and the child table.
            m_levels[i]->GetNextStepThroughFork(
                value,
                rem_weights[lhs_table_number], /* parent_weight */
                rem_weights[i], /* my_weight */
                out[i],
                &join_weights[i]);
            std::cerr << "i=" << i << " w=" << rem_weights[i] << std::endl;
            // TODO: is this the correct weight?
            join_weights[i] = rem_weights[i];
        }
    }
#endif
    std::cerr << "[final return] join_weights.size()=" << join_weights.size() << std::endl;
    std::cerr << "pointer=" << &join_weights << std::endl;
    return join_weights;
}

void jefastIndexFork::GetRandomJoin(std::vector<int64_t> &out) {
    weight_t random_join_number = m_distribution(m_generator);
    return this->GetJoinNumber(random_join_number, out);
}

std::vector<weight_t> jefastIndexFork::GetRandomJoinWithWeights(std::vector<int64_t> &out) {
    weight_t random_join_number = m_distribution(m_generator);
    return this->GetJoinNumberWithWeights(random_join_number, out);
}

std::pair<std::vector<std::vector<int64_t>>, std::vector<std::vector<uint64_t>>> jefastIndexFork::GenerateData(size_t count)
// Generate `count` samples, along with the weights of the tuples.
{
    // Convert `weight_t` to `uint64_t`.
    auto convert = [&](std::vector<weight_t>& ws) {
        std::vector<uint64_t> transformed;
        std::transform(ws.begin(), ws.end(), std::back_inserter(transformed), [](auto w) {
            std::ostringstream stream; stream << w;
            return static_cast<uint64_t>(std::stoull(stream.str())); 
        });
        return std::move(transformed);
    };

    // And sample.
    std::vector<std::vector<int64_t>> sampled(count);
    std::vector<std::vector<uint64_t>> weights(count);
    for (size_t index = 0; index != count; ++index) {
        std::vector<int64_t> out;
        auto ws = GetRandomJoinWithWeights(out);
        sampled[index] = std::move(out);
        weights[index] = convert(ws);
    }
    return {sampled, weights};
}

