#include "query9_jefast.h"

#include "../database/TableLineitem.h"

int64_t query9_jefast::GetTotal()
{
    return start_weight;
}

void query9_jefast::GetJoinNumber(int64_t joinNumber, std::vector<int64_t>& out)
{
    assert(joinNumber < start_weight);

    out.resize(this->GetNumberOfLevels());

    // find the first level
    weight_t current_weight = joinNumber;
    std::unique_ptr<JefastLevelEnumerator> first_phase = mp_first->GetLHSEnumerator();
    first_phase->Step();
    while (current_weight >= first_phase->getWeight()) {
        current_weight -= first_phase->getWeight();
        first_phase->Step();
    }

    // find the second level
    out[0] = first_phase->getRecordId();
    auto value1 = first_phase->getVertexValue();

    mp_first->GetNextStep(value1, current_weight, out[1]);
    value1 = mp_second->get_LHS_Table()->get_int64(out[1], mp_second->get_LHS_table_index());
    mp_second->GetNextStep(value1, current_weight, out[2]);

    query9ForkKey_t value2(mp_second->get_RHS_Table()->get_int64(out[2], Table_Lineitem::L_PARTKEY)
                         , mp_second->get_RHS_Table()->get_int64(out[2], Table_Lineitem::L_SUPPKEY)
                         , mp_second->get_RHS_Table()->get_int64(out[2], Table_Lineitem::L_ORDERKEY));


    mp_final->GetNextStep(value2, current_weight, out, 3);
}

void query9_jefast::GetRandomJoin(std::vector<int64_t>& out)
{
    int64_t random_join_number = m_distribution(m_generator);

    return this->GetJoinNumber(random_join_number, out);
}

int query9_jefast::GetNumberOfLevels()
{
    return 6;
}

int query9_jefast::getOlkinCoeffecient()
{
    // after observing how the query works, everything fanning out from
    // the final stage should have max cardinality=1
    return mp_first->getMaxOutdegree() * mp_second->getMaxOutdegree();
}
