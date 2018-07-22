#include "query9_Fork.h"

bool JefastQuery9Fork::InsertLHSRecord(const query9ForkKey_t & value, jfkey_t LHS_recordId, jfkey_t prev_value)
{
    //auto search = m_data.find(value);
    //if (search != m_data.end()) {
    //    search->second.m_matching_lhs_record_ids.push_back(LHS_recordId);
    //    search->second.m_matching_lhs_record_prev_value.push_back(prev_value);
    //}
    //else {
    //    auto insertedValue = m_data.emplace(value, true);
    //    insertedValue.first->second.m_matching_lhs_record_ids.push_back(LHS_recordId);
    //    insertedValue.first->second.m_matching_lhs_record_prev_value.push_back(prev_value);
    //}
    return true;
}

bool JefastQuery9Fork::InsertRHSRecord(Branch branch, jfkey_t value, jfkey_t RHS_recordId)
{
    std::map<jfkey_t, JefastVertex >* branchptr;
    switch (branch) {
    case Branch::ORDERS:
        branchptr = &this->m_ordersData;
        break;
    case Branch::PART:
        branchptr = &this->m_partData;
        break;
    default:
        throw "CAN\'T INSERT TO PROVIDED BRANCH";
        break;
    }

    auto search = branchptr->find(value);
    if (search != branchptr->end()) {
        search->second.insert_rhs_record_ids(RHS_recordId);
        return true;
    }
    else {
        auto insertedValue = branchptr->emplace(value, true);
        insertedValue.first->second.insert_rhs_record_ids(RHS_recordId);
        return true;
    }
}

bool JefastQuery9Fork::InsertRHSRecord(Branch branch, const query9PartsuppKey_t & value, jfkey_t RHS_recordId)
{
    std::map<query9PartsuppKey_t, JefastVertex >* branchptr;
    switch (branch) {
    case Branch::PARTSUPP:
        branchptr = &this->m_partsuppData;
        break;
    default:
        throw "CAN\'T INSERT TO PROVIDED BRANCH";
        break;
    }

    auto search = branchptr->find(value);
    if (search != branchptr->end()) {
        search->second.insert_rhs_record_ids(RHS_recordId);
        return true;
    }
    else {
        auto insertedValue = branchptr->emplace(value, true);
        insertedValue.first->second.insert_rhs_record_ids(RHS_recordId);
        return true;
    }
}

void JefastQuery9Fork::GetNextStep(const query9ForkKey_t & id, weight_t & inout_weight, std::vector<int64_t>& out_indexes, size_t out_starting_index)
{
    // TODO: Check that the fork id is valid?

    // when doing the GetNextStep we will place out indexes in the following order
    // (also, weights will be adjusted according to the order of this list):
    // 1 - partsupp
    // 2 - orders
    // 3 - parts
    query9PartsuppKey_t partsuppkey(std::get<PARTKEY_IDX>(id), std::get<SUPKEY_IDX>(id));
    JefastVertex *itr1 = &this->m_partsuppData.at(partsuppkey);
    JefastVertex *itr2 = &this->m_ordersData.at(std::get<ORDERKEY_IDX>(id));
    JefastVertex *itr3 = &this->m_partData.at(std::get<PARTKEY_IDX>(id));

    weight_t partsupp_wmax = itr1->getWeight();
    weight_t orders_wmax = itr2->getWeight();

    weight_t partsupp_weight = inout_weight % partsupp_wmax;
    weight_t orders_weight = (inout_weight / partsupp_wmax) % orders_wmax;
    weight_t parts_weight = (inout_weight / (partsupp_wmax * orders_wmax));

    //jfkey_t empty;
    itr1->get_records(partsupp_weight, out_indexes[out_starting_index]);
    itr2->get_records(orders_weight, out_indexes[out_starting_index + 1]);
    itr3->get_records(parts_weight, out_indexes[out_starting_index + 2]);
}

void JefastQuery9Fork::propegate_weights_internal()
{
    throw "unsupported function";
}

void JefastQuery9Fork::propegate_weights_prev(std::shared_ptr<JefastLevel<jfkey_t> > prev_level)
{
    auto iter = prev_level->GetRHSEnumerator();

    while (iter->Step()) {
        iter->setWeight(this->get_weight(iter->getRecordId()));
    }
}

weight_t JefastQuery9Fork::get_weight(const query9ForkKey_t & id)
{
    query9PartsuppKey_t partsuppkey(std::get<PARTKEY_IDX>(id), std::get<SUPKEY_IDX>(id));
    auto partsuppData_ptr = this->m_partsuppData.find(partsuppkey);
    auto orderData_ptr = this->m_ordersData.find(std::get<ORDERKEY_IDX>(id));
    auto partData_ptr = this->m_partData.find(std::get<PARTKEY_IDX>(id));

    weight_t weight1 = (partsuppData_ptr == this->m_partsuppData.end()) ? 0 : partsuppData_ptr->second.getWeight();
    weight_t weight2 = (orderData_ptr == this->m_ordersData.end()) ? 0 : orderData_ptr->second.getWeight();
    weight_t weight3 = (partData_ptr == this->m_partData.end()) ? 0 : partData_ptr->second.getWeight();

    return weight1 * weight2 * weight3;
}

weight_t JefastQuery9Fork::get_weight(const jfkey_t record_id)
{
    jfkey_t partkey  = mp_lineitemTable->get_int64(record_id, Table_Lineitem::L_PARTKEY);
    jfkey_t suppkey  = mp_lineitemTable->get_int64(record_id, Table_Lineitem::L_SUPPKEY);
    jfkey_t orderkey = mp_lineitemTable->get_int64(record_id, Table_Lineitem::L_ORDERKEY);

    query9PartsuppKey_t partsuppkey(partkey, suppkey);
    auto partsuppData_ptr = this->m_partsuppData.find(partsuppkey);
    auto orderData_ptr = this->m_ordersData.find(orderkey);
    auto partData_ptr = this->m_partData.find(partkey);

    weight_t weight1 = (partsuppData_ptr == this->m_partsuppData.end()) ? 0 : partsuppData_ptr->second.getWeight();
    weight_t weight2 = (orderData_ptr == this->m_ordersData.end()) ? 0 : orderData_ptr->second.getWeight();
    weight_t weight3 = (partData_ptr == this->m_partData.end()) ? 0 : partData_ptr->second.getWeight();

    return weight1 * weight2 * weight3;
}
