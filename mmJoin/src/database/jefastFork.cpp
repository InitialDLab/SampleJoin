#include "jefastFork.h"

//bool JefastFork2Way::InsertLHSRecord(const Fork2way_t &value, jfkey_t LHS_recordId, jfkey_t prev_value)
//{
//    auto search = m_data.find(value);
//    if (search != m_data.end()) {
//        search->second.m_matching_lhs_record_ids.push_back(LHS_recordId);
//        search->second.m_matching_lhs_record_prev_value.push_back(prev_value);
//        return true;
//    }
//    else {
//        auto insertedValue = m_data.emplace(value, true);
//        insertedValue.first->second.m_matching_lhs_record_ids.push_back(LHS_recordId);
//        insertedValue.first->second.m_matching_lhs_record_prev_value.push_back(prev_value);
//        return true;
//    }
//}
//
//bool JefastFork2Way::InsertRHSRecord(int branch, jfkey_t value, jfkey_t RHS_recordId)
//{
//    std::map<jfkey_t, JefastVertex>* branchptr;
//    switch (branch) {
//    case 1:
//        branchptr = &m_dataBranch1;
//        break;
//    case 2:
//        branchptr = &m_dataBranch2;
//        break;
//    default:
//        throw "NON-EXISTANT BRANCH";
//    }
//
//    auto search = branchptr->find(value);
//    if (search != branchptr->end()) {
//        //search->second.insert_rhs_record_ids(RHS_recordId, 0, 1);
//        search->second.insert_rhs_record_ids(RHS_recordId);
//        return true;
//    }
//    else {
//        auto insertedValue = branchptr->emplace(value, true);
//        //insertedValue.first->second.insert_rhs_record_ids(RHS_recordId, 0, 1);
//        insertedValue.first->second.insert_rhs_record_ids(RHS_recordId);
//        return true;
//    }
//}
//
//weight_t JefastFork2Way::GetBranchWeight(int branch)
//{
//    throw "NOT IMPLEMENTED";
//}
//
//void JefastFork2Way::GetNextStep(const Fork2way_t &id, weight_t & inout_weight, std::vector<int64_t>& out_indexes, size_t & out_starting_index)
//{
//    // TODO: check that the fork id is valid?
//
//    // for doing a forked next step, the weight of the first branch takes priority.
//    JefastVertex *itr1 = &m_dataBranch1.at(std::get<0>(id));
//    JefastVertex *itr2 = &m_dataBranch2.at(std::get<1>(id));
//    weight_t branch1_weight = inout_weight % itr1->getWeight();
//    weight_t branch2_weight = inout_weight / itr1->getWeight();
//
//    itr1->get_records(branch1_weight, out_indexes[out_starting_index]);
//    itr2->get_records(branch2_weight, out_indexes[out_starting_index + 1]);
//}
