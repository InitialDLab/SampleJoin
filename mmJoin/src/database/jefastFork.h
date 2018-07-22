// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Implement a 'fork' in the jefast join tree
#pragma once

#include <tuple>
#include <map>
#include <vector>

#include "DatabaseSharedTypes.h"
#include "jefastVertex.h"

typedef std::tuple<jfkey_t, jfkey_t> Fork2way_t;

//class JefastFork2Way {
//public:
//    JefastFork2Way()
//    { };
//
//    bool InsertLHSRecord(const Fork2way_t& value, jfkey_t LHS_recordId, jfkey_t prev_value);
//
//    // inserting a RHS record will involve specifying which branch it will take
//    // (branch 1 or branch 2)
//    // since this is the last item in the jefast, we know all entries will have
//    // weight=1, unless it is another jefast, in that case we will have to
//    // lookup the weight.  However, this implementation does not currently
//    // support chaining jefast.
//    bool InsertRHSRecord(int branch, jfkey_t value, jfkey_t RHS_recordId);
//
//    // return the weight associated with a particular branch
//    weight_t GetBranchWeight(int branch);
//
//    // used when traversing the level to lookup a join result
//    void GetNextStep(const Fork2way_t &id, weight_t &inout_weight, std::vector<int64_t> &out_indexes, size_t &out_starting_index);
//private:
//
//    struct LHS_Information {
//        LHS_Information()
//        {};
//        // dummy constructor to simplify using emplace.
//        LHS_Information(bool)
//        {};
//
//        // the outgoing weight must be set at a later time, used for pushback?
//        weight_t m_outgoing_weight;
//        std::vector<jfkey_t> m_matching_lhs_record_ids;
//        std::vector<jfkey_t> m_matching_lhs_record_prev_value;
//    };
//
//    // the map is setup so a pair defines the location where we go.  since this is a 'fork'
//    // we also need to store two different pairs for the vertex.  Each entry in the second 
//    // tuple is independent from the other entry of the tuple.
//    std::map<Fork2way_t, LHS_Information > m_data;
//
//    std::map<jfkey_t, JefastVertex > m_dataBranch1;
//    std::map<jfkey_t, JefastVertex > m_dataBranch2;
//
//    //weight_t m_weight;
//};