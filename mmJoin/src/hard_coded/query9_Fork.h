// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Special implementation for fork for query 9
#pragma once

#include <tuple>
#include <map>
#include <vector>

#include "../database/jefastVertex.h"
#include "../database/jefastLevel.h"

#include "../database/TableOrders.h"
#include "../database/TablePartsupp.h"
#include "../database/TablePart.h"
#include "../database/TableLineitem.h"

//struct query9ForkKey {
//    jfkey_t partkey;
//    jfkey_t supkey;
//    jfkey_t order;
//};

// tuple is used to make it more easy to get it working with map
typedef std::tuple<jfkey_t, jfkey_t> query9PartsuppKey_t;
typedef std::tuple<jfkey_t, jfkey_t, jfkey_t> query9ForkKey_t;
#define PARTKEY_IDX 0
#define SUPKEY_IDX 1
#define ORDERKEY_IDX 2

class JefastQuery9Fork {
public:
    JefastQuery9Fork(std::shared_ptr<Table> lineitemTable)
        : mp_lineitemTable{ lineitemTable }
    { };

    enum Branch {PARTSUPP, ORDERS, PART};

    bool InsertLHSRecord(const query9ForkKey_t& value, jfkey_t LHS_recordId, jfkey_t prev_value);

    bool InsertRHSRecord(Branch branch, jfkey_t value, jfkey_t RHS_recordId);
    bool InsertRHSRecord(Branch branch, const query9PartsuppKey_t& value, jfkey_t RHS_recordId);

    void GetNextStep(const query9ForkKey_t &id, weight_t &inout_weight, std::vector<int64_t> &out_indexes, size_t out_starting_index);

    void propegate_weights_internal();

    void propegate_weights_prev(std::shared_ptr<JefastLevel<jfkey_t> > prev_level);

    weight_t get_weight(const query9ForkKey_t &id);

    // given a record if for the lineitem table, look up the values and return the weights
    weight_t get_weight(const jfkey_t record_id);
private:

    std::shared_ptr<Table> mp_lineitemTable;

    std::map<query9PartsuppKey_t, JefastVertex > m_partsuppData;

    // A simple optimization that can be done is to change vertex to change the
    // way it handles weights.  down all paths here we know the weight=1 and
    // we do not need a next pointer, because we know it will always point to nowhere
    // (because this is the last element)
    std::map<jfkey_t, JefastVertex > m_ordersData;
    std::map<jfkey_t, JefastVertex > m_partData;

    const int m_partssuppIndex1 = Table_Partsupp::PS_PARTKEY;
    const int m_partssuppIndex2 = Table_Partsupp::PS_PARTKEY;

    const int m_orderDataIndex = Table_Orders::O_ORDERKEY;
    const int m_partDataIndex = Table_Part::P_PARTKEY;
};