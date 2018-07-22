// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Implements a special implementation for query 9
#pragma once

#include <map>
#include <memory>
#include <tuple>
#include <random>

#include "query9_Fork.h"
#include "../database/Table.h"
#include "../database/DatabaseSharedTypes.h"
#include "../database/jefastLevel.h"
#include "../database/jefastVertex.h"
#include "../database/jefastIndex.h"

class query9_jefast : public jefastIndexBase{
public:
    virtual ~query9_jefast()
    {};

    int64_t GetTotal();

    void GetJoinNumber(int64_t joinNumber, std::vector<int64_t> &out);

    void GetRandomJoin(std::vector<int64_t> &out);

    int GetNumberOfLevels();

    int getOlkinCoeffecient();

private:
    query9_jefast()
    {};

    std::shared_ptr<JefastLevel<jfkey_t> > mp_first;
    std::shared_ptr<JefastLevel<jfkey_t> > mp_second;
    std::shared_ptr<JefastQuery9Fork> mp_final;

    weight_t start_weight;

    std::default_random_engine m_generator;
    std::uniform_int_distribution<weight_t> m_distribution;

    friend class Query9_JefastBuilder;
};