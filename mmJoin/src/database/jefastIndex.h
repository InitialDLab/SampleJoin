// Programer: Robert Christensen
// email: robertc@cs.utah.edu
// header file for the jefast index
#pragma once

// NOTE: THE INITIAL IMPLEMENTATION IS VERY RIGID, ONLY TO VERIFY THE IDEA
//    IS VALID.  THE INTERFACE WILL LIKELY CHANGE VERY SOON
#include <map>
#include <memory>
#include <tuple>
#include <random>

#include "Table.h"
#include "DatabaseSharedTypes.h"
#include "jefastLevel.h"
#include "jefastFork.h"


class jefastIndexBase {
public:
    virtual ~jefastIndexBase()
    {};

    // get the total number of join possibilities.
    virtual int64_t GetTotal() = 0;

    virtual void GetJoinNumber(int64_t joinNumber, std::vector<int64_t> &out)= 0;

    virtual void GetRandomJoin(std::vector<int64_t> &out) = 0;

    // return the number of levels in this jefastIndex
    // (how large a vector will be if a join value is reported)
    virtual int GetNumberOfLevels() = 0;
};

class jefastIndexLinear : public jefastIndexBase {
public:
    virtual ~jefastIndexLinear()
    {};

    // get the total number of join possibilities.
    int64_t GetTotal();

    void GetJoinNumber(int64_t joinNumber, std::vector<int64_t> &out);

    void GetRandomJoin(std::vector<int64_t> &out);

    // return the number of levels in this jefastIndex
    // (how large a vector will be if a join value is reported)
    virtual int GetNumberOfLevels();

    // insert a new item into the index
    void Insert(int table_id, jefastKey_t record_id);

    void Delete(int table_id, jefastKey_t record_id);

    std::vector<weight_t> MaxOutdegree();

    int64_t MaxIndegree();

    void print_search_weights()
    {
        m_levels[0]->dump_weights();
    }

    void rebuild_initial();
    void set_postponeRebuild(bool value = true)
    {
        postpone_rebuild = value;
    }
private:
    jefastIndexLinear()
        : postpone_rebuild{ false }
    {};

    std::vector<std::shared_ptr<JefastLevel<jfkey_t> > > m_levels;
    weight_t start_weight;

    // random number stuff for reporting random results of the join
    std::default_random_engine m_generator;
    std::uniform_int_distribution<weight_t> m_distribution;

    bool postpone_rebuild;

    friend class JefastBuilder;
    friend class jefastBuilderWJoinAttribSelection;
    friend class jefastBuilderWNonJoinAttribSelection;
};