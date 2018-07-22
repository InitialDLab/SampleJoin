// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Implements a builder for the jefast graph
#pragma once

#include <vector>
#include <list>
#include <map>
#include <random>

#include "jefastIndex.h"
#include "jefastVertex.h"
#include "jefastLevel.h"
#include "jefastFilter.h"
#include "Table.h"



class JefastBuilder {
public:
    JefastBuilder();
    ~JefastBuilder()
    {};

    int AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex, int tableNumber);
    int AddFilter(std::shared_ptr<jefastFilter> filter, int tableNumber);

    // calling either of the following functions will make the last node in the graph a 'fork'

    // add a table only to the fork.  This means that this table is that last portion of a join before
    // the join is finished.
    // the 'table number' is the number used to pull that particular table from the results list
    //int AddTableToFork(std::shared_ptr<Table> table, int RHSIndex, int tableNumber);

    // the 'table number' is the starting index of the index used to pull a particular table value
    // from the join results.
    //int AddJefastToFork(std::shared_ptr<jefastIndexBase> branch, int RHSIndex, int LHSIndex, int tableNumber);

    struct BuilderSuggestion {
        enum side {LEFT, RIGHT};

        BuilderSuggestion(int t, side s)
            :
            table_id{ t },
            build_side{ s }
        {};
        int table_id;
        side build_side;
    };

    // the builder suggestions will be used in the order they are inserted.
    // if a suggestion has been made but the table has already been scanned, the suggestion
    // will be ignored.
    int AppendBuildSuggestion(int table, BuilderSuggestion::side buildSide);

    std::shared_ptr<jefastIndexLinear> Build();

private:
    std::vector<std::shared_ptr<Table> > m_joinedTables;
    std::vector<int> m_LHSJoinIndex;
    std::vector<int> m_RHSJoinIndex;

    std::vector<std::vector<std::shared_ptr<jefastFilter> > > m_filters;
    std::vector<BuilderSuggestion> m_buildOrder;
};

//class JefastBuilderWFork {
//public:
//    JefastBuilderWFork();
//    ~JefastBuilderWFork()
//    {};
//
//    int AppendTable(std::shared_ptr<Table> table, int RHSIndex, int LHSIndex);
//    // we will not be supporting filtering yet
//    //int AddFilter(std::shared_ptr<jefastFilter> filter, int tableNumber);
//
//    // This will insert the needed information to build a fork event.  The join results
//    // will appear at the end of the results for calling jefast join number
//    void AddForkInformation(std::shared_ptr<Table> table1, int RHSIndex1, int LHSIndex1
//                         , std::shared_ptr<Table> table2, int RHSIndex2, int LHSIndex2);
//
//
//    std::shared_ptr<jefastIndexBase> Build();
//
//private:
//    std::vector<std::shared_ptr<Table> > m_joinedTables;
//    std::vector<int> m_LHSJoinIndex;
//    std::vector<int> m_RHSJoinIndex;
//
//    std::shared_ptr<Table> m_ForkTable1;
//    int m_ForkTable1_RHSIndex;
//    int m_ForkTable1_LHSIndex;
//
//    std::shared_ptr<Table> m_ForkTable2;
//    int m_ForkTable2_RHSIndex;
//    int m_ForkTable2_LHSIndex;
//
//
//    //std::vector<std::vector<std::shared_ptr<jefastFilter> > > m_filters;
//    //std::vector<BuilderSuggestion> m_buildOrder;
//};