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
    
    // Joins ``table'' with the last table appended.
    // The last int parameter (was TableNumber) is not used and is
    // kept for backward compatibility only.
    //
    // Being called after any call to AddTableToFork is an error.
    // 
    // Returns the table number, which starts from 0. Or returns -1
    // if there's an error.
    int AppendTable(
        std::shared_ptr<Table> table,
        int RHSIndex, // RHS is the join column with the prev table
        int LHSIndex, // LHS is the join column with the next table
        int = 0/* unused, kept for backward compatibility */);
    
    // XXX not implemented.
    // Build will() always throw an expection if AddFilter() is called.
    int AddFilter(std::shared_ptr<jefastFilter> filter, int tableNumber);
    
    // Joins ``table'' with Table ``prevTableNumber''.
    // Returns the table number, which starts from 0. Or returns -1
    // if there's an error.
    //
    // If AppendTable() was ever called before this, those query preds are
    // still valid, except for the last table's LHSIndex which is ignored.
    //
    // If this is the first table added to the builder,
    // thisTableJoinColIndex, prevTableJoinColIndex and prevTableNumber
    // are ignored. Otherwise, prevTableNumber must be smaller than the
    // return value.
    int AddTableToFork(
        std::shared_ptr<Table> table,
        int thisTableJoinColIndex, // aka LHSIndex for this table
        int prevTableJoinColIndex, // aka RHSIndex for the prev table
        int prevTableNumber);

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
    //
    // XXX now completely ignored
    int AppendBuildSuggestion(int table, BuilderSuggestion::side buildSide);
    
    // Can only be called if AddTableToFork is never called, or
    // it returns nullptr.
    std::shared_ptr<jefastIndexLinear> Build();
    
    // Can only be called if AddTableToFork is called at least once,
    // or it returns nullptr.
    std::shared_ptr<jefastIndexFork> BuildFork();

private:
    bool m_has_fork;

    std::vector<std::shared_ptr<Table> > m_joinedTables;
    std::vector<int> m_parentTableNumber;
    std::vector<int> m_LHSJoinIndex;
    std::vector<int> m_RHSJoinIndex;

    std::vector<std::vector<std::shared_ptr<jefastFilter> > > m_filters;
    std::vector<BuilderSuggestion> m_buildOrder;
};
