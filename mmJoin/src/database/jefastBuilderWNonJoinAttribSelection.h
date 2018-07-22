//// Programmer: Robert Christensen
//// email: robertc@cs.utah.edu
//// Implements a builder which uses an already constructed jefast and
//// uses that to build a modified version which has filtering on attributes
//#pragma once
//
//#include <memory>
//#include <map>
//#include <vector>
//#include <limits>
//
//#include "jefastIndex.h"
//#include "jefastLevel.h"
//#include "jefastVertex.h"
//
//class jefastBuilderWNonJoinAttribSelection {
//public:
//    jefastBuilderWNonJoinAttribSelection(std::shared_ptr<jefastIndexLinear> initialIndex)
//        : m_initialIndex{ initialIndex }
//    {
//        m_filters.resize(initialIndex->GetNumberOfLevels());
//    }
//    ~jefastBuilderWNonJoinAttribSelection()
//    {};
//
//    std::shared_ptr<jefastIndexLinear> Build();
//
//    // adds a filter to the list of filters to apply when building.
//    // the first filter inserted will be the one the builder will use with
//    // an index.  All the others will be used to verify the tuple.
//    void AddFilter(std::shared_ptr<jefastFilter> filter, int tableNumber);
//
//private:
//    std::shared_ptr<jefastIndexLinear> m_initialIndex;
//
//    std::vector<std::vector<std::shared_ptr<jefastFilter> > > m_filters;
//};