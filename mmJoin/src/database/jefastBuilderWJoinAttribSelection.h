//// Programmer: Robert Christensen
//// email: robertc@cs.utah.edu
//// Implements a builder which uses an already constructed jefast and uses
//// it to build a modified version which has filtering on the Join conditions.
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
//class jefastBuilderWJoinAttribSelection {
//public :
//    jefastBuilderWJoinAttribSelection(std::shared_ptr<jefastIndexLinear> initialIndex)
//        : m_initialIndex{ initialIndex }
//    {
//        m_filters.resize(initialIndex->GetNumberOfLevels());
//    };
//    ~jefastBuilderWJoinAttribSelection()
//    {};
//
//    std::shared_ptr<jefastIndexLinear> Build();
//
//    void AddFilter(jfkey_t min, jfkey_t max, int joinNumber);
//
//private:
//    std::shared_ptr<jefastIndexLinear> m_initialIndex;
//
//    struct filter_MinMax {
//        filter_MinMax()
//        : set{ false }
//        , min{std::numeric_limits<jfkey_t>::min() }
//        , max{std::numeric_limits<jfkey_t>::max() }
//        {};
//        bool set;
//        jfkey_t min;
//        jfkey_t max;
//    };
//    std::vector<filter_MinMax> m_filters;
//};