cmake_minimum_required(VERSION 2.6.0 FATAL_ERROR)

find_package(Threads)
include_directories(../../third_party/)

set(DATABASE_TABLES
	database/Table.cpp
	database/Table.h
	database/TableRepository.h
	database/DatabaseSharedTypes.h
	database/TableRegion.cpp
	database/TableRegion.h
	database/TableNation.cpp
	database/TableNation.h
	database/TableSupplier.cpp
	database/TableSupplier.h
	database/TablePartsupp.cpp
	database/TablePartsupp.h
	database/TablePart.cpp
	database/TablePart.h
	database/TableCustomer.cpp
	database/TableCustomer.h
	database/TableOrders.cpp
	database/TableOrders.h
	database/TableLineitem.cpp
	database/TableLineitem.h
    database/olkenIndex.h
)

SET(JEFAST_FILES
	database/jefastIndex.cpp
	database/jefastIndex.h
	database/jefastVertex.cpp
	database/jefastVertex.h
	database/jefastLevel.cpp
	database/jefastLevel.h
	database/jefastFilter.cpp
	database/jefastFilter.h
	database/jefastBuilder.cpp
	database/jefastBuilder.h
	database/jefastFork.cpp
	database/jefastFork.h
	database/jefastBuilderWJoinAttribSelection.cpp
	database/jefastBuilderWJoinAttribSelection.h
	database/jefastBuilderWNonJoinAttribSelection.cpp
	database/jefastBuilderWNonJoinAttribSelection.h
	)

set(UTILITY_FILES
	util/Timer.cpp
	util/Timer.h
	util/FileSizeTable.cpp
	util/FileSizeTable.h
	util/FileKeyValue.cpp
	util/FileKeyValue.h
	util/accumulator.cpp
	util/accumulator.h
	util/joinSettings.cpp
	util/joinSettings.h
)

set(DATABASE_FILES
	)
	
set(QUERY9_SUPPORT_FILES
	hard_coded/query9_jefast.cpp
	hard_coded/query9_jefast.h
	hard_coded/query9_Fork.cpp
	hard_coded/query9_Fork.h
	hard_coded/query9_jefastBuilder.cpp
	hard_coded/query9_jefastBuilder.h
	)
    
set(ADAPTIVE_SAMPLING_FILES
    database/TableGeneric.cpp
    database/TableGeneric.h
    database/ColumnExtractor.h
    util/FenwickTree.h
    util/StatelessFenwickTree.h
    util/StatelessFenwickTree.cpp
    database/AdaptiveProbabilityIndex.cpp
    database/AdaptiveProbabilityIndex.h
    database/AdaptiveProbabilityIndexTriangle.cpp
    database/AdaptiveProbabilityIndexTriangle.h
    database/DynamicVertex.cpp
    database/DynamicVertex.h
    database/DynamicLevel.cpp
    database/DynamicLevel.h
    database/DynamicIndex.cpp
    database/DynamicIndex.h
    )

#ADD_EXECUTABLE(query0_test query0_main.cpp ${DATABASE_TABLES} ${UTILITY_FILES} ${JEFAST_FILES})

#ADD_EXECUTABLE(query3_test query3_main.cpp ${DATABASE_TABLES} ${UTILITY_FILES} ${JEFAST_FILES})

#ADD_EXECUTABLE(query5_test query5_main.cpp ${DATABASE_TABLES} ${UTILITY_FILES} ${JEFAST_FILES})

# ADD_EXECUTABLE(query5_filtering_test query5_filtering_main.cpp ${DATABASE_TABLES} ${UTILITY_FILES} ${JEFAST_FILES})

#ADD_EXECUTABLE(query7_test query7_main.cpp ${DATABASE_TABLES} ${UTILITY_FILES} ${JEFAST_FILES})

#ADD_EXECUTABLE(query9_test query9_main.cpp ${DATABASE_TABLES} ${UTILITY_FILES} ${JEFAST_FILES} ${QUERY9_SUPPORT_FILES})

#ADD_EXECUTABLE(query2_test query2_main.cpp ${DATABASE_TABLES} ${UTILITY_FILES} ${JEFAST_FILES})

#ADD_EXECUTABLE(query10_test query10_main.cpp ${DATABASE_TABLES} ${UTILITY_FILES} ${JEFAST_FILES})

#ADD_EXECUTABLE(adaptive_test adaptive_test.cpp ${ADAPTIVE_SAMPLING_FILES})

#ADD_EXECUTABLE(adaptive_triangle adaptive_triangle.cpp ${ADAPTIVE_SAMPLING_FILES} ${DATABASE_TABLES} ${UTILITY_FILES})

#ADD_EXECUTABLE(playground playground.cpp ${ADAPTIVE_SAMPLING_FILES} ${JEFAST_FILES} ${DATABASE_TABLES} ${UTILITY_FILES})

#ADD_EXECUTABLE(iid_test iid_test.cpp ${ADAPTIVE_SAMPLING_FILES} ${JEFAST_FILES} ${DATABASE_TABLES} ${UTILITY_FILES})
#target_link_libraries(iid_test ${CMAKE_THREAD_LIBS_INIT})

ADD_EXECUTABLE(generic_sample_test generic_sample_test.cpp ${ADAPTIVE_SAMPLING_FILES} ${JEFAST_FILES} ${DATABASE_TABLES} ${UTILITY_FILES})

#ADD_EXECUTABLE(condition_sample_test condition_sample_test.cpp ${ADAPTIVE_SAMPLING_FILES} ${JEFAST_FILES} ${DATABASE_TABLES} ${UTILITY_FILES})

#ADD_EXECUTABLE(scale_sample_test scale_sample_test.cpp ${ADAPTIVE_SAMPLING_FILES} ${JEFAST_FILES} ${DATABASE_TABLES} ${UTILITY_FILES})

#ADD_EXECUTABLE(calc_ks_threshold calc_ks_threshold.cpp)

# ADD_EXECUTABLE(querya_test querya_main.cpp ${DATABASE_TABLES} ${UTILITY_FILES} ${JEFAST_FILES})
