project(main_mem_join)
cmake_minimum_required(VERSION 2.6.0 FATAL_ERROR)

IF(CMAKE_COMPILER_IS_GNUCXX)
    SET (CMAKE_CXX_FLAGS "-std=gnu++14 -O2")
ELSEIF(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    SET (CMAKE_CXX_FLAGS "-std=c++14 -lpthread")
ELSEIF(MSVC)
    # ON BY DEFAULT
ENDIF ()

set (SamplingJoin_VERSION_MAJOR 0)
set (SamplingJoin_VERSION_MINOR 0)

add_subdirectory(src)
