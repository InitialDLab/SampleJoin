// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// -----------------------------
// This contains shared data types for the database
#pragma once
#include <vector>
#include <tuple>
#include <map>
#include <cstdint>
#include <ctime>

enum DATABASE_DATA_TYPES { INT32, INT64, FLOAT, CHAR, STRING, NONEXISTANT, LAST };

const char FILE_DELIMINTAR{ '|' };

typedef int64_t jfkey_t;

// for date, to simplify the indexes we will use the number of seconds
// since Jan 1, 1970
typedef int64_t jefastTime_t;

// for the dynamic programming technique, we will use the following structures for the index:
typedef jfkey_t jefastKey_t;
typedef int64_t weight_t;

// for this tuple:
//   0- the number of unique join tuples which can be produced starting at this vertex
//   1- A list of indexes to tuples in the left table which match this join
//   2- A list of indexes to tuples in the right table which match this join
//   3- A list of weights for the join cardinality for each of the right table elements
//      (An index in vector(2) should correspond to vector(3))
//   4- A list of keys for the next join
//      (An index in vector(2) and vector(3) should correspond to elements in vector(4))
struct jefastPayload_t
{
    jefastPayload_t()
        : weight{ 0 }
    {};

    weight_t weight;
    std::vector<jfkey_t> left_table_index;
    std::vector<jfkey_t> right_table_index;
    std::vector<weight_t> out_weight;
    std::vector<jfkey_t> next_join_elements;
};
//typedef std::tuple<weight_t, std::vector<jfkey_t>, std::vector<jfkey_t>, std::vector<weight_t>, std::vector<jfkey_t> > jefastPayload_t;

typedef std::map<jefastKey_t, jefastPayload_t> jefast_index_t;