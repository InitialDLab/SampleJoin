
#include <vector>
#include <memory>
#include <map>
#include <unordered_map>
#include <random>


#include "TableGeneric.h"

enum SAMPLE_STATUS { GOOD_S = 0, FAIL_S = 1, REJECT_S = 2 };

class AdaptiveProbabilityIndex
{
public:
    AdaptiveProbabilityIndex(std::shared_ptr<TableGeneric> Table1, std::shared_ptr<TableGeneric> Table2, std::shared_ptr<TableGeneric> Table3);

    SAMPLE_STATUS sample_join(std::vector<int> &table_indexes, int64_t rval = 0);

    int64_t maxAGM()
    {
        return this->initial_AGM;
    }

    // this will do a scan of the index, trying to update the indexes
    // NOTE: this is an exhaustive search
    int64_t test_scan();

    double last_p;
private:
    std::default_random_engine m_rgen;

    struct point_information {
        // the frequency table.  Use a stateless Fenwick tree to interact with
        // the data here.
        std::vector<int64_t> m_frequencies;

        // max frequency used in the frequency tables.
        // we use this to quickly know if we should reject a sample
        int64_t max_frequency;

        // what is the value we are storing for this point?
        int64_t value;
    };


    std::vector<std::shared_ptr<TableGeneric> > m_data;
    std::vector<std::vector<int64_t> > m_probability_index;

    // this contains the max frequency for a given value.
    // in our join R--S--T with R(A,B), S(B,C), T(C,D),
    // this table will contain the max AGM bound for S(B).
    // this is updated dynamically during queries
    std::unordered_map<int64_t, point_information> m_maxFrequency;
    std::unordered_map<int64_t, int64_t> m_lastFrequency;

    int64_t initial_AGM;
    int64_t default_round2_AGM;
};