// Applies join settings
// Programmer: Robert Christensen
// robertc@cs.utah.edu

#include "tclap-1.2.1/include/tclap/CmdLine.h"

#include <cstdint>
#include <vector>

struct global_settings {
    int modifyCount;

    bool hashJoin;
    bool jefastSample;
    bool olkenSample;
    bool threading;
    bool jefastRate;
    bool OlkenRate;
    bool joinAttribHash;
    bool joinAttribJefast;
    bool nonJoinAttribHash;
    bool nonJoinAttribJefast;
    bool jefastModify;

    bool buildJefast;
    bool buildIndex;

    std::string null_file_name;
};

global_settings parse_args(int argc, char** argv);

std::vector<int64_t> generate_unique_random_numbers(size_t count, int64_t min, int64_t max);