#include "Timer.h"

time_t getEpoch()
{
    struct tm epoch;
    epoch.tm_year = 70;
    epoch.tm_hour = 0;
    epoch.tm_isdst = 0;
    epoch.tm_mday = 1;
    epoch.tm_min = 0;
    epoch.tm_mon = 0;
    epoch.tm_sec = 0;
    return mktime(&epoch);
}

Timer::Timer()
{
}

Timer::~Timer()
{
}