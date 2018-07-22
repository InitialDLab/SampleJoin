#pragma once

#include <chrono>

time_t getEpoch();

class Timer
{
public:
    Timer();

    void start()
    {
        // TODO: should we check to see if the timer has already started?
        m_active = true;
        m_start = std::chrono::high_resolution_clock::now();
    }

    void stop()
    {
        if (m_active)
        {
            m_accumulator += std::chrono::duration_cast<std::chrono::high_resolution_clock::duration>(std::chrono::high_resolution_clock::now() - m_start);
            m_start = std::chrono::high_resolution_clock::time_point();
            m_active = false;
        }
    }

    void pause()
    {
        this->stop();
    }

    void reset()
    {
        m_start = std::chrono::high_resolution_clock::time_point();
        m_accumulator = std::chrono::high_resolution_clock::duration();
        m_active = false;
    };

    void update_accumulator()
    {
        stop();
        start();
    }

    double getSeconds()
    {
        return 0.0 + std::chrono::duration_cast<std::chrono::seconds>(m_accumulator).count();
    }

    double getMilliseconds()
    {
        return 0.0 + std::chrono::duration_cast<std::chrono::milliseconds>(m_accumulator).count();
    }

    double getMicroseconds()
    {
        return 0.0 + std::chrono::duration_cast<std::chrono::microseconds>(m_accumulator).count();
    }

    virtual ~Timer();
private:
    std::chrono::high_resolution_clock::time_point m_start;
    std::chrono::high_resolution_clock::duration m_accumulator;

    bool m_active;
};