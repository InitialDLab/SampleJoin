#include "accumulator.h"

#include <math.h>

void accumulator_double::insert(double x)
{
    m_sum += x;
    m_sum2 += (x * x);
    ++m_count;
}

int accumulator_double::get_count()
{
    return m_count;
}

void accumulator_double::set_total(long total)
{
    m_total = total;
}

double accumulator_double::get_mean()
{
    return m_sum / m_count;
}

double accumulator_double::get_variance()
{
    double scaler = 1.0 / (m_count * (m_count - 1.0));

    return scaler * ((m_count * m_sum2) - m_sum * m_sum);
}

double accumulator_double::get_sum()
{
    return get_mean() * m_total;
}

double accumulator_double::get_95ConfidenceRange_mean()
{
    return z_value95 * sqrt(get_variance()) / sqrt(m_count);
}

double accumulator_double::get_95ConfidenceRange_sum()
{
    return m_total * z_value95 * sqrt(get_variance()) / sqrt(m_count);
}

bool accumulator_double::within_mean(float p)
{
    return get_mean() * p < get_95ConfidenceRange_mean();
}

bool accumulator_double::within_sum(float p)
{
    return get_sum() * p < get_95ConfidenceRange_sum();
}

int accumulator_double::estimate_n_needed_95confidence(float p)
{
    float numerator = z_value95 * sqrt(get_variance());
    float denominator = get_mean() * p * 0.5;
    float fraction = numerator / denominator;

    return ceil(fraction * fraction);
}