// implements an accumulator with confidence bounds
// Robert Christensen
// robertc@cs.utah.edu

#include <iostream>
#include <iterator>

class accumulator_double {
public:
    accumulator_double() :
        z_value95{ 1.96 }
        , m_sum{ 0.0 }
        , m_sum2{ 0.0 }
        , m_count{ 0 }
        , m_total{ 1 } 
    {
    };

    ~accumulator_double()
    {};

    void insert(double x);
    template<class Iter>
    void insert(Iter start, Iter end);

    int get_count();

    void set_total(long total);

    double get_mean();
    double get_variance();
    double get_sum();

    double get_95ConfidenceRange_mean();
    double get_95ConfidenceRange_sum();

    // returns true if it is within p percentage of the mean.
    bool within_mean(float p);
    bool within_sum(float p);

    int estimate_n_needed_95confidence(float p);

private:
    double z_value95;

    double m_sum;
    double m_sum2;
    int m_count; // number the accumulator has seen
    long m_total; // total number that are possible
};

template<class Iter>
inline void accumulator_double::insert(Iter start, Iter end)
{
    while (start != end) {
        insert(*start);
        ++start;
    }
}
