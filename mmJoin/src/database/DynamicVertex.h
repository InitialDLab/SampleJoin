#pragma once

#include <stdint.h>
#include <vector>
#include <assert.h>
#include <random>

#include "../database/TableGeneric.h"
#include "../util/StatelessFenwickTree.h"

//enum WALK_STATUS { GOOD = 0b0, REJECT = 0b1, FAIL = 0b10, REJECTAFAIL = 0b11 };
typedef int WALK_STATUS;
constexpr int GOOD = 0;
constexpr int REJECT = 1;
constexpr int FAIL = 2;
constexpr int REJECTAFAIL = REJECT | FAIL;

static std::mt19937_64 gen{ (unsigned)std::chrono::system_clock::now().time_since_epoch().count() };

class DynamicVertex {
public:
    DynamicVertex(int64_t max_wgt, int64_t cardinality, int64_t value, std::shared_ptr<TableGenericBase> home)
        : walk_count{ 0 }
        , successful_walks{ 0 }
        , m_value{ value }
        , S_sum{ 0.0 }
        , V_sum{ 0.0 }
        , m_DPset{ 0 }
        , max_weight{ max_wgt * cardinality }
        , m_home{ home }
    {
        weights.resize(cardinality, max_wgt);
        if(weights.size() > 0)
            StatelessFenwickTree::initialize(&weights[0], cardinality);
    }

    virtual ~DynamicVertex()
    {
    };

    int64_t walk_count;
    int64_t successful_walks;
    int64_t m_value;
    double S_sum;
    double V_sum;

    int m_DPset;
protected:
    int64_t max_weight;
public:


    std::vector<int64_t> weights;
    std::shared_ptr<TableGenericBase> m_home;

    int64_t get_max_weight()
    {
        if (is_DPset() == true)
            return max_weight;

        // if DP is set, we return the max weight.  Otherwise, return the WJ estimation.
        else if (successful_walks == 0)
            return 1;
        else return ceil(this->calculate_b());
    }

    bool WJ_stats_good()
    {
        // it is good is we have at least 10 successful walks and at least 30 walks.
        // if none are successful, we just say it is good if we have at least 1000 walks (in that case we just say the weight is really small)
        return ((successful_walks > 10) && (walk_count > 30)) || (walk_count > 1000);
    }

    // return false if it fails
    WALK_STATUS get_records(int64_t &inout_weight_condition, int64_t &out_record_id, int64_t &update_key, double &p)
    {
        if (weights.size() == 0 || max_weight == 0)
            return FAIL;

        // report that we had a traversal.  Successful walks will be reported at the end
        //++walk_count;

        WALK_STATUS ret_val = GOOD;

        if (inout_weight_condition >= max_weight) {
            ret_val = REJECT;

            // adjust the weights so we can continue the walk (even though we failed to sample)
            std::uniform_int_distribution<int64_t> dist(0, max_weight - 1);

            inout_weight_condition = dist(gen);
        }

        update_key = StatelessFenwickTree::findG_mod(&weights[0], weights.size(), inout_weight_condition);
        //out_record_id = m_home->get_column2_value(m_home->get_column1_index_offset(m_value, update_key));
        out_record_id = m_home->get_column1_index_offset(m_value, update_key);

        double pre_p = p;

        // we have to say p = 1 * weight.  each p value is calculated independent of
        // each other and will be combined later when doing updates
        if (weights.size() != 1)
        {
            if (update_key != weights.size() - 1)
            {
                p = ((double)StatelessFenwickTree::readSingle_value(&weights[0], update_key + 1) / max_weight);
            }
            else
            {
                p = ((max_weight - ((double)StatelessFenwickTree::read_value(&weights[0], update_key))) / max_weight);
            }
        }
        else {
            p = 1.0;
        }

        assert(p != 0);
        return ret_val;
    }

    // we change the value of max_prev to be a new value for max_prev
    WALK_STATUS get_records_p(int64_t &max_prev, int64_t &out_record_id, int64_t &update_key, double &p)
    {
        if (weights.size() == 0 || max_weight == 0)
            return FAIL;

        // report that we had a traversal.  Successful walks will be reported at the end
        //++walk_count;

        WALK_STATUS ret_val = GOOD;

        std::uniform_int_distribution<int64_t> i_dist(0, std::max(max_prev, max_weight) - 1);
        int64_t weight{ i_dist(gen) };

        if (weight >= max_weight) {
            ret_val = REJECT;

            // adjust the weights so we can continue the walk (even though we failed to sample)
            //std::default_random_engine gen(std::chrono::system_clock::now().time_since_epoch().count());
            std::uniform_int_distribution<int64_t> dist(0, max_weight - 1);

            weight = dist(gen);
        }

        update_key = StatelessFenwickTree::findG_mod(&weights[0], weights.size(), weight);
        //out_record_id = m_home->get_column2_value(m_home->get_column1_index_offset(m_value, update_key));
        out_record_id = m_home->get_column1_index_offset(m_value, update_key);

        double pre_p = p;

        // we have to say p = 1 * weight.  each p value is calculated independent of
        // each other and will be combined later when doing updates
        if (weights.size() != 1)
        {
            if (update_key != weights.size() - 1)
            {
                max_prev = StatelessFenwickTree::readSingle_value(&weights[0], update_key + 1);
                //p = ((double)StatelessFenwickTree::readSingle_value(&weights[0], update_key + 1) / max_weight);
            }
            else
            {
                max_prev = max_weight - StatelessFenwickTree::read_value(&weights[0], update_key);
                //p = ((max_weight - ((double)StatelessFenwickTree::read_value(&weights[0], update_key))) / max_weight);
            }


        }
        else {
            max_prev = max_weight;
        }

        p = (double)max_prev / max_weight;

        assert(p != 0);
        return ret_val;
    }

    void adjust_weight(int64_t new_weight, int64_t update_key)
    {
        if (update_key >= weights.size()) {
            assert(false);
        }

        if (update_key == weights.size() - 1)
        {
            int64_t last_largest_weight{ 0 };
            if (weights.size() > 1)
                last_largest_weight = StatelessFenwickTree::read_value(&weights[0], weights.size() - 1);

            max_weight = last_largest_weight + new_weight;
        }
        else {
            int64_t current_weight = StatelessFenwickTree::readSingle_value(&weights[0], update_key + 1);
            StatelessFenwickTree::update_add_value(&weights[0], weights.size(), update_key + 1, new_weight - current_weight);
            max_weight += new_weight - current_weight;
        }
        
        assert(!(max_weight == 0 && (successful_walks > 0)));
    }

    void adjust_DP_weight(int64_t new_weight, int64_t update_key)
    {
        adjust_weight(new_weight, update_key);

        ++m_DPset;
    }

    bool is_DPset()
    {
        return m_DPset == weights.size();
    }

    int64_t get_weight()
    {
        return max_weight;
    }

    void report(double p, bool success)
    {
        assert(p != 0);

        ++walk_count;
        if (success)
        {
            ++successful_walks;
            S_sum += 1 / p;
            V_sum += (1 / p) * (1 / p);
        }
        // if it was not successful, only walk count it updated.
        //if (walk_count % 100000 == 99999)
        //this->calculate_b();

        //std::cout << "id= " << this->m_value << "est= " << (S_sum / walk_count) << " bound: +" << calculate_b() << " max weight: " << this->get_max_weight() << '\n';

    }

    double calculate_b()
    {
        //constexpr double sqrt2{ 1.4142135623730950488016887242097 };

        // this is a table of erfinv(x) * sqrt2.  the z value must account for
        // both left and right side of the bell curve.
        //constexpr double err{ 1.6449 }; // 90% confidence
        //constexpr double err{ 1.96 }; // 95% confidence
        //constexpr double err{ 2.5758}; // 99% confidence
        constexpr double err{ 3.2905}; // 99.9% confidence

        double s = sqrt((V_sum - S_sum * S_sum / walk_count) / (walk_count - 1));
        if (isnan(s)) s = 0.0;

        //std::cout << "id= " << this->m_value << "est= " << (S_sum / walk_count) << " bound: +" << (S_sum / walk_count) + s * err / sqrt(walk_count) << " max weight: " << this->get_max_weight() << '\n';

        return (S_sum / walk_count) + s * err / sqrt(walk_count);
        //return z * sqrt(V_sum / walk_count - (S_sum / walk_count) * (S_sum / walk_count)) / sqrt(walk_count) + S_sum / walk_count;
        //return z * (V_sum * sqrt(walk_count)) + S_sum / walk_count;
    }

    double est_size()
    {
        return (S_sum / walk_count);
    }
};

class DynamicOriginVertex : public DynamicVertex
{
public:
    DynamicOriginVertex(int64_t max_wgt, std::shared_ptr<TableGenericBase> table)
        : DynamicVertex(max_wgt, table->get_size(), 0, table)
    {};

    virtual ~DynamicOriginVertex()
    {};

    // we change the query to use the column rather then column + offset
    WALK_STATUS get_records(int64_t &inout_weight_condition, int64_t &out_record_id, int64_t &update_key, double &p)
    {
        //++walk_count;

        if (inout_weight_condition >= max_weight)
            return REJECT;

        update_key = StatelessFenwickTree::findG_mod(&weights[0], weights.size(), inout_weight_condition);

        // for this one, we just join with all other items.
        //out_record_id = m_home->get_column2_value(update_key);
        out_record_id = update_key;

        if (update_key != weights.size() - 1)
        {
            p = ((double)StatelessFenwickTree::readSingle_value(&weights[0], update_key + 1) / max_weight);
        }
        else
        {
            p = ((max_weight - ((double)StatelessFenwickTree::read_value(&weights[0], update_key))) / max_weight);
        }

        assert(p != 0);

        return GOOD;
    }

    WALK_STATUS get_records_p(int64_t &max_prev, int64_t &out_record_id, int64_t &update_key, double &p)
    {
        //++walk_count;

        //if (max_prev >= max_weight)
        //    return REJECT;
        std::uniform_int_distribution<int64_t> i_dist(0, std::max(max_prev, max_weight) - 1);
        int64_t weight{ i_dist(gen) };


        update_key = StatelessFenwickTree::findG_mod(&weights[0], weights.size(), weight);

        // for this one, we just join with all other items.
        //out_record_id = m_home->get_column2_value(update_key);
        out_record_id = update_key;

        if (weights.size() != 1)
        {
            if (update_key != weights.size() - 1)
            {
                max_prev = StatelessFenwickTree::readSingle_value(&weights[0], update_key + 1);
                //p = ((double)StatelessFenwickTree::readSingle_value(&weights[0], update_key + 1) / max_weight);
            }
            else
            {
                max_prev = max_weight - StatelessFenwickTree::read_value(&weights[0], update_key);
                //p = ((max_weight - ((double)StatelessFenwickTree::read_value(&weights[0], update_key))) / max_weight);
            }
        }
        else {
            max_prev = max_weight;
        }

        p = (double)max_prev / max_weight;

        assert(p != 0);

        return GOOD;
    }
};
