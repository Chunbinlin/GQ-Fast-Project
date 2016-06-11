#ifndef global_vars_
#define global_vars_

#include <vector>
#include <boost/serialization/vector.hpp>
#include <ctime>
#include <chrono>
#include "fastr_index.hpp"

#define MAX_INDICES 6
#define MAX_THREADS 8

#define SIZE_UINT32_T 4

#define ABS(x) ((x)<0 ? -(x) : (x))


// Clock variables
extern struct timespec start;
extern struct timespec finish;

extern chrono::steady_clock::time_point benchmark_t1;
extern chrono::steady_clock::time_point benchmark_t2;

// Pre-declared index pointers
extern fastr_index<uint32_t>* idx[MAX_INDICES];

// Metadata
struct Metadata
{
    vector<uint64_t> idx_domains[MAX_INDICES];
    vector<int> idx_cols_byte_sizes[MAX_INDICES];
    int idx_map_byte_sizes[MAX_INDICES];
    int idx_num_encodings[MAX_INDICES];
    int idx_max_fragment_sizes[MAX_INDICES];

    Metadata() {}


    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {

        for (int i=0; i<MAX_INDICES; i++)
        {
            ar & idx_domains[i];
        }
        for (int i=0; i<MAX_INDICES; i++)
        {
            ar & idx_cols_byte_sizes[i];
        }
        for (int i=0; i<MAX_INDICES; i++)
        {
            ar & idx_map_byte_sizes[i];
        }
        for (int i=0; i<MAX_INDICES; i++)
        {
            ar & idx_num_encodings[i];
        }
        for (int i=0; i<MAX_INDICES; i++)
        {
            ar & idx_max_fragment_sizes[i];
        }

    }
};

struct args_threading
{
    uint32_t start;
    uint32_t end;
    int thread_id;
};


extern pthread_t threads[MAX_THREADS];
extern pthread_spinlock_t * spin_locks[MAX_INDICES];

extern struct Metadata metadata;

// Buffers
extern uint64_t**** buffer_arrays[MAX_INDICES] __attribute__ ((visibility ("default")));

int num_loaded_indices;

#endif
