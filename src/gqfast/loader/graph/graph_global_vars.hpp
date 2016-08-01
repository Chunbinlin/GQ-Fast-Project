#ifndef graph_global_vars_
#define graph_global_vars_

#include <vector>
#include <ctime>
#include <chrono>
#include "graph_index.hpp"
#include <map>
#define MAX_INDICES 6
#define MAX_THREADS 8

#define MAX_DOC_ID 23326299
#define MAX_TERM_ID_MESH 223705
#define MAX_TERM_ID_TAG 247030

#define DOC_PROPERTY 1
#define TERM_PROPERTY 2
#define AUTHOR_PROPERTY 3

#define SIZE_UINT32_T 4

#define ABS(x) ((x)<0 ? -(x) : (x))


// Clock variables
extern struct timespec start;
extern struct timespec finish;

extern chrono::steady_clock::time_point benchmark_t1;
extern chrono::steady_clock::time_point benchmark_t2;

// Pre-declared index pointers
extern graph_index<uint32_t>* idx[MAX_INDICES];

// Metadata
struct Metadata
{
    vector<uint64_t> idx_domains[MAX_INDICES];
    vector<int> idx_cols_byte_sizes[MAX_INDICES];
    int idx_map_byte_sizes[MAX_INDICES];
    int idx_num_encodings[MAX_INDICES];
    int idx_max_fragment_sizes[MAX_INDICES];

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
extern uint64_t**** buffer_arrays[MAX_INDICES];

int num_loaded_indices;
bool idx_position_in_use[MAX_INDICES];

#endif
