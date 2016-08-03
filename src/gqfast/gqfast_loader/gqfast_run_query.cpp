#ifndef gqfast_run_query_cpp_
#define gqfast_run_query_cpp_

#include <iostream>
#include <fstream>
#include <dlfcn.h>             // dll functions
#include <utility>             // std::pair
#include "serialization.hpp"
#include "gqfast_run_query.hpp"

chrono::steady_clock::time_point benchmark_t1;
chrono::steady_clock::time_point benchmark_t2;

// Pre-declared index pointers
GqFastIndex<uint32_t>** idx;

pthread_t threads[MAX_THREADS];
pthread_spinlock_t** spin_locks;

static int num_indexes;

void init_globals()
{

    idx = new GqFastIndex<uint32_t>*[num_indexes];
    spin_locks = new pthread_spinlock_t*[num_indexes];
    // Globals are initially null/0
    for (int i=0; i<num_indexes; i++)
    {
        idx[i] = nullptr;
        buffer_arrays[i] = nullptr;
        idx_position_in_use[i] = false;
    }
}

void delete_globals()
{
    for (int i=0; i<num_indexes; i++)
    {
        if (idx[i])
        {
            int num_encodings = idx[i]->num_fragment_data;
            // Free the associated buffer
            for (int j=0; j<num_encodings; j++)
            {
                delete[] buffer_arrays[i][j];
            }

            delete[] buffer_arrays[i];
            delete idx[i];
            delete[] spin_locks[i];
        }
    }
    delete[] idx;
    delete[] spin_locks;
}

int main(int argc, char ** argv)
{

    num_indexes = 3;

    string table_name = "dt";
    string lookup_name = "doc";

    string filename = table_name + "_" + lookup_name + ".txt";

    int idx_position = 0;

    load_index(idx[idx_position], filename.c_str());




    init_globals();
    delete_globals();

    return 0;
}




#endif
