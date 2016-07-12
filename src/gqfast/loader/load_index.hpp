#ifndef  load_index_
#define load_index_

#include <iostream>
#include "loader.hpp"

using namespace std;


int findEmptyIndexPosition()
{
    for (int i=0; i<MAX_INDICES; i++)
    {
        if (!idx_position_in_use[i]) {
            return i;
        }
    }

    cerr << "Error: No index slots available\n";
    return -1;

}



void init_buffer(int pos)
{

    int num_encodings = metadata.idx_num_encodings[pos];
    int max_frag = metadata.idx_max_fragment_sizes[pos];

    // Allocate and initialize buffer arrays
    buffer_arrays[pos] = new uint64_t***[num_encodings];
    for (int i=0; i<num_encodings; i++)
    {
        buffer_arrays[pos][i] = new uint64_t**[MAX_THREADS];
    }
    // Domain buffer for Foreign key column
    uint64_t domain = metadata.idx_domains[pos][0];

    // Init locks
    spin_locks[pos] = new pthread_spinlock_t[domain];
    for (uint64_t i=0; i<domain; i++)
    {
        pthread_spin_init(&spin_locks[pos][i], PTHREAD_PROCESS_PRIVATE);
    }

}

int load(string path, Encodings encodings[], int num_encodings)
{

    int indexID = findEmptyIndexPosition();
    if (indexID < 0) return -1;


    idx[indexID] = buildIndex<int, uint32_t>(path, encodings, numEncodings, indexID);
    init_buffer(indexID);
    idx_position_in_use[indeXID] = true;

    return indexID;

}







#endif
