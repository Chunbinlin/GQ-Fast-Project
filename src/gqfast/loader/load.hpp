#ifndef  load_
#define load_

#include <iostream>
#include "loader.hpp"

#define SEMMEDDB 1
#define PUBMED_MESH_ONLY 2
#define PUBMED_MESH_PLUS_SUPP 3

#define OPTIMAL_COMPRESSION 5

#define DT1_MESH "./pubmed/dt1_mesh.csv"
#define DT2_MESH "./pubmed/dt2_mesh.csv"
#define DT1_TAG "./pubmed/dt1_tag.csv"
#define DT2_TAG "./pubmed/dt2_tag.csv"

using namespace std;

void init_buffer(int pos) {

    int num_encodings = metadata.idx_num_encodings[pos];  
    int max_frag = metadata.idx_max_fragment_sizes[pos];

    // Allocate and initialize buffer arrays
    buffer_arrays[pos] = new int***[num_encodings];
    for (int i=0; i<num_encodings; i++) {
        buffer_arrays[pos][i] = new int**[MAX_THREADS];
        /*for (int j=0; j<MAX_THREADS; j++) {
            buffer_arrays[pos][i][j] = new int*[BUFFER_POOL_SIZE];
            for (int k=0; k<BUFFER_POOL_SIZE; k++) {
                buffer_arrays[pos][i][j][k] = new int[max_frag]();
            }
        }
        */
    }
    // Domain buffer for Foreign key column
    uint64_t domain = metadata.idx_domains[pos][0];

    // Init locks
    spin_locks[pos] = new pthread_spinlock_t[domain];
    for (uint64_t i=0; i<domain; i++) {
        pthread_spin_init(&spin_locks[pos][i], PTHREAD_PROCESS_PRIVATE); 
    }

}


template <typename TValue, typename TIndexMap>
void pubmed_create_indices(int my_encoding, string dt1, string dt2) {


    Encodings encoding1("Doc", my_encoding);
    Encodings first_index_encodings[1] = {encoding1};

    idx[0] = buildIndex<TValue, TIndexMap>("./pubmed/da1.csv", first_index_encodings, 1, 0);
    init_buffer(0);

    Encodings encoding2("Year", my_encoding);
    Encodings second_index_encodings[1] = {encoding2};

    idx[1] = buildIndex<TValue, TIndexMap>("./pubmed/dy.csv", second_index_encodings, 1, 1);
    init_buffer(1);

    Encodings encoding3("Term", my_encoding);
    Encodings encoding4("Fre",my_encoding);
    Encodings third_index_encodings[2] = {encoding3, encoding4};  

    idx[2] = buildIndex<TValue, TIndexMap>(dt1, third_index_encodings, 2, 2);
    init_buffer(2);

    Encodings encoding5("Doc", my_encoding);
    Encodings encoding6("Fre", my_encoding);

    Encodings fourth_index_encodings[2] = {encoding5, encoding6};
    
    idx[3] = buildIndex<TValue, TIndexMap>(dt2, fourth_index_encodings, 2, 3);
    init_buffer(3);

    Encodings encoding7("Author", my_encoding);

    Encodings fifth_index_encodings[1] = {encoding7};
    
    idx[4] = buildIndex<TValue, TIndexMap>("./pubmed/da2.csv", fifth_index_encodings, 1, 4);
    init_buffer(4);
    
}

template <typename TValue, typename TIndexMap>
void pubmed_create_optimal_indices(string dt1, string dt2) {


    Encodings encoding1("Doc", ENCODING_BYTE_ALIGNED_BITMAP);
    Encodings first_index_encodings[1] = {encoding1};

    idx[0] = buildIndex<TValue, TIndexMap>("./pubmed/da1.csv", first_index_encodings, 1, 0);
    init_buffer(0);

    Encodings encoding2("Year", ENCODING_BIT_ALIGNED_COMPRESSED);
    Encodings second_index_encodings[1] = {encoding2};

    idx[1] = buildIndex<TValue, TIndexMap>("./pubmed/dy.csv", second_index_encodings, 1, 1);
    init_buffer(1);

    Encodings encoding3("Term", ENCODING_BYTE_ALIGNED_BITMAP);
    Encodings encoding4("Fre",ENCODING_HUFFMAN);
    Encodings third_index_encodings[2] = {encoding3, encoding4};  

    idx[2] = buildIndex<TValue, TIndexMap>(dt1, third_index_encodings, 2, 2);
    init_buffer(2);

    Encodings encoding5("Doc", ENCODING_BYTE_ALIGNED_BITMAP);
    Encodings encoding6("Fre", ENCODING_HUFFMAN);

    Encodings fourth_index_encodings[2] = {encoding5, encoding6};
    
    idx[3] = buildIndex<TValue, TIndexMap>(dt2, fourth_index_encodings, 2, 3);
    init_buffer(3);

    Encodings encoding7("Author", ENCODING_BIT_ALIGNED_COMPRESSED);

    Encodings fifth_index_encodings[1] = {encoding7};
    
    idx[4] = buildIndex<TValue, TIndexMap>("./pubmed/da2.csv", fifth_index_encodings, 1, 4);
    init_buffer(4);

}

template <typename TValue, typename TIndexMap>
void semmeddb_create_indices(int encoding_name) {


    Encodings encoding1("CONCEPT_SEMTYPE_ID", encoding_name);
    Encodings first_index_encodings[1] = {encoding1};

    idx[0] = buildIndex<TValue, TIndexMap>("./semmeddb/cs1.csv", first_index_encodings, 1, 0);
    init_buffer(0);

    Encodings encoding2("PREDICATION_ID", encoding_name);
    Encodings second_index_encodings[1] = {encoding2};

    idx[1] = buildIndex<TValue, TIndexMap>("./semmeddb/pa1.csv", second_index_encodings, 1, 1);
    init_buffer(1);

    Encodings encoding3("SENTENCE_ID", encoding_name);

    Encodings third_index_encodings[1] = {encoding3};  

    idx[2] = buildIndex<TValue, TIndexMap>("./semmeddb/sp1.csv", third_index_encodings, 1, 2);
    init_buffer(2);

    Encodings encoding4("PREDICATION_ID", encoding_name);

    Encodings fourth_index_encodings[1] = {encoding4};
    
    idx[3] = buildIndex<TValue, TIndexMap>("./semmeddb/sp2.csv", fourth_index_encodings, 1, 3);
    init_buffer(3);


    Encodings encoding5("CONCEPT_SEMTYPE_ID", encoding_name);

    Encodings fifth_index_encodings[1] = {encoding5};
    
    idx[4] = buildIndex<TValue, TIndexMap>("./semmeddb/pa2.csv", fifth_index_encodings, 1, 4);
    init_buffer(4);

    Encodings encoding6("CONCEPT_ID", encoding_name);

    Encodings sixth_index_encodings[1] = {encoding6};
    
    idx[5] = buildIndex<TValue, TIndexMap>("./semmeddb/cs2.csv", sixth_index_encodings, 1, 5);
    init_buffer(5);

}


template <typename TValue, typename TIndexMap>
void semmeddb_create_optimal_indices() {


    Encodings encoding1("CONCEPT_SEMTYPE_ID", ENCODING_BYTE_ALIGNED_BITMAP);
    Encodings first_index_encodings[1] = {encoding1};

    idx[0] = buildIndex<TValue, TIndexMap>("./semmeddb/cs1.csv", first_index_encodings, 1, 0);
    init_buffer(0);

    Encodings encoding2("PREDICATION_ID", ENCODING_BYTE_ALIGNED_BITMAP);
    Encodings second_index_encodings[1] = {encoding2};

    idx[1] = buildIndex<TValue, TIndexMap>("./semmeddb/pa1.csv", second_index_encodings, 1, 1);
    init_buffer(1);

    Encodings encoding3("SENTENCE_ID", ENCODING_BYTE_ALIGNED_BITMAP);

    Encodings third_index_encodings[1] = {encoding3};  

    idx[2] = buildIndex<TValue, TIndexMap>("./semmeddb/sp1.csv", third_index_encodings, 1, 2);
    init_buffer(2);

    Encodings encoding4("PREDICATION_ID", ENCODING_HUFFMAN);

    Encodings fourth_index_encodings[1] = {encoding4};
    
    idx[3] = buildIndex<TValue, TIndexMap>("./semmeddb/sp2.csv", fourth_index_encodings, 1, 3);
    init_buffer(3);

    Encodings encoding5("CONCEPT_SEMTYPE_ID", ENCODING_HUFFMAN);

    Encodings fifth_index_encodings[1] = {encoding5};
    
    idx[4] = buildIndex<TValue, TIndexMap>("./semmeddb/pa2.csv", fifth_index_encodings, 1, 4);
    init_buffer(4);

    Encodings encoding6("CONCEPT_ID", ENCODING_BYTE_ALIGNED_BITMAP);

    Encodings sixth_index_encodings[1] = {encoding6};
    
    idx[5] = buildIndex<TValue, TIndexMap>("./semmeddb/cs2.csv", sixth_index_encodings, 1, 5);
    init_buffer(5);
}




template <typename TValue, typename TIndexMap>
int load(int database, int compression) {



    if (database == SEMMEDDB) {
        if (compression == OPTIMAL_COMPRESSION) {
            semmeddb_create_optimal_indices<TValue, TIndexMap>();
            return 1;
        }
        else if (compression >= 0 && compression <= NUM_ENCODING_TYPES_SUPPORTED) {
            semmeddb_create_indices<TValue, TIndexMap>(compression);
            return 1;
        }
    }
    else if (database == PUBMED_MESH_ONLY) {
        if (compression == ENCODING_BYTE_ALIGNED_BITMAP) {
            return 0;
        }

        if (compression == OPTIMAL_COMPRESSION) {
            pubmed_create_optimal_indices<TValue, TIndexMap>(DT1_MESH, DT2_MESH);
            return 1;
        }
        else if (compression >= 0 && compression <= NUM_ENCODING_TYPES_SUPPORTED) {
            pubmed_create_indices<TValue, TIndexMap>(compression, DT1_MESH, DT2_MESH);
            return 1;
        }
    }
    else if (database == PUBMED_MESH_PLUS_SUPP) {
        if (compression == ENCODING_BYTE_ALIGNED_BITMAP) {
            return 0;
        }
        
        if (compression == OPTIMAL_COMPRESSION) {
            pubmed_create_optimal_indices<TValue, TIndexMap>(DT1_TAG, DT2_TAG);
            return 1;
        }
        else if (compression >= 0 && compression <= NUM_ENCODING_TYPES_SUPPORTED) {
            pubmed_create_indices<TValue, TIndexMap>(compression, DT1_TAG, DT2_TAG);
            return 1;
        }
    }


	return 0;
}

#endif