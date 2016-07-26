#ifndef graph_index_hpp_
#define graph_index_hpp_

#include <iostream>
#include <unordered_map>
#include "../dictionary.hpp"
#include "../huffman.hpp"
#include "../byte_aligned_bitmap.hpp"
#include "../encodings.hpp"

template<typename T>
class graph_index
{

public:

    const uint64_t domain_size;

    int huffman_tree_array_size;

    unordered_map<T, unsigned char*> index_map;

    int fragment_encoding_type;

    // Dictionary related
    dictionary * dict;

    // Huffman related
    int* huffman_tree_array;
    bool* huffman_terminator_array;


    hash_index(int ds, int htas) : domain_size(ds)
    {

    }

    // Destructor

    ~fastr_index()
    {

        delete[] huffman_tree_array;
        delete[] huffman_terminator_array;
        delete dict;

    }

    void set_index_map(unordered_map<T, unsigned char*> new_map)
    {
        index_map = new_map;
    }

    unsigned char* get(T row)
    {
        return index_map[row];
    }

    void set_dictionary(dictionary* d)
    {

        dict = d;
    }

    void set_huffman(int* hta, bool* hterma, int htas)
    {
        huffman_tree_array = hta;
        huffman_tree_array_size = htas;
        huffman_terminator_array = hterma;

    }

};

#endif
