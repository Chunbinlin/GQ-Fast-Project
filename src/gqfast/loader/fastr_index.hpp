#ifndef  fastr_index_hpp_
#define fastr_index_hpp_

#include <iostream>  
#include "huffman.hpp"
#include "byte_aligned_bitmap.hpp"
#include "dictionary.hpp"
#include "encodings.hpp"
#include <boost/serialization/array.hpp>
#include <boost/serialization/version.hpp>
#include <cstring>
#include <utility>
#include <memory>

template<typename T>
class fastr_index {

public:
    


    const uint64_t domain_size;
    const int num_fragment_data;
    const unsigned int * fragment_data_length;
    const int* huffman_tree_array_size;
    int load_flag;

    T** index_map;       // Points to values of starts and stops for variables 

    int * fragment_encoding_type;
    unsigned char** fragment_data;
    

    // Dictionary related 
    dictionary ** dict;

    // Huffman related 
    int**  huffman_tree_array;    
    bool** huffman_terminator_array;

    


    fastr_index(int ds, int nfd, unsigned int* fdl, int* htas, bool flag) : domain_size(ds), num_fragment_data(nfd), 
    fragment_data_length(fdl), huffman_tree_array_size(htas), load_flag(flag) { 
        index_map = new T*[domain_size+1];
        for (uint64_t i=0; i<domain_size+1; i++) {
            index_map[i] = new T[num_fragment_data]();
        }

        fragment_encoding_type = new int[num_fragment_data]();

        fragment_data = new unsigned char*[num_fragment_data];
        for (int i=0; i<num_fragment_data; i++) {
            fragment_data[i] = new unsigned char[fragment_data_length[i]]();
        }

        dict = new dictionary*[num_fragment_data];
        for (int i=0; i<num_fragment_data; i++) {
            dict[i] = new dictionary();
        }

        huffman_tree_array = new int*[num_fragment_data];
        huffman_terminator_array = new bool*[num_fragment_data];
        for (int i=0; i<num_fragment_data; i++) {
            huffman_tree_array[i] = new int[huffman_tree_array_size[i]]();
            huffman_terminator_array[i] = new bool[huffman_tree_array_size[i]]();
        }
    }

        // Constructor    
    fastr_index(int ds, int nfd, unsigned int* fdl, int* fet) : domain_size(ds), 
    num_fragment_data(nfd), fragment_data_length(fdl), fragment_encoding_type(fet) { 
        load_flag = 0;
                // only needs to assign variables from parameters
    }

     // Destructor     

    ~fastr_index() {

        for (int i=0; i<num_fragment_data; i++) {        
            delete[] fragment_data[i];
        }


        delete[] fragment_data;

        for (uint64_t i=0;i<domain_size;i++) {
            delete[] index_map[i];
        }   

        delete[] index_map;



            // Dictionaries and Huffman
        for (int i=0; i<num_fragment_data; i++) {
          //  if (fragment_encoding_type[i] == ENCODING_HUFFMAN) {     
                // May have problem with this one
                delete[] huffman_tree_array[i];

                delete[] huffman_terminator_array[i];
                
        //    }

        //    if (fragment_encoding_type[i] == ENCODING_BIT_ALIGNED_COMPRESSED) {
                delete dict[i];
        //    }
        }

        
        delete[] huffman_tree_array;
        delete[] huffman_terminator_array;
        delete[] dict;
 

        delete[] huffman_tree_array_size;

        delete[] fragment_encoding_type;
        delete[] fragment_data_length;
    }

    void set_index_map(T** new_map) {
        index_map = new_map;
    }

    void set_data(unsigned char** new_data) {
        fragment_data = new_data;
    }

    T* get(int row) {
        return index_map[row];
    }

    void set_dictionary(dictionary ** d) {
        dict = d;
    }

    void set_huffman(int** hta, bool** hterma, int* htas) {
        huffman_tree_array = hta;
        huffman_tree_array_size = htas;
        huffman_terminator_array = hterma; 
        /*
        for (int i=0; i<num_fragment_data; i++) {
            if (huffman_tree_array_size[i] == 0) {
                huffman_tree_array_size[i] = 1;
                huffman_tree_array[i] = new int[1]();
                huffman_terminator_array[i] = new bool[1]();
            }
        }
        */
    }

    template<class Archive>
    void serialize(Archive & ar, const unsigned int version) {

        //cerr << "loading index\n";
        for (uint64_t i=0; i<domain_size+1; i++) {
            for (int j=0; j<num_fragment_data; j++) {
                ar & index_map[i][j];
            }
        }
        //cerr << "loading fragment encoding\n";
        for (int i=0; i< num_fragment_data; i++) {
            ar & fragment_encoding_type[i];
        }
       
        //cerr << "loading fragment data\n";
        for (int i=0; i< num_fragment_data; i++) {
            for (T j=0; j<fragment_data_length[i]; j++) {
               ar & fragment_data[i][j]; 
            }
        }        
        
        //cerr << "loading dict\n";      
        for (int i=0; i<num_fragment_data; i++) {
            ar & *dict[i];  
        }
        
        //cerr << "loading huffman tree array\n";
        for (int i=0; i<num_fragment_data; i++) {
            for (int j=0; j<huffman_tree_array_size[i]; j++) {
                ar & huffman_tree_array[i][j];
            }
        }

        //cerr << "loading huff term array\n";
        for (int i=0; i<num_fragment_data; i++) {
            for (int j=0; j<huffman_tree_array_size[i]; j++) {
                ar & huffman_terminator_array[i][j];
            }
        }      

    }

};


namespace boost { namespace serialization {
template<class Archive, class TIndexMap>
inline void save_construct_data(
    Archive & ar, const fastr_index<TIndexMap> * t, const unsigned int file_version
){
   
    uint64_t domain_size;
    int num_fragment_data;
    unsigned int * fragment_data_length;
    int* huffman_tree_array_size;

    // save data required to construct instance
    ar << t->domain_size;
    ar << t->num_fragment_data;
    for (int i=0; i<t->num_fragment_data; i++) {
        ar << t->fragment_data_length[i];
    }
    for (int i=0; i<t->num_fragment_data; i++) {
        ar << t->huffman_tree_array_size[i];
    }
    ar << t->load_flag;
   
}

template<class Archive, class TIndexMap>
inline void load_construct_data(
    Archive & ar, fastr_index<TIndexMap> * t, const unsigned int file_version
){
    // retrieve data from archive required to construct new instance
    uint64_t domain_size;
    int num_fragment_data;
    ar >> domain_size;
    ar >> num_fragment_data;

    unsigned int* fragment_data_length = new unsigned int[num_fragment_data];
    for (int i=0; i<num_fragment_data; i++) {
        ar >> fragment_data_length[i];
    }
    int* huffman_tree_array_size = new int[num_fragment_data];
    for (int i=0; i<num_fragment_data; i++) {
        ar >> huffman_tree_array_size[i];
    }
    int load_flag;
    ar >> load_flag;
    // invoke inplace constructor to initialize instance of my_class
    ::new(t)fastr_index<TIndexMap>(domain_size, num_fragment_data, fragment_data_length, huffman_tree_array_size, load_flag);
}

}}



#endif