#ifndef serialization_
#define serialization_

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include "global_vars.hpp"
#include <fstream>
void save_index(fastr_index** s, const char * filename){
    
    std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
    // make an archive
    std::ofstream ofs(filename);
    boost::archive::text_oarchive oa(ofs);
    
    oa << num_loaded_indices;
    
    for (int i=0; i<num_loaded_indices; i++) {
        oa << s[i]->domain_size;
        oa << s[i]->num_fragment_data;
        //cerr << "num frag data = " << s[i]->num_fragment_data;
        for (int j=0; j<s[i]->num_fragment_data; j++) {
            oa << s[i]->fragment_data_length[j];
        }
        for (int j=0; j<s[i]->num_fragment_data; j++) {
            oa << s[i]->huffman_tree_array_size[j];
        }

        oa << s[i]->load_flag;
        oa << *(s[i]);

    }
    
   	oa << metadata;

    std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
    
    std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);

    std::cout << "Saving indices took " << time_span.count() << " seconds.";
    std::cout << std::endl;




}

void load_index(fastr_index** s, const char * filename) {
    //cerr << "loading database\n";
    std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
    std::ifstream ifs(filename);
    boost::archive::text_iarchive ia(ifs);
    // read class state from archive
    ia >> num_loaded_indices;
    for (int i=0; i<num_loaded_indices; i++) {
        cerr << "loading index " << i << "\n";
   		int domain_size, num_fragment_data;
        ia >> domain_size;
        ia >> num_fragment_data;
    //    cerr << "num fragment data = " << num_fragment_data << "\n";
        unsigned int* fragment_data_length = new unsigned int[num_fragment_data];
        for (int j=0; j<num_fragment_data; j++) {
        //    cerr << "loading fragment_data_length " << j << "\n";
            ia >> fragment_data_length[j];
        }
        int* huffman_tree_array_size = new int[num_fragment_data];
        for (int j=0; j<num_fragment_data; j++) {
        //    cerr << "loading huff tree array size " << j << "\n";
            ia >> huffman_tree_array_size[j];
        }

        int load_flag;
        ia >> load_flag;
     //   cerr << "creating index\n";
        fastr_index* temp_index = new fastr_index(domain_size, num_fragment_data, fragment_data_length, huffman_tree_array_size, load_flag);
    //    cerr << "copying index\n";
        ia >> *temp_index;
     //   cerr << "setting index\n";
        idx[i] = temp_index;
    }
    
    ia >> metadata;

    for (int i=0; i<num_loaded_indices; i++) {
    	init_buffer(i);
    }

    std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
    
    std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);

    std::cout << "Loading indices took " << time_span.count() << " seconds.";
    std::cout << std::endl;


}
   #endif
