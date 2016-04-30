#ifndef join_q5_bitmap_threaded_
#define join_q5_bitmap_threaded_

#include <iostream>
#include "fastr_index.hpp"
#include "global_vars.hpp"

#define NUM_THREADS 4
#define NUM_BUFFERS 5
#define BUFFER_POOL_SIZE 1

using namespace std;

struct args_s{
    uint32_t start;
    uint32_t end;
    int thread_id;
};

// Thread param
args_s arguments[NUM_THREADS];

int* q5_bitmap_threaded_dt1_huffman_tree_array;
bool* q5_bitmap_threaded_dt1_huffman_terminator_array; 

int* q5_bitmap_threaded_dt2_huffman_tree_array;
bool* q5_bitmap_threaded_dt2_huffman_terminator_array; 

uint32_t* q5_bitmap_threaded_authors_bits_info;
uint32_t* q5_bitmap_threaded_year_bitmap_bits_info;

int* RC_bitmap;
double* R_bitmap;

extern inline void q5_bitmap_decode_threaded_BB_da1_docs(unsigned char* byte_pos, uint32_t bytes_size, int & size) __attribute__((always_inline));
extern inline void q5_bitmap_decode_threaded_BB_dt1_terms(unsigned char* byte_pos, uint32_t bytes_size, int & size) __attribute__((always_inline));
extern inline void q5_bitmap_decode_threaded_BB_dt2_docs(int thread_id, unsigned char* byte_pos, uint32_t bytes_size, int & size) __attribute__((always_inline));
extern inline void q5_bitmap_decode_threaded_BD_da2_authors(int thread_id, unsigned char * byte_pos, int reps) __attribute__((always_inline));
extern inline void q5_bitmap_decode_threaded_BD_year_bitmap(uint32_t* byte_pos, int & year) __attribute__((always_inline));
extern inline void q5_bitmap_decode_threaded_Huffman_dt1_fre(unsigned char* byte_pos, int size) __attribute__((always_inline));
extern inline void q5_bitmap_decode_threaded_Huffman_dt2_fre(int thread_id, unsigned char* byte_pos, int size) __attribute__((always_inline));

void q5_bitmap_decode_threaded_BB_da1_docs(unsigned char* byte_pos, uint32_t bytes_size, int & size) {
//    // cerr << "\nbegin q5 decode BB da1 docs function\n";
    buffer_arrays[0][0][0][0][0] = 0;
    

    // Calculate first decoding
    int shiftbits = 0;
    do {
        // // cerr << "byte pos = " << *byte_pos << "\n";
        bytes_size--;
        uint32_t next_seven_bits = *byte_pos & 127;
        next_seven_bits = next_seven_bits << shiftbits;
        buffer_arrays[0][0][0][0][0] |= next_seven_bits;
        shiftbits += 7;
    
    }  while (!(*byte_pos++ & 128));
    size++;
    // Subsequent decodings are added to previous
    while (bytes_size > 0) {    

        shiftbits = 0;
        uint32_t result = 0;
        // Are we at the last byte?
        do {
            // // cerr << "byte pos = " << *byte_pos << "\n";
               
            bytes_size--;
             // Extract lower 7 bits from byte
            uint32_t next_seven_bits = *byte_pos & 127;
            next_seven_bits = next_seven_bits << shiftbits;
            result |= next_seven_bits;

            shiftbits += 7;

        } while (!(*byte_pos++ & 128));
        buffer_arrays[0][0][0][0][size] = buffer_arrays[0][0][0][0][size-1]+1+result;
        size++;
    }
    // // cerr << "Exit decode term\n";
//    // cerr << "\nend q5 decode BB da1 docs function\n";
}

void q5_bitmap_decode_threaded_BB_dt1_terms(unsigned char* byte_pos, uint32_t bytes_size, int & size) {
//    // cerr << "\nbegin q5 decode BB da1 docs function\n";
    buffer_arrays[2][0][0][0][0] = 0;
    

    // Calculate first decoding
    int shiftbits = 0;
    do {
        // // cerr << "byte pos = " << *byte_pos << "\n";
        bytes_size--;
        uint32_t next_seven_bits = *byte_pos & 127;
        next_seven_bits = next_seven_bits << shiftbits;
        buffer_arrays[2][0][0][0][0] |= next_seven_bits;
        shiftbits += 7;
    
    }  while (!(*byte_pos++ & 128));
    size++;
    // Subsequent decodings are added to previous
    while (bytes_size > 0) {    

        shiftbits = 0;
        uint32_t result = 0;
        // Are we at the last byte?
        do {
            // // cerr << "byte pos = " << *byte_pos << "\n";
               
            bytes_size--;
             // Extract lower 7 bits from byte
            uint32_t next_seven_bits = *byte_pos & 127;
            next_seven_bits = next_seven_bits << shiftbits;
            result |= next_seven_bits;

            shiftbits += 7;

        } while (!(*byte_pos++ & 128));
        buffer_arrays[2][0][0][0][size] = buffer_arrays[2][0][0][0][size-1]+1+result;
        size++;
    }
    // // cerr << "Exit decode term\n";
//    // cerr << "\nend q5 decode BB da1 docs function\n";
}

void q5_bitmap_decode_threaded_BB_dt2_docs(int thread_id, unsigned char* byte_pos, uint32_t bytes_size, int & size) {
//    // cerr << "\nbegin q5 decode BB da1 docs function\n";
    buffer_arrays[3][0][thread_id][0][0] = 0;


    // Calculate first decoding
    int shiftbits = 0;
    do {
        // // cerr << "byte pos = " << *byte_pos << "\n";
        bytes_size--;
        uint32_t next_seven_bits = *byte_pos & 127;
        next_seven_bits = next_seven_bits << shiftbits;
        buffer_arrays[3][0][thread_id][0][0] |= next_seven_bits;
        shiftbits += 7;
    
    }  while (!(*byte_pos++ & 128));
    size++;
    // Subsequent decodings are added to previous
    while (bytes_size > 0) {    

        shiftbits = 0;
        uint32_t result = 0;
        // Are we at the last byte?
        do {
            // // cerr << "byte pos = " << *byte_pos << "\n";
               
            bytes_size--;
             // Extract lower 7 bits from byte
            uint32_t next_seven_bits = *byte_pos & 127;
            next_seven_bits = next_seven_bits << shiftbits;
            result |= next_seven_bits;

            shiftbits += 7;

        } while (!(*byte_pos++ & 128));
        buffer_arrays[3][0][thread_id][0][size] = buffer_arrays[3][0][thread_id][0][size-1]+1+result;
        size++;
    }
    // // cerr << "Exit decode term\n";
//    // cerr << "\nend q5 decode BB da1 docs function\n";
}


void q5_bitmap_decode_threaded_BD_da2_authors(int thread_id, unsigned char * byte_pos, int reps) {

    int bit_pos = 0;
    for (int i=0; i<reps; i++) {
        uint32_t encoded_value = q5_bitmap_threaded_authors_bits_info[1] << bit_pos;
            // Read current + next 7 bytes
        uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(byte_pos);
        encoded_value &= *next_8_ptr;
        encoded_value >>= bit_pos;
   
        // if bits_size equal x, then we want x 1-bits in a row
        byte_pos += (bit_pos + q5_bitmap_threaded_authors_bits_info[0]) / 8;
        bit_pos = (bit_pos + q5_bitmap_threaded_authors_bits_info[0]) % 8;
             // // // // cerr << "encoded_value = " << encoded_value << "\n";
        buffer_arrays[4][0][thread_id][0][i] = encoded_value;
            // // // // cerr << "decoded #" << i << " = " << decoded[i] << "\n";
    
    }
}

void q5_bitmap_decode_threaded_BD_year_bitmap(uint32_t* byte_pos, int & year) {

    year = q5_bitmap_threaded_year_bitmap_bits_info[1];
        //    // cerr << "encoded = " << encoded_value << "\n";
            // Read current + next 7 bytes
    year &= *byte_pos;

}

void q5_bitmap_decode_threaded_Huffman_dt1_fre(unsigned char* byte_pos, int size) {
    bool* terminate_start = &(q5_bitmap_threaded_dt1_huffman_terminator_array[0]);
    int* tree_array_start = &(q5_bitmap_threaded_dt1_huffman_tree_array[0]);


    int mask = 0x100;
    // // cerr << "in huff decode fre1, q5_huff_threaded_decoded_dt1_size= " << q5_huff_threaded_decoded_dt1_size << "\n";
    for (int i=0; i<size; i++) {

        bool* terminator_array = terminate_start;
        int* tree_array = tree_array_start;

        while (!*terminator_array) {
        //  // cerr << " byte pos = " << *byte_pos << "\n";
            char direction = *byte_pos & (mask >>= 1);               
        //   // cerr << "direction = " << (uint32_t) direction << "\n";
                        
            if (mask == 1) {
                mask = 0x100;
                byte_pos++;
            } 
       //     // cerr << "terminator array ptr = " << (uint64_t) terminator_array << "\n";
            terminator_array += *tree_array;
            tree_array += *tree_array;

            
            if (direction) {
                terminator_array++;
                tree_array++;
            }
        //   std::// // cerr << "huffman_pos now = " << huffman_pos << "\n";
        //    // cerr << "terminator array ptr now = " << (uint64_t) terminator_array << "\n";
        //    std::// // cerr << "loop iter end\n";
        }
    // // cerr << "end decode next\n";
    //    // cerr << "q2 decoded dt1 fre[" << i << "]= " << *tree_array << "\n"; 
        buffer_arrays[2][1][0][0][i] = *tree_array;

    }
} 

void q5_bitmap_decode_threaded_Huffman_dt2_fre(int thread_id, unsigned char* byte_pos, int size) {
    bool* terminate_start = &(q5_bitmap_threaded_dt2_huffman_terminator_array[0]);
    int* tree_array_start = &(q5_bitmap_threaded_dt2_huffman_tree_array[0]);


    int mask = 0x100;
//    // cerr << "in huff decode fre1, q5_huff_threaded_decoded_dt1_size= " << q5_huff_threaded_decoded_dt1_size << "\n";
    for (int i=0; i<size; i++) {

        bool* terminator_array = terminate_start;
        int* tree_array = tree_array_start;

        while (!*terminator_array) {
        //  // cerr << " byte pos = " << *byte_pos << "\n";
            char direction = *byte_pos & (mask >>= 1);               
        //   // cerr << "direction = " << (uint32_t) direction << "\n";
                        
            if (mask == 1) {
                mask = 0x100;
                byte_pos++;
            } 
       //     // cerr << "terminator array ptr = " << (uint64_t) terminator_array << "\n";
            terminator_array += *tree_array;
            tree_array += *tree_array;

            
            if (direction) {
                terminator_array++;
                tree_array++;
            }
        //   std::// // cerr << "huffman_pos now = " << huffman_pos << "\n";
        //    // cerr << "terminator array ptr now = " << (uint64_t) terminator_array << "\n";
        //    std::// // cerr << "loop iter end\n";
        }
    // // cerr << "end decode next\n";
   //     // cerr << "q2 decoded dt1 fre[" << i << "]= " << *tree_array << "\n"; 
        buffer_arrays[3][1][thread_id][0][i] = *tree_array;

    }
} 

void* pthread_bitmap_worker(void* arguments) {

    args_s* args = (args_s *) arguments;
    
    uint32_t it2 = args->start;
    uint32_t it2_end = args->end;

    int thread_id = args->thread_id;

    for (; it2 < it2_end; it2++) {
        // cerr << "buffer_arrays_DT1[0][0][it2] = " << buffer_arrays[2][0][0][0][it2] << "\n";
        unsigned char fre1 = buffer_arrays[2][1][0][0][it2];

        // cerr << "fre1 = " << (int) fre1 << "\n";
        int t1 = buffer_arrays[2][0][0][0][it2];
        uint32_t * r4 = idx[3]->index_map[t1];
        uint32_t d2_bytes = idx[3]->index_map[t1+1][0] - r4[0];
        // cerr << "d2_bytes = " << d2_bytes << "\n";  
                // All possible terms appear in DT2, so no need for if statement

        unsigned char* fre2_ptr = &(idx[3]->fragment_data[1][r4[1]]);
       
                // i4 will find the docs
        unsigned char* d2_ptr = &(idx[3]->fragment_data[0][r4[0]]);

        int d2_size = 0;
        q5_bitmap_decode_threaded_BB_dt2_docs(thread_id, d2_ptr, d2_bytes, d2_size);
        q5_bitmap_decode_threaded_Huffman_dt2_fre(thread_id, fre2_ptr, d2_size);
        // cerr << "q5_bitmap_decoded_threaded_dt2_size = " << d2_size << "\n";
        for (int it3=0; it3 < d2_size; it3++) {
            // cerr << "q5_bitmap_decoded_threaded_dt2_docs[it3] = " << buffer_arrays[3][0][thread_id][0][it3] << "\n";
            unsigned char fre2 = buffer_arrays[3][1][thread_id][0][it3];

            int d2 = buffer_arrays[3][0][thread_id][0][it3];
            uint32_t * r2 = idx[1]->index_map[d2];
            uint32_t* year_bitmap_ptr = (uint32_t *) &(idx[1]->fragment_data[0][r2[0]]);
            int year_bitmap;
            q5_bitmap_decode_threaded_BD_year_bitmap(year_bitmap_ptr, year_bitmap);
            // cerr << "fre2 = " << (int) fre2 << "\n";         

            uint32_t * r5 = idx[4]->index_map[d2];
            uint32_t a2_bytes = idx[4]->index_map[d2+1][0] - r5[0];
                    // Not all docs exist, so we need to check
            // cerr << "a2_bytes = " << a2_bytes << "\n";
            if (a2_bytes) {

                unsigned char* a2_ptr = &(idx[4]->fragment_data[0][r5[0]]);

                int it4_end = a2_bytes*8/q5_bitmap_threaded_authors_bits_info[0];
                // cerr << "it4 end = " << it4_end << "\n";
                q5_bitmap_decode_threaded_BD_da2_authors(thread_id, a2_ptr, it4_end);
                    //    // cerr << "q5_bitmap_decoded_threaded_da2_size = " << q5_bitmap_decoded_threaded_da2_size << "\n";
                for (int it4=0; it4<it4_end; it4++) {
                    
                            // // cerr << "a2 = " << a2 << "\n";
                    int curr = buffer_arrays[4][0][thread_id][0][it4];
                    // cerr << "q5_bitmap_decoded_threaded_da2_authors[it4] = " <<  curr << "\n";
                    RC_bitmap[curr] = 1;
                    pthread_spin_lock(&spin_locks[4][curr]);
                    R_bitmap[curr] += (double)(fre1 * fre2)/(2017 - year_bitmap);
                    pthread_spin_unlock(&spin_locks[4][curr]);
                            // // cerr << "adding " << (fre1*fre2)/(2015-year_bitmap) << " at " << a2 << "; is now " << R[0][a2] << "\n";
                    // cerr << "end loop four\n";
                }                   
            }
            // cerr << "end loop three\n";
        }               
        // cerr << "end loop two\n";
    }
}


extern "C" int* q5_bitmap_threaded(int** null_checks) {

    benchmark_t1 = chrono::steady_clock::now();


    //// cerr << "in function\n";
    // Allocate buffers
    for (int i=0; i<NUM_BUFFERS; i++) {
        int max_frag = metadata.idx_max_fragment_sizes[i];
        for (int j=0; j<metadata.idx_num_encodings[i]; j++) {
            for (int k=0; k<NUM_THREADS; k++) {
                buffer_arrays[i][j][k] = new int*[BUFFER_POOL_SIZE];
                for (int l=0; l<BUFFER_POOL_SIZE; l++) {
                    buffer_arrays[i][j][k][l] = new int[max_frag];
                }
            }
        }
    }
    // cerr << "next\n";

    RC_bitmap = new int[metadata.idx_domains[4][0]]();
    // cerr << "next2\n";
    
    R_bitmap = new double[metadata.idx_domains[4][0]]();

    // cerr << "next3\n";
	q5_bitmap_threaded_dt1_huffman_tree_array = idx[2]->huffman_tree_array[1];
	q5_bitmap_threaded_dt1_huffman_terminator_array = idx[2]->huffman_terminator_array[1];

	q5_bitmap_threaded_dt2_huffman_tree_array = idx[3]->huffman_tree_array[1];
	q5_bitmap_threaded_dt2_huffman_terminator_array = idx[3]->huffman_terminator_array[1];

	q5_bitmap_threaded_year_bitmap_bits_info = idx[1]->dict[0]->bits_info;
    q5_bitmap_threaded_authors_bits_info = idx[4]->dict[0]->bits_info;

    // cerr << "next4\n";
    // DA1
	uint32_t* r1 = idx[0]->index_map[4945389];
	uint32_t d1_bytes = idx[0]->index_map[4945389+1][0] - r1[0];
    // cerr << "next5\n";
	
	unsigned char* d1_ptr = &(idx[0]->fragment_data[0][r1[0]]);
	
    // cerr << "d1_bytes = " << d1_bytes << "\n";
    int d1_size = 0;
	q5_bitmap_decode_threaded_BB_da1_docs(d1_ptr, d1_bytes, d1_size);
    // cerr << "d1_size = " << d1_size << "\n";
	for (int it1=0; it1 < d1_size; it1++) {
        // cerr << "buffer_arrays_DA1[0][0][it1] = " << buffer_arrays[0][0][0][0][it1] << "\n";
        // DY

        int d1 = buffer_arrays[0][0][0][0][it1];
		uint32_t * r3 = idx[2]->index_map[d1];
        uint32_t t1_bytes = idx[2]->index_map[d1+1][0] - r3[0];
         // cerr << "t1_bytes = " << t1_bytes << "\n";
		if (t1_bytes) {

            unsigned char* fre1_ptr = &(idx[2]->fragment_data[1][r3[1]]);
		     	
			// i2 will find the terms
			unsigned char* t1_ptr = &(idx[2]->fragment_data[0][r3[0]]);
			
            int t1_size = 0;
			q5_bitmap_decode_threaded_BB_dt1_terms(t1_ptr, t1_bytes, t1_size);
			q5_bitmap_decode_threaded_Huffman_dt1_fre(fre1_ptr, t1_size);
            // cerr << "t1size = " << t1_size << "\n";
			uint32_t thread_size = t1_size/NUM_THREADS;

            int position = 0;
            for (int i=0; i<NUM_THREADS; i++) {

                arguments[i].start = position;
                position += thread_size;

                arguments[i].end = position;
                arguments[i].thread_id = i;

            }
            arguments[NUM_THREADS-1].end = t1_size;

            for (int i=0; i<NUM_THREADS; i++) {
                pthread_create(&threads[i], NULL, &pthread_bitmap_worker, (void *) &arguments[i]);
            }
            // cerr << "a8\n";
            // Main function waits until all threads have completed
            for (int i=0; i<NUM_THREADS; i++) {
                pthread_join(threads[i], NULL);
            }


		}
		//  // cerr << "end loop one\n";
	}	

    //// cerr << "deallocating\n";

    // Deallocate buffers
    for (int i=0; i<NUM_BUFFERS; i++) {
        for (int j=0; j<metadata.idx_num_encodings[i]; j++) {
            for (int k=0; k<NUM_THREADS; k++) {
                for (int l=0; l<BUFFER_POOL_SIZE; l++) {
                    delete[] buffer_arrays[i][j][k][l];
                }
                delete[] buffer_arrays[i][j][k];
            }
        }
    }

    //// cerr << "returning\n";

    *null_checks = RC_bitmap;
	return R_bitmap;
}

#endif
