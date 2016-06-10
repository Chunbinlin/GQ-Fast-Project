#ifndef q1_opt_0threads_17495979_
#define q1_opt_0threads_17495979_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 1
#define BUFFER_POOL_SIZE 1

using namespace std;

static int* R;
static int* RC;

uint64_t*** index2_col0_buffer;
uint64_t*** index2_col1_buffer;
uint64_t*** index3_col0_buffer;
uint64_t*** index3_col1_buffer;

extern inline void q1_opt_0threads_17495979_term1_col0_decode_BB(unsigned char* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) __attribute__((always_inline));

extern inline void q1_opt_0threads_17495979_doc2_col0_decode_BB(unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) __attribute__((always_inline));

void q1_opt_0threads_17495979_term1_col0_decode_BB(unsigned char* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) {

    cerr << "enter q1 opt term1 col0 decode bb\n";
	index2_col0_buffer[0][0][0] = 0;
    cerr << "c1\n";
	int shiftbits = 0;
	do {
		term1_col0_bytes--;
		uint32_t next_seven_bits = *term1_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		index2_col0_buffer[0][0][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*term1_col0_ptr++ & 128));
	term1_fragment_size++;
    cerr << "c2\n";
	while (term1_col0_bytes > 0) {
		cerr << "c3\n";
		shiftbits = 0;
		uint32_t result = 0;

		do {

			term1_col0_bytes--;
			uint32_t next_seven_bits = *term1_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*term1_col0_ptr++ & 128));
		index2_col0_buffer[0][0][term1_fragment_size] = index2_col0_buffer[0][0][term1_fragment_size-1]+1+result;
		term1_fragment_size++;
		cerr << "end c3\n";
	}
	cerr << "exit q1 opt term1 col0 decode bb\n";
}

void q1_opt_0threads_17495979_doc2_col0_decode_BB(unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) {

	index3_col0_buffer[0][0][0] = 0;

	int shiftbits = 0;
	do {
		doc2_col0_bytes--;
		uint32_t next_seven_bits = *doc2_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		index3_col0_buffer[0][0][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*doc2_col0_ptr++ & 128));
	doc2_fragment_size++;

	while (doc2_col0_bytes > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			doc2_col0_bytes--;
			uint32_t next_seven_bits = *doc2_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*doc2_col0_ptr++ & 128));
		index3_col0_buffer[0][0][doc2_fragment_size] = index3_col0_buffer[0][0][doc2_fragment_size-1]+1+result;
		doc2_fragment_size++;
	}
}

extern "C" int* q1_opt_0threads_17495979(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

    cerr << "check 1\n";
	max_frag = metadata.idx_max_fragment_sizes[2];
	cerr << "max frag = " << max_frag << "\n";
	cerr << "check 1b\n";
	uint64_t*** index2_col0_buffer = buffer_arrays[2][0];
	for (int i=0; i<NUM_THREADS; i++) {
		index2_col0_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index2_col0_buffer[i][j] = new uint64_t[max_frag];
			for (int k=0; k<max_frag; k++) {
                index2_col0_buffer[i][j][k] = 0;
			}
		}
	}
	cerr << "check 2\n";
	uint64_t*** index2_col1_buffer = buffer_arrays[2][1];
	for (int i=0; i<NUM_THREADS; i++) {
		index2_col1_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index2_col1_buffer[i][j] = new uint64_t[max_frag];
		}
	}


	cerr << "check 3\n";
	max_frag = metadata.idx_max_fragment_sizes[3];
	cerr << "check 3b\n";
	uint64_t*** index3_col0_buffer = buffer_arrays[3][0];
	for (int i=0; i<NUM_THREADS; i++) {
		index3_col0_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index3_col0_buffer[i][j] = new uint64_t[max_frag];
		}
	}
	cerr << "check 4\n";
	uint64_t*** index3_col1_buffer = buffer_arrays[3][1];
	for (int i=0; i<NUM_THREADS; i++) {
		index3_col1_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index3_col1_buffer[i][j] = new uint64_t[max_frag];
		}
	}

	cerr << "check5\n";
	RC = new int[metadata.idx_domains[3][0]]();
	R = new int[metadata.idx_domains[3][0]]();


	uint64_t doc1_list[1];
	doc1_list[0] = 17495979;

	cerr << "check6\n";
	for (int doc1_it = 0; doc1_it<1; doc1_it++) {
        cerr << "check a\n";
		uint64_t doc1_col0_element = doc1_list[doc1_it];

		uint32_t* row_term1 = idx[2]->index_map[doc1_col0_element];
		uint32_t term1_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_term1[0];
		if(term1_col0_bytes) {
            cerr << "check b\n";
			unsigned char* term1_col0_ptr = &(idx[2]->fragment_data[0][row_term1[0]]);
			uint32_t term1_fragment_size = 0;
			q1_opt_0threads_17495979_term1_col0_decode_BB(term1_col0_ptr, term1_col0_bytes, term1_fragment_size);

			for (uint32_t term1_it = 0; term1_it < term1_fragment_size; term1_it++) {
                cerr << "check c\n";
				uint32_t term1_col0_element = index2_col0_buffer[0][0][term1_it];

				uint32_t* row_doc2 = idx[3]->index_map[term1_col0_element];
				uint32_t doc2_col0_bytes = idx[3]->index_map[term1_col0_element+1][0] - row_doc2[0];
				if(doc2_col0_bytes) {

					unsigned char* doc2_col0_ptr = &(idx[3]->fragment_data[0][row_doc2[0]]);
					uint32_t doc2_fragment_size = 0;
					q1_opt_0threads_17495979_doc2_col0_decode_BB(doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

					for (uint32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++){
                        cerr << "check d\n";
						uint32_t doc2_col0_element = index3_col0_buffer[0][0][doc2_it];
						RC[doc2_col0_element] = 1;
						R[doc2_col0_element] += 1;
						cerr << "end d\n";
					}
				}
				cerr << "end c\n";
			}
		}
		cerr << "end a\n";
	}

    cerr << "check end\n";
	for (int j=0; j<NUM_THREADS; j++) {
		for (int k=0; k<BUFFER_POOL_SIZE; k++) {
			delete[] index2_col0_buffer[j][k];
		}
		delete[] index2_col0_buffer[j];
	}
	for (int j=0; j<NUM_THREADS; j++) {
		for (int k=0; k<BUFFER_POOL_SIZE; k++) {
			delete[] index2_col1_buffer[j][k];
		}
		delete[] index2_col1_buffer[j];
	}
	for (int j=0; j<NUM_THREADS; j++) {
		for (int k=0; k<BUFFER_POOL_SIZE; k++) {
			delete[] index3_col0_buffer[j][k];
		}
		delete[] index3_col0_buffer[j];
	}
	for (int j=0; j<NUM_THREADS; j++) {
		for (int k=0; k<BUFFER_POOL_SIZE; k++) {
			delete[] index3_col1_buffer[j][k];
		}
		delete[] index3_col1_buffer[j];
	}


    cerr << "check return\n";
	*null_checks = RC;
	return R;

}

#endif

