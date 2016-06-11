#ifndef q1_array_0threads_17495979_
#define q1_array_0threads_17495979_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 1
#define BUFFER_POOL_SIZE 1

using namespace std;

static int* R;
static int* RC;

static uint64_t*** index2_col0_buffer;
static uint64_t*** index2_col1_buffer;
static uint64_t*** index3_col0_buffer;
static uint64_t*** index3_col1_buffer;

extern inline void q1_array_0threads_17495979_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) __attribute__((always_inline));

extern inline void q1_array_0threads_17495979_doc2_col0_decode_UA(uint32_t* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) __attribute__((always_inline));

void q1_array_0threads_17495979_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) {

	term1_fragment_size = term1_col0_bytes/4;

	for (uint32_t i=0; i<term1_fragment_size; i++) {
		index2_col0_buffer[0][0][i] = *term1_col0_ptr++;
	}
}

void q1_array_0threads_17495979_doc2_col0_decode_UA(uint32_t* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) {

	doc2_fragment_size = doc2_col0_bytes/4;

	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		index3_col0_buffer[0][0][i] = *doc2_col0_ptr++;
	}
}

extern "C" int* q1_array_0threads_17495979(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[2];
	index2_col0_buffer = buffer_arrays[2][0];
	for (int i=0; i<NUM_THREADS; i++) {
		index2_col0_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index2_col0_buffer[i][j] = new uint64_t[max_frag];
		}
	}
	index2_col1_buffer = buffer_arrays[2][1];
	for (int i=0; i<NUM_THREADS; i++) {
		index2_col1_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index2_col1_buffer[i][j] = new uint64_t[max_frag];
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[3];
	index3_col0_buffer = buffer_arrays[3][0];
	for (int i=0; i<NUM_THREADS; i++) {
		index3_col0_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index3_col0_buffer[i][j] = new uint64_t[max_frag];
		}
	}
	index3_col1_buffer = buffer_arrays[3][1];
	for (int i=0; i<NUM_THREADS; i++) {
		index3_col1_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index3_col1_buffer[i][j] = new uint64_t[max_frag];
		}
	}

	RC = new int[metadata.idx_domains[3][0]]();
	R = new int[metadata.idx_domains[3][0]]();


	uint64_t doc1_list[1];
	doc1_list[0] = 17495979;

	for (int doc1_it = 0; doc1_it<1; doc1_it++) {

		uint64_t doc1_col0_element = doc1_list[doc1_it];

		uint32_t* row_term1 = idx[2]->index_map[doc1_col0_element];
		uint32_t term1_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_term1[0];
		if(term1_col0_bytes) {

			uint32_t* term1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[2]->fragment_data[0][row_term1[0]]));
			uint32_t term1_fragment_size = 0;
			q1_array_0threads_17495979_term1_col0_decode_UA(term1_col0_ptr, term1_col0_bytes, term1_fragment_size);

			for (uint32_t term1_it = 0; term1_it < term1_fragment_size; term1_it++) {

				uint32_t term1_col0_element = index2_col0_buffer[0][0][term1_it];

				uint32_t* row_doc2 = idx[3]->index_map[term1_col0_element];
				uint32_t doc2_col0_bytes = idx[3]->index_map[term1_col0_element+1][0] - row_doc2[0];
				if(doc2_col0_bytes) {

					uint32_t* doc2_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[3]->fragment_data[0][row_doc2[0]]));
					uint32_t doc2_fragment_size = 0;
					q1_array_0threads_17495979_doc2_col0_decode_UA(doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

					for (uint32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {
						uint32_t doc2_col0_element = index3_col0_buffer[0][0][doc2_it];

						RC[doc2_col0_element] = 1;
						R[doc2_col0_element] += 1;
					}
				}
			}
		}
	}


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


	*null_checks = RC;
	return R;

}

#endif

