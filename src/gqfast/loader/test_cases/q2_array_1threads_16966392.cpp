#ifndef q2_array_1threads_16966392_
#define q2_array_1threads_16966392_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 10
#define BUFFER_POOL_SIZE 1

using namespace std;

static args_threading arguments[NUM_THREADS];

static uint32_t year1_col0_element;

static double* R;
static int* RC;

static uint64_t*** index1_col0_buffer;
static uint64_t*** index2_col0_buffer;
static uint64_t*** index3_col0_buffer;
static uint64_t*** index3_col1_buffer;

extern inline void q2_array_1threads_16966392_year1_col0_decode_UA(uint32_t* year1_col0_ptr) __attribute__((always_inline));

extern inline void q2_array_1threads_16966392_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) __attribute__((always_inline));

extern inline void q2_array_1threads_16966392_term1_col1_decode_UA(unsigned char* term1_col1_ptr, uint32_t term1_fragment_size) __attribute__((always_inline));

void* pthread_q2_array_1threads_16966392_worker(void* arguments);

extern inline void q2_array_1threads_16966392_doc2_col0_decode_UA_threaded(int thread_id, uint32_t* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) __attribute__((always_inline));

extern inline void q2_array_1threads_16966392_doc2_col1_decode_UA_threaded(int thread_id, unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) __attribute__((always_inline));

extern inline void q2_array_1threads_16966392_year2_col0_decode_UA(uint32_t* year2_col0_ptr, uint32_t & year2_col0_element) __attribute__((always_inline));

void q2_array_1threads_16966392_year1_col0_decode_UA(uint32_t* year1_col0_ptr) {

	year1_col0_element = *year1_col0_ptr;
}

void q2_array_1threads_16966392_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) {

	term1_fragment_size = term1_col0_bytes/4;

	for (uint32_t i=0; i<term1_fragment_size; i++) {
		index2_col0_buffer[0][0][i] = *term1_col0_ptr++;
	}
}

void q2_array_1threads_16966392_term1_col1_decode_UA(unsigned char* term1_col1_ptr, uint32_t term1_fragment_size) {

	for (uint32_t i=0; i<term1_fragment_size; i++) {
		index2_col1_buffer[0][0][i] = *term1_col1_ptr++;
	}
}

void* pthread_q2_array_1threads_16966392_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t term1_it = args->start;
	uint32_t term1_fragment_size = args->end;
	int thread_id = args->thread_id;

	for (; term1_it < term1_fragment_size; term1_it++) {

		uint32_t term1_col0_element = index2_col0_buffer[0][0][term1_it];
		unsigned char term1_col1_element = index2_col1_buffer[0][0][term1_it];

		uint32_t* row_doc2 = idx[3]->index_map[term1_col0_element];
		uint32_t doc2_col0_bytes = idx[3]->index_map[term1_col0_element+1][0] - row_doc2[0];
		if(doc2_col0_bytes) {

			uint32_t* doc2_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[3]->fragment_data[0][row_doc2[0]]));
			uint32_t doc2_fragment_size = 0;
			q2_array_1threads_16966392_doc2_col0_decode_UA_threaded(thread_id, doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

			unsigned char* doc2_col1_ptr = &(idx[3]->fragment_data[1][row_doc2[1]]);
			q2_array_1threads_16966392_doc2_col1_decode_UA_threaded(thread_id, doc2_col1_ptr, doc2_fragment_size);

			for (uint32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {

				uint32_t doc2_col0_element = index3_col0_buffer[thread_id][0][doc2_it];
				unsigned char doc2_col1_element = index3_col1_buffer[thread_id][0][doc2_it];

				uint32_t* row_year2 = idx[1]->index_map[doc2_col0_element];

				uint32_t* year2_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[1]->fragment_data[0][row_year2[0]]));
				uint32_t year2_col0_element;
				q2_array_1threads_16966392_year2_col0_decode_UA(year2_col0_ptr, year2_col0_element);

				RC[doc2_col0_element] = 1;

				pthread_spin_lock(&spin_locks[3][doc2_col0_element]);
				R[doc2_col0_element] += (double)(term1_col1_element*doc2_col1_element)/(ABS((int)year1_col0_element-(int)year2_col0_element)+1);
				pthread_spin_unlock(&spin_locks[3][doc2_col0_element]);

			}
		}
	}
	return nullptr;
}

void q2_array_1threads_16966392_doc2_col0_decode_UA_threaded(int thread_id, uint32_t* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) {

	doc2_fragment_size = doc2_col0_bytes/4;

	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		index3_col0_buffer[thread_id][0][i] = *doc2_col0_ptr++;
	}
}

void q2_array_1threads_16966392_doc2_col1_decode_UA_threaded(int thread_id, unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) {

	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		index3_col1_buffer[thread_id][0][i] = *doc2_col1_ptr++;
	}
}

void q2_array_1threads_16966392_year2_col0_decode_UA(uint32_t* year2_col0_ptr, uint32_t & year2_col0_element) {

	year2_col0_element = *year2_col0_ptr;
}

extern "C" double* q2_array_1threads_16966392(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[1];
	index1_col0_buffer = buffer_arrays[1][0];
	for (int i=0; i<NUM_THREADS; i++) {
		index1_col0_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index1_col0_buffer[i][j] = new uint64_t[max_frag];
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[2];
	index2_col0_buffer = buffer_arrays[2][0];
	for (int i=0; i<NUM_THREADS; i++) {
		index2_col0_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index2_col0_buffer[i][j] = new uint64_t[max_frag];
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
	R = new double[metadata.idx_domains[3][0]]();


	uint64_t doc1_list[1];
	doc1_list[0] = 16966392;

	for (int doc1_it = 0; doc1_it<1; doc1_it++) {

		uint64_t doc1_col0_element = doc1_list[doc1_it];

		uint32_t* row_year1 = idx[1]->index_map[doc1_col0_element];

		uint32_t* year1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[1]->fragment_data[0][row_year1[0]]));
		q2_array_1threads_16966392_year1_col0_decode_UA(year1_col0_ptr);

		uint32_t* row_term1 = idx[2]->index_map[doc1_col0_element];
		uint32_t term1_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_term1[0];
		if(term1_col0_bytes) {

			uint32_t* term1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[2]->fragment_data[0][row_term1[0]]));
			uint32_t term1_fragment_size = 0;
			q2_array_1threads_16966392_term1_col0_decode_UA(term1_col0_ptr, term1_col0_bytes, term1_fragment_size);

			unsigned char* term1_col1_ptr = &(idx[2]->fragment_data[1][row_term1[1]]);
			q2_array_1threads_16966392_term1_col1_decode_UA(term1_col1_ptr, term1_fragment_size);

			uint32_t thread_size = term1_fragment_size/NUM_THREADS;
			uint32_t position = 0;

			for (int i=0; i<NUM_THREADS; i++) {
				arguments[i].start = position;
				position += thread_size;
				arguments[i].end = position;
				arguments[i].thread_id = i;
			}
			arguments[NUM_THREADS-1].end = term1_fragment_size;

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_create(&threads[i], NULL, &pthread_q2_array_1threads_16966392_worker, (void *) &arguments[i]);
			}

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_join(threads[i], NULL);
			}
		}
	}


	for (int j=0; j<NUM_THREADS; j++) {
		for (int k=0; k<BUFFER_POOL_SIZE; k++) {
			delete[] index1_col0_buffer[j][k];
		}
		delete[] index1_col0_buffer[j];
	}
	for (int j=0; j<NUM_THREADS; j++) {
		for (int k=0; k<BUFFER_POOL_SIZE; k++) {
			delete[] index2_col0_buffer[j][k];
		}
		delete[] index2_col0_buffer[j];
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

