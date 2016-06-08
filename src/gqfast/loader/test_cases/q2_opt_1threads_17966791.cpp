#ifndef q2_opt_1threads_17966791_
#define q2_opt_1threads_17966791_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 1
#define BUFFER_POOL_SIZE 1

using namespace std;

static args_threading arguments[NUM_THREADS];

static uint32_t year1_col0_element;

static double* R;
static int* RC;

static uint32_t* year1_col0_bits_info;
static uint64_t year1_col0_offset;

static int* term1_col1_huffman_tree_array;
static bool* term1_col1_huffman_terminator_array;

static int* doc2_col1_huffman_tree_array;
static bool* doc2_col1_huffman_terminator_array;

static uint32_t* year2_col0_bits_info;
static uint64_t year2_col0_offset;

extern inline void q2_opt_1threads_17966791_year1_col0_decode_BCA(uint64_t* year1_col0_ptr) __attribute__((always_inline));

extern inline void q2_opt_1threads_17966791_term1_col0_decode_BB(unsigned char* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) __attribute__((always_inline));

extern inline void q2_opt_1threads_17966791_term1_col1_decode_Huffman(unsigned char* term1_col1_ptr, uint32_t term1_fragment_size) __attribute__((always_inline));

void* pthread_q2_opt_1threads_17966791_worker(void* arguments);

extern inline void q2_opt_1threads_17966791_doc2_col0_decode_BB_threaded(int thread_id, unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) __attribute__((always_inline));

extern inline void q2_opt_1threads_17966791_doc2_col1_decode_Huffman_threaded(int thread_id, unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) __attribute__((always_inline));

extern inline void q2_opt_1threads_17966791_year2_col0_decode_BCA(uint64_t* year2_col0_ptr, uint32_t & year2_col0_element) __attribute__((always_inline));

void q2_opt_1threads_17966791_year1_col0_decode_BCA(uint64_t* year1_col0_ptr) {

	year1_col0_element = year1_col0_bits_info[1];
	year1_col0_element &= *year1_col0_ptr;
	year1_col0_element += year1_col0_offset;
}

void q2_opt_1threads_17966791_term1_col0_decode_BB(unsigned char* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) {

	buffer_arrays[2][0][0][0][0] = 0;

	int shiftbits = 0;
	do { 
		term1_col0_bytes--;
		uint32_t next_seven_bits = *term1_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		buffer_arrays[2][0][0][0][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*term1_col0_ptr++ & 128));
	term1_fragment_size++;

	while (term1_col0_bytes > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			term1_col0_bytes--;
			uint32_t next_seven_bits = *term1_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*term1_col0_ptr++ & 128));
		buffer_arrays[2][0][0][0][term1_fragment_size] = buffer_arrays[2][0][0][0][term1_fragment_size-1]+1+result;
		term1_fragment_size++;
	}
}

void q2_opt_1threads_17966791_term1_col1_decode_Huffman(unsigned char* term1_col1_ptr, uint32_t term1_fragment_size) {

	bool* terminate_start = &(term1_col1_huffman_terminator_array[0]);
	int* tree_array_start = &(term1_col1_huffman_tree_array[0]);

	int mask = 0x100;

	for (uint32_t i=0; i<term1_fragment_size; i++) {

		bool* terminator_array = terminate_start;
		int* tree_array = tree_array_start;

		while(!*terminator_array) { 

			char direction = *term1_col1_ptr & (mask >>= 1);

			if (mask == 1) { 
				mask = 0x100;
				term1_col1_ptr++;
			}

			terminator_array += *tree_array;
			tree_array += *tree_array;

			if (direction) {
				terminator_array++;
				tree_array++;
			}
		}

		buffer_arrays[2][1][0][0][i] = *tree_array;
	}
}

void* pthread_q2_opt_1threads_17966791_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t term1_it = args->start;
	uint32_t term1_fragment_size = args->end;
	int thread_id = args->thread_id;

	for (; term1_it < term1_fragment_size; term1_it++) {

		uint32_t term1_col0_element = buffer_arrays[2][0][0][0][term1_it];
		unsigned char term1_col1_element = buffer_arrays[2][1][0][0][term1_it];

		uint32_t* row_doc2 = idx[3]->index_map[term1_col0_element];
		uint32_t doc2_col0_bytes = idx[3]->index_map[term1_col0_element+1][0] - row_doc2[0];
		if(doc2_col0_bytes) {

			unsigned char* doc2_col0_ptr = &(idx[3]->fragment_data[0][row_doc2[0]]);
			uint32_t doc2_fragment_size = 0;
			q2_opt_1threads_17966791_doc2_col0_decode_BB_threaded(thread_id, doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

			unsigned char* doc2_col1_ptr = &(idx[3]->fragment_data[1][row_doc2[1]]);
			q2_opt_1threads_17966791_doc2_col1_decode_Huffman_threaded(thread_id, doc2_col1_ptr, doc2_fragment_size);

			for (uint32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {

				uint32_t doc2_col0_element = buffer_arrays[3][0][thread_id][0][doc2_it];
				unsigned char doc2_col1_element = buffer_arrays[3][1][thread_id][0][doc2_it];

				uint32_t* row_year2 = idx[1]->index_map[doc2_col0_element];

				uint64_t* year2_col0_ptr = reinterpret_cast<uint64_t *>(&(idx[1]->fragment_data[0][row_year2[0]]));
				uint32_t year2_col0_element;
				q2_opt_1threads_17966791_year2_col0_decode_BCA(year2_col0_ptr, year2_col0_element);

				RC[doc2_col0_element] = 1;

				pthread_spin_lock(&spin_locks[3][doc2_col0_element]);
				R[doc2_col0_element] += (double)(term1_col1_element*doc2_col1_element)/(ABS((int)year1_col0_element-(int)year2_col0_element)+1);
				pthread_spin_unlock(&spin_locks[3][doc2_col0_element]);

			}
		}
	}
	return nullptr;
}

void q2_opt_1threads_17966791_doc2_col0_decode_BB_threaded(int thread_id, unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) {

	buffer_arrays[3][0][thread_id][0][0] = 0;

	int shiftbits = 0;
	do { 
		doc2_col0_bytes--;
		uint32_t next_seven_bits = *doc2_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		buffer_arrays[3][0][thread_id][0][0] |= next_seven_bits;
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
		buffer_arrays[3][0][thread_id][0][doc2_fragment_size] = buffer_arrays[3][0][thread_id][0][doc2_fragment_size-1]+1+result;
		doc2_fragment_size++;
	}
}

void q2_opt_1threads_17966791_doc2_col1_decode_Huffman_threaded(int thread_id, unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) {

	bool* terminate_start = &(doc2_col1_huffman_terminator_array[0]);
	int* tree_array_start = &(doc2_col1_huffman_tree_array[0]);

	int mask = 0x100;

	for (uint32_t i=0; i<doc2_fragment_size; i++) {

		bool* terminator_array = terminate_start;
		int* tree_array = tree_array_start;

		while(!*terminator_array) { 

			char direction = *doc2_col1_ptr & (mask >>= 1);

			if (mask == 1) { 
				mask = 0x100;
				doc2_col1_ptr++;
			}

			terminator_array += *tree_array;
			tree_array += *tree_array;

			if (direction) {
				terminator_array++;
				tree_array++;
			}
		}

		buffer_arrays[3][1][thread_id][0][i] = *tree_array;
	}
}

void q2_opt_1threads_17966791_year2_col0_decode_BCA(uint64_t* year2_col0_ptr, uint32_t & year2_col0_element) {

	year2_col0_element = year2_col0_bits_info[1];
	year2_col0_element &= *year2_col0_ptr;
	year2_col0_element += year2_col0_offset;
}

extern "C" double* q2_opt_1threads_17966791(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[1];
	for(int i=0; i<metadata.idx_num_encodings[1]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[1][i][j] = new uint64_t*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[1][i][j][k] = new uint64_t[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[2];
	for(int i=0; i<metadata.idx_num_encodings[2]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[2][i][j] = new uint64_t*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[2][i][j][k] = new uint64_t[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[3];
	for(int i=0; i<metadata.idx_num_encodings[3]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[3][i][j] = new uint64_t*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[3][i][j][k] = new uint64_t[max_frag];
			}
		}
	}

	RC = new int[metadata.idx_domains[3][0]]();
	R = new double[metadata.idx_domains[3][0]]();


	year1_col0_bits_info = idx[1]->dict[0]->bits_info;
	year1_col0_offset = idx[1]->dict[0]->offset;

	term1_col1_huffman_tree_array = idx[2]->huffman_tree_array[1];
	term1_col1_huffman_terminator_array = idx[2]->huffman_terminator_array[1];

	doc2_col1_huffman_tree_array = idx[3]->huffman_tree_array[1];
	doc2_col1_huffman_terminator_array = idx[3]->huffman_terminator_array[1];

	year2_col0_bits_info = idx[1]->dict[0]->bits_info;
	year2_col0_offset = idx[1]->dict[0]->offset;

	uint64_t doc1_list[1];
	doc1_list[0] = 17996791;

	for (int doc1_it = 0; doc1_it<1; doc1_it++) {

		uint64_t doc1_col0_element = doc1_list[doc1_it];

		uint32_t* row_year1 = idx[1]->index_map[doc1_col0_element];

		uint64_t* year1_col0_ptr = reinterpret_cast<uint64_t *>(&(idx[1]->fragment_data[0][row_year1[0]]));
		q2_opt_1threads_17966791_year1_col0_decode_BCA(year1_col0_ptr);

		uint32_t* row_term1 = idx[2]->index_map[doc1_col0_element];
		uint32_t term1_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_term1[0];
		if(term1_col0_bytes) {

			unsigned char* term1_col0_ptr = &(idx[2]->fragment_data[0][row_term1[0]]);
			uint32_t term1_fragment_size = 0;
			q2_opt_1threads_17966791_term1_col0_decode_BB(term1_col0_ptr, term1_col0_bytes, term1_fragment_size);

			unsigned char* term1_col1_ptr = &(idx[2]->fragment_data[1][row_term1[1]]);
			q2_opt_1threads_17966791_term1_col1_decode_Huffman(term1_col1_ptr, term1_fragment_size);

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
				pthread_create(&threads[i], NULL, &pthread_q2_opt_1threads_17966791_worker, (void *) &arguments[i]);
			}

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_join(threads[i], NULL);
			}
		}
	}


	for (int j=0; j<metadata.idx_num_encodings[1]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[1][j][k][l];
			}
			delete[] buffer_arrays[1][j][k];
		}
	}
	for (int j=0; j<metadata.idx_num_encodings[2]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[2][j][k][l];
			}
			delete[] buffer_arrays[2][j][k];
		}
	}
	for (int j=0; j<metadata.idx_num_encodings[3]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[3][j][k][l];
			}
			delete[] buffer_arrays[3][j][k];
		}
	}


	*null_checks = RC;
	return R;

}

#endif

