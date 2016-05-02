#ifndef test_pubmed_q5_bca_threaded_
#define test_pubmed_q5_bca_threaded_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 4
#define NUM_BUFFERS 5
#define BUFFER_POOL_SIZE 1

using namespace std;

static args_threading arguments[NUM_THREADS];

static double* R;
static int* RC;

static uint32_t* doc1_col0_bits_info;

static uint32_t* term_col0_bits_info;

static uint32_t* term_col1_bits_info;

static uint32_t* doc2_col0_bits_info;

static uint32_t* doc2_col1_bits_info;

static uint32_t* year_col0_bits_info;

static uint32_t* author2_col0_bits_info;

extern inline void test_pubmed_q5_bca_threaded_doc1_col0_decode_BCA(unsigned char* doc1_col0_ptr, uint32_t doc1_col0_bytes, uint32_t & doc1_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q5_bca_threaded_term_col0_decode_BCA(unsigned char* term_col0_ptr, uint32_t term_col0_bytes, uint32_t & term_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q5_bca_threaded_term_col1_decode_BCA(unsigned char* term_col1_ptr, uint32_t term_fragment_size) __attribute__((always_inline));

void* pthread_test_pubmed_q5_bca_threaded_worker(void* arguments);

extern inline void test_pubmed_q5_bca_threaded_doc2_col0_decode_BCA_threaded(int thread_id, unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q5_bca_threaded_doc2_col1_decode_BCA_threaded(int thread_id, unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q5_bca_threaded_year_col0_decode_BCA(uint64_t* year_col0_ptr, uint32_t & year_col0_element) __attribute__((always_inline));

extern inline void test_pubmed_q5_bca_threaded_author2_col0_decode_BCA_threaded(int thread_id, unsigned char* author2_col0_ptr, uint32_t author2_col0_bytes, uint32_t & author2_fragment_size) __attribute__((always_inline));

void test_pubmed_q5_bca_threaded_doc1_col0_decode_BCA(unsigned char* doc1_col0_ptr, uint32_t doc1_col0_bytes, uint32_t & doc1_fragment_size) {

	doc1_fragment_size = doc1_col0_bytes* 8 / doc1_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<doc1_fragment_size; i++) {
		uint32_t encoded_value = doc1_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(doc1_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		doc1_col0_ptr += (bit_pos + doc1_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + doc1_col0_bits_info[0]) % 8;
		buffer_arrays[0][0][0][0][i] = encoded_value;
	}
}

void test_pubmed_q5_bca_threaded_term_col0_decode_BCA(unsigned char* term_col0_ptr, uint32_t term_col0_bytes, uint32_t & term_fragment_size) {

	term_fragment_size = term_col0_bytes* 8 / term_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<term_fragment_size; i++) {
		uint32_t encoded_value = term_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(term_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		term_col0_ptr += (bit_pos + term_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + term_col0_bits_info[0]) % 8;
		buffer_arrays[2][0][0][0][i] = encoded_value;
	}
}

void test_pubmed_q5_bca_threaded_term_col1_decode_BCA(unsigned char* term_col1_ptr, uint32_t term_fragment_size) {

	int bit_pos = 0;
	for (uint32_t i=0; i<term_fragment_size; i++) {
		uint32_t encoded_value = term_col1_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(term_col1_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		term_col1_ptr += (bit_pos + term_col1_bits_info[0]) / 8;
		bit_pos = (bit_pos + term_col1_bits_info[0]) % 8;
		buffer_arrays[2][1][0][0][i] = encoded_value;
	}
}

void* pthread_test_pubmed_q5_bca_threaded_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t term_it = args->start;
	uint32_t term_fragment_size = args->end;
	int thread_id = args->thread_id;

	for (; term_it < term_fragment_size; term_it++) {

		uint32_t term_col0_element = buffer_arrays[2][0][0][0][term_it];
		unsigned char term_col1_element = buffer_arrays[2][1][0][0][term_it];

		uint32_t* row_op4 = idx[3]->index_map[term_col0_element];
		uint32_t doc2_col0_bytes = idx[3]->index_map[term_col0_element+1][0] - row_op4[0];
		if(doc2_col0_bytes) {

			unsigned char* doc2_col0_ptr = &(idx[3]->fragment_data[0][row_op4[0]]);
			uint32_t doc2_fragment_size = 0;
			test_pubmed_q5_bca_threaded_doc2_col0_decode_BCA_threaded(thread_id, doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

			unsigned char* doc2_col1_ptr = &(idx[3]->fragment_data[1][row_op4[1]]);
			test_pubmed_q5_bca_threaded_doc2_col1_decode_BCA_threaded(thread_id, doc2_col1_ptr, doc2_fragment_size);

			for (uint32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {

				uint32_t doc2_col0_element = buffer_arrays[3][0][thread_id][0][doc2_it];
				unsigned char doc2_col1_element = buffer_arrays[3][1][thread_id][0][doc2_it];

				uint32_t* row_op5 = idx[1]->index_map[doc2_col0_element];

				uint64_t* year_col0_ptr = reinterpret_cast<uint64_t *>(&(idx[1]->fragment_data[0][row_op5[0]]));
				uint32_t year_col0_element;
				test_pubmed_q5_bca_threaded_year_col0_decode_BCA(year_col0_ptr, year_col0_element);

				uint32_t* row_op6 = idx[4]->index_map[doc2_col0_element];
				uint32_t author2_col0_bytes = idx[4]->index_map[doc2_col0_element+1][0] - row_op6[0];
				if(author2_col0_bytes) {

					unsigned char* author2_col0_ptr = &(idx[4]->fragment_data[0][row_op6[0]]);
					uint32_t author2_fragment_size = 0;
					test_pubmed_q5_bca_threaded_author2_col0_decode_BCA_threaded(thread_id, author2_col0_ptr, author2_col0_bytes, author2_fragment_size);

					for (uint32_t author2_it = 0; author2_it < author2_fragment_size; author2_it++) {
						uint32_t author2_col0_element = buffer_arrays[4][0][thread_id][0][author2_it];

						RC[author2_col0_element] = 1;

						pthread_spin_lock(&spin_locks[4][author2_col0_element]);
						R[author2_col0_element] += (double)(term_col1_element*doc2_col1_element)/(2017-year_col0_element);
						pthread_spin_unlock(&spin_locks[4][author2_col0_element]);

					}
				}
			}
		}
	}
	return nullptr;
}

void test_pubmed_q5_bca_threaded_doc2_col0_decode_BCA_threaded(int thread_id, unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) {

	doc2_fragment_size = doc2_col0_bytes* 8 / doc2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		uint32_t encoded_value = doc2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(doc2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		doc2_col0_ptr += (bit_pos + doc2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + doc2_col0_bits_info[0]) % 8;
		buffer_arrays[3][0][thread_id][0][i] = encoded_value;
	}
}

void test_pubmed_q5_bca_threaded_doc2_col1_decode_BCA_threaded(int thread_id, unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) {

	int bit_pos = 0;
	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		uint32_t encoded_value = doc2_col1_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(doc2_col1_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		doc2_col1_ptr += (bit_pos + doc2_col1_bits_info[0]) / 8;
		bit_pos = (bit_pos + doc2_col1_bits_info[0]) % 8;
		buffer_arrays[3][1][thread_id][0][i] = encoded_value;
	}
}

void test_pubmed_q5_bca_threaded_year_col0_decode_BCA(uint64_t* year_col0_ptr, uint32_t & year_col0_element) {

	year_col0_element = year_col0_bits_info[1];
	year_col0_element &= *year_col0_ptr;
}

void test_pubmed_q5_bca_threaded_author2_col0_decode_BCA_threaded(int thread_id, unsigned char* author2_col0_ptr, uint32_t author2_col0_bytes, uint32_t & author2_fragment_size) {

	author2_fragment_size = author2_col0_bytes* 8 / author2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<author2_fragment_size; i++) {
		uint32_t encoded_value = author2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(author2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		author2_col0_ptr += (bit_pos + author2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + author2_col0_bits_info[0]) % 8;
		buffer_arrays[4][0][thread_id][0][i] = encoded_value;
	}
}

extern "C" double* test_pubmed_q5_bca_threaded(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[0];
	for(int i=0; i<metadata.idx_num_encodings[0]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[0][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[0][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[1];
	for(int i=0; i<metadata.idx_num_encodings[1]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[1][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[1][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[2];
	for(int i=0; i<metadata.idx_num_encodings[2]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[2][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[2][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[3];
	for(int i=0; i<metadata.idx_num_encodings[3]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[3][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[3][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[4];
	for(int i=0; i<metadata.idx_num_encodings[4]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[4][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[4][i][j][k] = new int[max_frag];
			}
		}
	}

	RC = new int[metadata.idx_domains[4][0]]();
	R = new double[metadata.idx_domains[4][0]]();


	doc1_col0_bits_info = idx[0]->dict[0]->bits_info;

	term_col0_bits_info = idx[2]->dict[0]->bits_info;

	term_col1_bits_info = idx[2]->dict[1]->bits_info;

	doc2_col0_bits_info = idx[3]->dict[0]->bits_info;

	doc2_col1_bits_info = idx[3]->dict[1]->bits_info;

	year_col0_bits_info = idx[1]->dict[0]->bits_info;

	author2_col0_bits_info = idx[4]->dict[0]->bits_info;

	uint64_t* author1_list = new uint64_t[1];
	author1_list[0] = 4945389;

	for (int author1_it = 0; author1_it<1; author1_it++) {

		uint64_t author1_col0_element = author1_list[author1_it];

		uint32_t* row_op1 = idx[0]->index_map[author1_col0_element];
		uint32_t doc1_col0_bytes = idx[0]->index_map[author1_col0_element+1][0] - row_op1[0];
		if(doc1_col0_bytes) {

			unsigned char* doc1_col0_ptr = &(idx[0]->fragment_data[0][row_op1[0]]);
			uint32_t doc1_fragment_size = 0;
			test_pubmed_q5_bca_threaded_doc1_col0_decode_BCA(doc1_col0_ptr, doc1_col0_bytes, doc1_fragment_size);

			for (uint32_t doc1_it = 0; doc1_it < doc1_fragment_size; doc1_it++) {

				uint32_t doc1_col0_element = buffer_arrays[0][0][0][0][doc1_it];

				uint32_t* row_op2 = idx[2]->index_map[doc1_col0_element];
				uint32_t term_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_op2[0];
				if(term_col0_bytes) {

					unsigned char* term_col0_ptr = &(idx[2]->fragment_data[0][row_op2[0]]);
					uint32_t term_fragment_size = 0;
					test_pubmed_q5_bca_threaded_term_col0_decode_BCA(term_col0_ptr, term_col0_bytes, term_fragment_size);

					unsigned char* term_col1_ptr = &(idx[2]->fragment_data[1][row_op2[1]]);
					test_pubmed_q5_bca_threaded_term_col1_decode_BCA(term_col1_ptr, term_fragment_size);

					uint32_t thread_size = term_fragment_size/NUM_THREADS;
					uint32_t position = 0;

					for (int i=0; i<NUM_THREADS; i++) {
						arguments[i].start = position;
						position += thread_size;
						arguments[i].end = position;
						arguments[i].thread_id = i;
					}
					arguments[NUM_THREADS-1].end = term_fragment_size;

					for (int i=0; i<NUM_THREADS; i++) {
						pthread_create(&threads[i], NULL, &pthread_test_pubmed_q5_bca_threaded_worker, (void *) &arguments[i]);
					}

					for (int i=0; i<NUM_THREADS; i++) {
						pthread_join(threads[i], NULL);
					}
				}
			}
		}
	}


	for (int j=0; j<metadata.idx_num_encodings[0]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[0][j][k][l];
			}
			delete[] buffer_arrays[0][j][k];
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
	for (int j=0; j<metadata.idx_num_encodings[4]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[4][j][k][l];
			}
			delete[] buffer_arrays[4][j][k];
		}
	}


	*null_checks = RC;
	return R;

}

#endif

