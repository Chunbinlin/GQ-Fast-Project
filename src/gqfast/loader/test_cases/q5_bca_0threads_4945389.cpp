#ifndef q5_bca_0threads_4945389_
#define q5_bca_0threads_4945389_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 1
#define BUFFER_POOL_SIZE 1

using namespace std;

static double* R;
static int* RC;

static uint64_t*** index0_col0_buffer;
static uint64_t*** index1_col0_buffer;
static uint64_t*** index2_col0_buffer;
static uint64_t*** index2_col1_buffer;
static uint64_t*** index3_col0_buffer;
static uint64_t*** index3_col1_buffer;
static uint64_t*** index4_col0_buffer;

static uint32_t* doc1_col0_bits_info;
static uint64_t doc1_col0_offset;

static uint32_t* term_col0_bits_info;
static uint64_t term_col0_offset;

static uint32_t* term_col1_bits_info;
static uint64_t term_col1_offset;

static uint32_t* doc2_col0_bits_info;
static uint64_t doc2_col0_offset;

static uint32_t* doc2_col1_bits_info;
static uint64_t doc2_col1_offset;

static uint32_t* year_col0_bits_info;
static uint64_t year_col0_offset;

static uint32_t* author2_col0_bits_info;
static uint64_t author2_col0_offset;

extern inline void q5_bca_0threads_4945389_doc1_col0_decode_BCA(unsigned char* doc1_col0_ptr, uint32_t doc1_col0_bytes, uint32_t & doc1_fragment_size) __attribute__((always_inline));

extern inline void q5_bca_0threads_4945389_term_col0_decode_BCA(unsigned char* term_col0_ptr, uint32_t term_col0_bytes, uint32_t & term_fragment_size) __attribute__((always_inline));

extern inline void q5_bca_0threads_4945389_term_col1_decode_BCA(unsigned char* term_col1_ptr, uint32_t term_fragment_size) __attribute__((always_inline));

extern inline void q5_bca_0threads_4945389_doc2_col0_decode_BCA(unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) __attribute__((always_inline));

extern inline void q5_bca_0threads_4945389_doc2_col1_decode_BCA(unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) __attribute__((always_inline));

extern inline void q5_bca_0threads_4945389_year_col0_decode_BCA(uint64_t* year_col0_ptr, uint32_t & year_col0_element) __attribute__((always_inline));

extern inline void q5_bca_0threads_4945389_author2_col0_decode_BCA(unsigned char* author2_col0_ptr, uint32_t author2_col0_bytes, uint32_t & author2_fragment_size) __attribute__((always_inline));

void q5_bca_0threads_4945389_doc1_col0_decode_BCA(unsigned char* doc1_col0_ptr, uint32_t doc1_col0_bytes, uint32_t & doc1_fragment_size) {

	doc1_fragment_size = doc1_col0_bytes* 8 / doc1_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<doc1_fragment_size; i++) {
		uint32_t encoded_value = doc1_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(doc1_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		doc1_col0_ptr += (bit_pos + doc1_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + doc1_col0_bits_info[0]) % 8;
		index0_col0_buffer[0][0][i] = encoded_value + doc1_col0_offset;
	}
}

void q5_bca_0threads_4945389_term_col0_decode_BCA(unsigned char* term_col0_ptr, uint32_t term_col0_bytes, uint32_t & term_fragment_size) {

	term_fragment_size = term_col0_bytes* 8 / term_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<term_fragment_size; i++) {
		uint32_t encoded_value = term_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(term_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		term_col0_ptr += (bit_pos + term_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + term_col0_bits_info[0]) % 8;
		index2_col0_buffer[0][0][i] = encoded_value + term_col0_offset;
	}
}

void q5_bca_0threads_4945389_term_col1_decode_BCA(unsigned char* term_col1_ptr, uint32_t term_fragment_size) {

	int bit_pos = 0;
	for (uint32_t i=0; i<term_fragment_size; i++) {
		uint32_t encoded_value = term_col1_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(term_col1_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		term_col1_ptr += (bit_pos + term_col1_bits_info[0]) / 8;
		bit_pos = (bit_pos + term_col1_bits_info[0]) % 8;
		index2_col1_buffer[0][0][i] = encoded_value + term_col1_offset;
	}
}

void q5_bca_0threads_4945389_doc2_col0_decode_BCA(unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) {

	doc2_fragment_size = doc2_col0_bytes* 8 / doc2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		uint32_t encoded_value = doc2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(doc2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		doc2_col0_ptr += (bit_pos + doc2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + doc2_col0_bits_info[0]) % 8;
		index3_col0_buffer[0][0][i] = encoded_value + doc2_col0_offset;
	}
}

void q5_bca_0threads_4945389_doc2_col1_decode_BCA(unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) {

	int bit_pos = 0;
	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		uint32_t encoded_value = doc2_col1_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(doc2_col1_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		doc2_col1_ptr += (bit_pos + doc2_col1_bits_info[0]) / 8;
		bit_pos = (bit_pos + doc2_col1_bits_info[0]) % 8;
		index3_col1_buffer[0][0][i] = encoded_value + doc2_col1_offset;
	}
}

void q5_bca_0threads_4945389_year_col0_decode_BCA(uint64_t* year_col0_ptr, uint32_t & year_col0_element) {

	year_col0_element = year_col0_bits_info[1];
	year_col0_element &= *year_col0_ptr;
	year_col0_element += year_col0_offset;
}

void q5_bca_0threads_4945389_author2_col0_decode_BCA(unsigned char* author2_col0_ptr, uint32_t author2_col0_bytes, uint32_t & author2_fragment_size) {

	author2_fragment_size = author2_col0_bytes* 8 / author2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<author2_fragment_size; i++) {
		uint32_t encoded_value = author2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(author2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		author2_col0_ptr += (bit_pos + author2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + author2_col0_bits_info[0]) % 8;
		index4_col0_buffer[0][0][i] = encoded_value + author2_col0_offset;
	}
}

extern "C" double* q5_bca_0threads_4945389(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[0];
	index0_col0_buffer = buffer_arrays[0][0];
	for (int i=0; i<NUM_THREADS; i++) {
		index0_col0_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index0_col0_buffer[i][j] = new uint64_t[max_frag];
		}
	}

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

	max_frag = metadata.idx_max_fragment_sizes[4];
	index4_col0_buffer = buffer_arrays[4][0];
	for (int i=0; i<NUM_THREADS; i++) {
		index4_col0_buffer[i] = new uint64_t*[BUFFER_POOL_SIZE];
		for (int j=0; j<BUFFER_POOL_SIZE; j++) {
			index4_col0_buffer[i][j] = new uint64_t[max_frag];
		}
	}

	RC = new int[metadata.idx_domains[4][0]]();
	R = new double[metadata.idx_domains[4][0]]();


	doc1_col0_bits_info = idx[0]->dict[0]->bits_info;
	doc1_col0_offset = idx[0]->dict[0]->offset;

	term_col0_bits_info = idx[2]->dict[0]->bits_info;
	term_col0_offset = idx[2]->dict[0]->offset;

	term_col1_bits_info = idx[2]->dict[1]->bits_info;
	term_col1_offset = idx[2]->dict[1]->offset;

	doc2_col0_bits_info = idx[3]->dict[0]->bits_info;
	doc2_col0_offset = idx[3]->dict[0]->offset;

	doc2_col1_bits_info = idx[3]->dict[1]->bits_info;
	doc2_col1_offset = idx[3]->dict[1]->offset;

	year_col0_bits_info = idx[1]->dict[0]->bits_info;
	year_col0_offset = idx[1]->dict[0]->offset;

	author2_col0_bits_info = idx[4]->dict[0]->bits_info;
	author2_col0_offset = idx[4]->dict[0]->offset;

	uint64_t author1_list[1];
	author1_list[0] = 4945389;

	for (int author1_it = 0; author1_it<1; author1_it++) {

		uint64_t author1_col0_element = author1_list[author1_it];

		uint32_t* row_doc1 = idx[0]->index_map[author1_col0_element];
		uint32_t doc1_col0_bytes = idx[0]->index_map[author1_col0_element+1][0] - row_doc1[0];
		if(doc1_col0_bytes) {

			unsigned char* doc1_col0_ptr = &(idx[0]->fragment_data[0][row_doc1[0]]);
			uint32_t doc1_fragment_size = 0;
			q5_bca_0threads_4945389_doc1_col0_decode_BCA(doc1_col0_ptr, doc1_col0_bytes, doc1_fragment_size);

			for (uint32_t doc1_it = 0; doc1_it < doc1_fragment_size; doc1_it++) {

				uint32_t doc1_col0_element = index0_col0_buffer[0][0][doc1_it];

				uint32_t* row_term = idx[2]->index_map[doc1_col0_element];
				uint32_t term_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_term[0];
				if(term_col0_bytes) {

					unsigned char* term_col0_ptr = &(idx[2]->fragment_data[0][row_term[0]]);
					uint32_t term_fragment_size = 0;
					q5_bca_0threads_4945389_term_col0_decode_BCA(term_col0_ptr, term_col0_bytes, term_fragment_size);

					unsigned char* term_col1_ptr = &(idx[2]->fragment_data[1][row_term[1]]);
					q5_bca_0threads_4945389_term_col1_decode_BCA(term_col1_ptr, term_fragment_size);

					for (uint32_t term_it = 0; term_it < term_fragment_size; term_it++) {

						uint32_t term_col0_element = index2_col0_buffer[0][0][term_it];
						unsigned char term_col1_element = index2_col1_buffer[0][0][term_it];

						uint32_t* row_doc2 = idx[3]->index_map[term_col0_element];
						uint32_t doc2_col0_bytes = idx[3]->index_map[term_col0_element+1][0] - row_doc2[0];
						if(doc2_col0_bytes) {

							unsigned char* doc2_col0_ptr = &(idx[3]->fragment_data[0][row_doc2[0]]);
							uint32_t doc2_fragment_size = 0;
							q5_bca_0threads_4945389_doc2_col0_decode_BCA(doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

							unsigned char* doc2_col1_ptr = &(idx[3]->fragment_data[1][row_doc2[1]]);
							q5_bca_0threads_4945389_doc2_col1_decode_BCA(doc2_col1_ptr, doc2_fragment_size);

							for (uint32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {

								uint32_t doc2_col0_element = index3_col0_buffer[0][0][doc2_it];
								unsigned char doc2_col1_element = index3_col1_buffer[0][0][doc2_it];

								uint32_t* row_year = idx[1]->index_map[doc2_col0_element];

								uint64_t* year_col0_ptr = reinterpret_cast<uint64_t *>(&(idx[1]->fragment_data[0][row_year[0]]));
								uint32_t year_col0_element;
								q5_bca_0threads_4945389_year_col0_decode_BCA(year_col0_ptr, year_col0_element);

								uint32_t* row_author2 = idx[4]->index_map[doc2_col0_element];
								uint32_t author2_col0_bytes = idx[4]->index_map[doc2_col0_element+1][0] - row_author2[0];
								if(author2_col0_bytes) {

									unsigned char* author2_col0_ptr = &(idx[4]->fragment_data[0][row_author2[0]]);
									uint32_t author2_fragment_size = 0;
									q5_bca_0threads_4945389_author2_col0_decode_BCA(author2_col0_ptr, author2_col0_bytes, author2_fragment_size);

									for (uint32_t author2_it = 0; author2_it < author2_fragment_size; author2_it++) {
										uint32_t author2_col0_element = index4_col0_buffer[0][0][author2_it];

										RC[author2_col0_element] = 1;
										R[author2_col0_element] += (double)(term_col1_element*doc2_col1_element)/(2017-year_col0_element);
									}
								}
							}
						}
					}
				}
			}
		}
	}


	for (int j=0; j<NUM_THREADS; j++) {
		for (int k=0; k<BUFFER_POOL_SIZE; k++) {
			delete[] index0_col0_buffer[j][k];
		}
		delete[] index0_col0_buffer[j];
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
	for (int j=0; j<NUM_THREADS; j++) {
		for (int k=0; k<BUFFER_POOL_SIZE; k++) {
			delete[] index4_col0_buffer[j][k];
		}
		delete[] index4_col0_buffer[j];
	}


	*null_checks = RC;
	return R;

}

#endif

