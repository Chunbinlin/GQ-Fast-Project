#ifndef test_pubmed_q2_opt_
#define test_pubmed_q2_opt_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 1
#define BUFFER_POOL_SIZE 1

using namespace std;

static double* R;
static int* RC;

static uint32_t* year1_col0_bits_info;

static int* term1_col1_huffman_tree_array;
static bool* term1_col1_huffman_terminator_array;

static int* doc2_col1_huffman_tree_array;
static bool* doc2_col1_huffman_terminator_array;

static uint32_t* year2_col0_bits_info;

extern inline void test_pubmed_q2_opt_year1_col0_decode_BCA(int64_t* year1_col0_ptr, uint32_t & year1_col0_element) __attribute__((always_inline));

extern inline void test_pubmed_q2_opt_term1_col0_decode_BB(unsigned char* term1_col0_ptr, int32_t term1_col0_bytes, int32_t & term1_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q2_opt_term1_col1_decode_Huffman(unsigned char* term1_col1_ptr, int32_t term1_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q2_opt_doc2_col0_decode_BB(unsigned char* doc2_col0_ptr, int32_t doc2_col0_bytes, int32_t & doc2_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q2_opt_doc2_col1_decode_Huffman(unsigned char* doc2_col1_ptr, int32_t doc2_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q2_opt_year2_col0_decode_BCA(int64_t* year2_col0_ptr, uint32_t & year2_col0_element) __attribute__((always_inline));

void test_pubmed_q2_opt_year1_col0_decode_BCA(int64_t* year1_col0_ptr, uint32_t & year1_col0_element) {

	year1_col0_element = year1_col0_bits_info[1];
	year1_col0_element &= *year1_col0_ptr;
}

void test_pubmed_q2_opt_term1_col0_decode_BB(unsigned char* term1_col0_ptr, int32_t term1_col0_bytes, int32_t & term1_fragment_size) {

	buffer_arrays[2][0][0][0][0] = 0;

	int shiftbits = 0;
	do { 
		term1_col0_bytes--;
		int32_t next_seven_bits = *term1_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		buffer_arrays[2][0][0][0][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*term1_col0_ptr++ & 128));
	term1_fragment_size++;

	while (term1_col0_bytes > 0) {
		shiftbits = 0;
		int32_t result = 0;

		do {

			term1_col0_bytes--;
			int32_t next_seven_bits = *term1_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*term1_col0_ptr++ & 128));
		buffer_arrays[2][0][0][0][term1_fragment_size] = buffer_arrays[2][0][0][0][term1_fragment_size-1]+1+result;
		term1_fragment_size++;
	}
}

void test_pubmed_q2_opt_term1_col1_decode_Huffman(unsigned char* term1_col1_ptr, int32_t term1_fragment_size) {

	bool* terminate_start = &(term1_col1_huffman_terminator_array[0]);
	int* tree_array_start = &(term1_col1_huffman_tree_array[0]);

	int mask = 0x100;

	for (int32_t i=0; i<term1_fragment_size; i++) {

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

void test_pubmed_q2_opt_doc2_col0_decode_BB(unsigned char* doc2_col0_ptr, int32_t doc2_col0_bytes, int32_t & doc2_fragment_size) {

	buffer_arrays[3][0][0][0][0] = 0;

	int shiftbits = 0;
	do { 
		doc2_col0_bytes--;
		int32_t next_seven_bits = *doc2_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		buffer_arrays[3][0][0][0][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*doc2_col0_ptr++ & 128));
	doc2_fragment_size++;

	while (doc2_col0_bytes > 0) {
		shiftbits = 0;
		int32_t result = 0;

		do {

			doc2_col0_bytes--;
			int32_t next_seven_bits = *doc2_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*doc2_col0_ptr++ & 128));
		buffer_arrays[3][0][0][0][doc2_fragment_size] = buffer_arrays[3][0][0][0][doc2_fragment_size-1]+1+result;
		doc2_fragment_size++;
	}
}

void test_pubmed_q2_opt_doc2_col1_decode_Huffman(unsigned char* doc2_col1_ptr, int32_t doc2_fragment_size) {

	bool* terminate_start = &(doc2_col1_huffman_terminator_array[0]);
	int* tree_array_start = &(doc2_col1_huffman_tree_array[0]);

	int mask = 0x100;

	for (int32_t i=0; i<doc2_fragment_size; i++) {

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

		buffer_arrays[3][1][0][0][i] = *tree_array;
	}
}

void test_pubmed_q2_opt_year2_col0_decode_BCA(int64_t* year2_col0_ptr, uint32_t & year2_col0_element) {

	year2_col0_element = year2_col0_bits_info[1];
	year2_col0_element &= *year2_col0_ptr;
}

extern "C" double* test_pubmed_q2_opt(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

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

	RC = new int[metadata.idx_domains[3][0]]();
	R = new double[metadata.idx_domains[3][0]]();


	year1_col0_bits_info = idx[1]->dict[0]->bits_info;

	term1_col1_huffman_tree_array = idx[2]->huffman_tree_array[1];
	term1_col1_huffman_terminator_array = idx[2]->huffman_terminator_array[1];

	doc2_col1_huffman_tree_array = idx[3]->huffman_tree_array[1];
	doc2_col1_huffman_terminator_array = idx[3]->huffman_terminator_array[1];

	year2_col0_bits_info = idx[1]->dict[0]->bits_info;

	int64_t doc1_list[1];
	doc1_list[0] = 1000;

	for (int doc1_it = 0; doc1_it<1; doc1_it++) {

		int64_t doc1_col0_element = doc1_list[doc1_it];

		uint32_t* row_year1 = idx[1]->index_map[doc1_col0_element];

		int64_t* year1_col0_ptr = reinterpret_cast<int64_t *>(&(idx[1]->fragment_data[0][row_year1[0]]));
		uint32_t year1_col0_element;
		test_pubmed_q2_opt_year1_col0_decode_BCA(year1_col0_ptr, year1_col0_element);

		uint32_t* row_term1 = idx[2]->index_map[doc1_col0_element];
		int32_t term1_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_term1[0];
		if(term1_col0_bytes) {

			unsigned char* term1_col0_ptr = &(idx[2]->fragment_data[0][row_term1[0]]);
			int32_t term1_fragment_size = 0;
			test_pubmed_q2_opt_term1_col0_decode_BB(term1_col0_ptr, term1_col0_bytes, term1_fragment_size);

			unsigned char* term1_col1_ptr = &(idx[2]->fragment_data[1][row_term1[1]]);
			test_pubmed_q2_opt_term1_col1_decode_Huffman(term1_col1_ptr, term1_fragment_size);

			for (int32_t term1_it = 0; term1_it < term1_fragment_size; term1_it++) {

				uint32_t term1_col0_element = buffer_arrays[2][0][0][0][term1_it];
				unsigned char term1_col1_element = buffer_arrays[2][1][0][0][term1_it];

				uint32_t* row_doc2 = idx[3]->index_map[term1_col0_element];
				int32_t doc2_col0_bytes = idx[3]->index_map[term1_col0_element+1][0] - row_doc2[0];
				if(doc2_col0_bytes) {

					unsigned char* doc2_col0_ptr = &(idx[3]->fragment_data[0][row_doc2[0]]);
					int32_t doc2_fragment_size = 0;
					test_pubmed_q2_opt_doc2_col0_decode_BB(doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

					unsigned char* doc2_col1_ptr = &(idx[3]->fragment_data[1][row_doc2[1]]);
					test_pubmed_q2_opt_doc2_col1_decode_Huffman(doc2_col1_ptr, doc2_fragment_size);

					for (int32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {

						uint32_t doc2_col0_element = buffer_arrays[3][0][0][0][doc2_it];
						unsigned char doc2_col1_element = buffer_arrays[3][1][0][0][doc2_it];

						uint32_t* row_year2 = idx[1]->index_map[doc2_col0_element];

						int64_t* year2_col0_ptr = reinterpret_cast<int64_t *>(&(idx[1]->fragment_data[0][row_year2[0]]));
						uint32_t year2_col0_element;
						test_pubmed_q2_opt_year2_col0_decode_BCA(year2_col0_ptr, year2_col0_element);

						RC[doc2_col0_element] = 1;
						R[doc2_col0_element] += (double)(term1_col1_element*doc2_col1_element)/(ABS((int)year1_col0_element-(int)year2_col0_element)+1);
					}
				}
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

