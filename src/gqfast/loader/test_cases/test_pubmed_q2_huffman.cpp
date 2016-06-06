#ifndef test_pubmed_q2_huffman_
#define test_pubmed_q2_huffman_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 1
#define BUFFER_POOL_SIZE 1

using namespace std;

static double* R;
static int* RC;

static int* year1_col0_huffman_tree_array;
static bool* year1_col0_huffman_terminator_array;

static int* term1_col0_huffman_tree_array;
static bool* term1_col0_huffman_terminator_array;

static int* term1_col1_huffman_tree_array;
static bool* term1_col1_huffman_terminator_array;

static int* doc2_col0_huffman_tree_array;
static bool* doc2_col0_huffman_terminator_array;

static int* doc2_col1_huffman_tree_array;
static bool* doc2_col1_huffman_terminator_array;

static int* year2_col0_huffman_tree_array;
static bool* year2_col0_huffman_terminator_array;

extern inline void test_pubmed_q2_huffman_year1_col0_decode_Huffman(unsigned char* year1_col0_ptr, uint32_t & year1_col0_element) __attribute__((always_inline));

extern inline void test_pubmed_q2_huffman_term1_col0_decode_Huffman(unsigned char* term1_col0_ptr, int32_t term1_col0_bytes, int32_t & term1_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q2_huffman_term1_col1_decode_Huffman(unsigned char* term1_col1_ptr, int32_t term1_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q2_huffman_doc2_col0_decode_Huffman(unsigned char* doc2_col0_ptr, int32_t doc2_col0_bytes, int32_t & doc2_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q2_huffman_doc2_col1_decode_Huffman(unsigned char* doc2_col1_ptr, int32_t doc2_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q2_huffman_year2_col0_decode_Huffman(unsigned char* year2_col0_ptr, uint32_t & year2_col0_element) __attribute__((always_inline));

void test_pubmed_q2_huffman_year1_col0_decode_Huffman(unsigned char* year1_col0_ptr, uint32_t & year1_col0_element) {

	int mask = 0x100;
	bool* terminator_array = &(year1_col0_huffman_terminator_array[0]);
	int* tree_array = &(year1_col0_huffman_tree_array[0]);

	while(!*terminator_array) {

		char direction = *year1_col0_ptr & (mask >>= 1);

		if (mask == 1) {
			mask = 0x100;
			year1_col0_ptr++;
		}

		terminator_array += *tree_array;
		tree_array += *tree_array;

		if (direction) {
			terminator_array++;
			tree_array++;
		}
	}

	year1_col0_element = *tree_array;
}

void test_pubmed_q2_huffman_term1_col0_decode_Huffman(unsigned char* term1_col0_ptr, int32_t term1_col0_bytes, int32_t & term1_fragment_size) {

	bool* terminate_start = &(term1_col0_huffman_terminator_array[0]);
	int* tree_array_start = &(term1_col0_huffman_tree_array[0]);

	int mask = 0x100;

	while (term1_col0_bytes > 1) {

		bool* terminator_array = terminate_start;
		int* tree_array = tree_array_start;

		while(!*terminator_array) { 

			char direction = *term1_col0_ptr & (mask >>= 1);

			if (mask == 1) { 
				mask = 0x100;
				term1_col0_ptr++;
				term1_col0_bytes--;
			}

			terminator_array += *tree_array;
			tree_array += *tree_array;

			if (direction) {
				terminator_array++;
				tree_array++;
			}
		}

		buffer_arrays[2][0][0][0][term1_fragment_size++] = *tree_array;
	}

	if (mask != 0x100) {
		int bit_pos = mask;
		unsigned char last_byte = *term1_col0_ptr;
		while (bit_pos > 1) {
			unsigned char bit = last_byte & (bit_pos >>= 1);
			if (bit) {
				bool* terminator_array = terminate_start;
				int* tree_array = tree_array_start;

				while (!*terminator_array) {
					char direction = *term1_col0_ptr & (mask >>= 1);

					terminator_array += *tree_array;
					tree_array += *tree_array;

					if (direction) {
						terminator_array++;
						tree_array++;
					}
				}

				buffer_arrays[2][0][0][0][term1_fragment_size++] = *tree_array;
				bit_pos = mask;
			}
		}
	}
}

void test_pubmed_q2_huffman_term1_col1_decode_Huffman(unsigned char* term1_col1_ptr, int32_t term1_fragment_size) {

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

void test_pubmed_q2_huffman_doc2_col0_decode_Huffman(unsigned char* doc2_col0_ptr, int32_t doc2_col0_bytes, int32_t & doc2_fragment_size) {

	bool* terminate_start = &(doc2_col0_huffman_terminator_array[0]);
	int* tree_array_start = &(doc2_col0_huffman_tree_array[0]);

	int mask = 0x100;

	while (doc2_col0_bytes > 1) {

		bool* terminator_array = terminate_start;
		int* tree_array = tree_array_start;

		while(!*terminator_array) { 

			char direction = *doc2_col0_ptr & (mask >>= 1);

			if (mask == 1) { 
				mask = 0x100;
				doc2_col0_ptr++;
				doc2_col0_bytes--;
			}

			terminator_array += *tree_array;
			tree_array += *tree_array;

			if (direction) {
				terminator_array++;
				tree_array++;
			}
		}

		buffer_arrays[3][0][0][0][doc2_fragment_size++] = *tree_array;
	}

	if (mask != 0x100) {
		int bit_pos = mask;
		unsigned char last_byte = *doc2_col0_ptr;
		while (bit_pos > 1) {
			unsigned char bit = last_byte & (bit_pos >>= 1);
			if (bit) {
				bool* terminator_array = terminate_start;
				int* tree_array = tree_array_start;

				while (!*terminator_array) {
					char direction = *doc2_col0_ptr & (mask >>= 1);

					terminator_array += *tree_array;
					tree_array += *tree_array;

					if (direction) {
						terminator_array++;
						tree_array++;
					}
				}

				buffer_arrays[3][0][0][0][doc2_fragment_size++] = *tree_array;
				bit_pos = mask;
			}
		}
	}
}

void test_pubmed_q2_huffman_doc2_col1_decode_Huffman(unsigned char* doc2_col1_ptr, int32_t doc2_fragment_size) {

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

void test_pubmed_q2_huffman_year2_col0_decode_Huffman(unsigned char* year2_col0_ptr, uint32_t & year2_col0_element) {

	int mask = 0x100;
	bool* terminator_array = &(year2_col0_huffman_terminator_array[0]);
	int* tree_array = &(year2_col0_huffman_tree_array[0]);

	while(!*terminator_array) {

		char direction = *year2_col0_ptr & (mask >>= 1);

		if (mask == 1) {
			mask = 0x100;
			year2_col0_ptr++;
		}

		terminator_array += *tree_array;
		tree_array += *tree_array;

		if (direction) {
			terminator_array++;
			tree_array++;
		}
	}

	year2_col0_element = *tree_array;
}

extern "C" double* test_pubmed_q2_huffman(int** null_checks) {

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


	year1_col0_huffman_tree_array = idx[1]->huffman_tree_array[0];
	year1_col0_huffman_terminator_array = idx[1]->huffman_terminator_array[0];

	term1_col0_huffman_tree_array = idx[2]->huffman_tree_array[0];
	term1_col0_huffman_terminator_array = idx[2]->huffman_terminator_array[0];

	term1_col1_huffman_tree_array = idx[2]->huffman_tree_array[1];
	term1_col1_huffman_terminator_array = idx[2]->huffman_terminator_array[1];

	doc2_col0_huffman_tree_array = idx[3]->huffman_tree_array[0];
	doc2_col0_huffman_terminator_array = idx[3]->huffman_terminator_array[0];

	doc2_col1_huffman_tree_array = idx[3]->huffman_tree_array[1];
	doc2_col1_huffman_terminator_array = idx[3]->huffman_terminator_array[1];

	year2_col0_huffman_tree_array = idx[1]->huffman_tree_array[0];
	year2_col0_huffman_terminator_array = idx[1]->huffman_terminator_array[0];

	int64_t doc1_list[1];
	doc1_list[0] = 1000;

	for (int doc1_it = 0; doc1_it<1; doc1_it++) {

		int64_t doc1_col0_element = doc1_list[doc1_it];

		uint32_t* row_year1 = idx[1]->index_map[doc1_col0_element];

		unsigned char* year1_col0_ptr = &(idx[1]->fragment_data[0][row_year1[0]]);
		uint32_t year1_col0_element;
		test_pubmed_q2_huffman_year1_col0_decode_Huffman(year1_col0_ptr, year1_col0_element);

		uint32_t* row_term1 = idx[2]->index_map[doc1_col0_element];
		int32_t term1_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_term1[0];
		if(term1_col0_bytes) {

			unsigned char* term1_col0_ptr = &(idx[2]->fragment_data[0][row_term1[0]]);
			int32_t term1_fragment_size = 0;
			test_pubmed_q2_huffman_term1_col0_decode_Huffman(term1_col0_ptr, term1_col0_bytes, term1_fragment_size);

			unsigned char* term1_col1_ptr = &(idx[2]->fragment_data[1][row_term1[1]]);
			test_pubmed_q2_huffman_term1_col1_decode_Huffman(term1_col1_ptr, term1_fragment_size);

			for (int32_t term1_it = 0; term1_it < term1_fragment_size; term1_it++) {

				uint32_t term1_col0_element = buffer_arrays[2][0][0][0][term1_it];
				unsigned char term1_col1_element = buffer_arrays[2][1][0][0][term1_it];

				uint32_t* row_doc2 = idx[3]->index_map[term1_col0_element];
				int32_t doc2_col0_bytes = idx[3]->index_map[term1_col0_element+1][0] - row_doc2[0];
				if(doc2_col0_bytes) {

					unsigned char* doc2_col0_ptr = &(idx[3]->fragment_data[0][row_doc2[0]]);
					int32_t doc2_fragment_size = 0;
					test_pubmed_q2_huffman_doc2_col0_decode_Huffman(doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

					unsigned char* doc2_col1_ptr = &(idx[3]->fragment_data[1][row_doc2[1]]);
					test_pubmed_q2_huffman_doc2_col1_decode_Huffman(doc2_col1_ptr, doc2_fragment_size);

					for (int32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {

						uint32_t doc2_col0_element = buffer_arrays[3][0][0][0][doc2_it];
						unsigned char doc2_col1_element = buffer_arrays[3][1][0][0][doc2_it];

						uint32_t* row_year2 = idx[1]->index_map[doc2_col0_element];

						unsigned char* year2_col0_ptr = &(idx[1]->fragment_data[0][row_year2[0]]);
						uint32_t year2_col0_element;
						test_pubmed_q2_huffman_year2_col0_decode_Huffman(year2_col0_ptr, year2_col0_element);

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

