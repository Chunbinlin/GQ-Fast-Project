#ifndef q5_huff_0threads_5450530_
#define q5_huff_0threads_5450530_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"


using namespace std;

static double* R;
static int* RC;

static uint64_t* doc1_col0_buffer;
static uint64_t* term_col0_buffer;
static uint64_t* term_col1_buffer;
static uint64_t* doc2_col0_buffer;
static uint64_t* doc2_col1_buffer;
static uint64_t* year_col0_buffer;
static uint64_t* author2_col0_buffer;

static int* doc1_col0_huffman_tree_array;
static bool* doc1_col0_huffman_terminator_array;

static int* term_col0_huffman_tree_array;
static bool* term_col0_huffman_terminator_array;

static int* term_col1_huffman_tree_array;
static bool* term_col1_huffman_terminator_array;

static int* doc2_col0_huffman_tree_array;
static bool* doc2_col0_huffman_terminator_array;

static int* doc2_col1_huffman_tree_array;
static bool* doc2_col1_huffman_terminator_array;

static int* year_col0_huffman_tree_array;
static bool* year_col0_huffman_terminator_array;

static int* author2_col0_huffman_tree_array;
static bool* author2_col0_huffman_terminator_array;

extern inline void q5_huff_0threads_5450530_doc1_col0_decode_Huffman(unsigned char* doc1_col0_ptr, uint32_t doc1_col0_bytes, uint32_t & doc1_fragment_size) __attribute__((always_inline));

extern inline void q5_huff_0threads_5450530_term_col0_decode_Huffman(unsigned char* term_col0_ptr, uint32_t term_col0_bytes, uint32_t & term_fragment_size) __attribute__((always_inline));

extern inline void q5_huff_0threads_5450530_term_col1_decode_Huffman(unsigned char* term_col1_ptr, uint32_t term_fragment_size) __attribute__((always_inline));

extern inline void q5_huff_0threads_5450530_doc2_col0_decode_Huffman(unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) __attribute__((always_inline));

extern inline void q5_huff_0threads_5450530_doc2_col1_decode_Huffman(unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) __attribute__((always_inline));

extern inline void q5_huff_0threads_5450530_year_col0_decode_Huffman(unsigned char* year_col0_ptr, uint32_t & year_col0_element) __attribute__((always_inline));

extern inline void q5_huff_0threads_5450530_author2_col0_decode_Huffman(unsigned char* author2_col0_ptr, uint32_t author2_col0_bytes, uint32_t & author2_fragment_size) __attribute__((always_inline));

void q5_huff_0threads_5450530_doc1_col0_decode_Huffman(unsigned char* doc1_col0_ptr, uint32_t doc1_col0_bytes, uint32_t & doc1_fragment_size) {

	bool* terminate_start = &(doc1_col0_huffman_terminator_array[0]);
	int* tree_array_start = &(doc1_col0_huffman_tree_array[0]);

	int mask = 0x100;

	while (doc1_col0_bytes > 1) {

		bool* terminator_array = terminate_start;
		int* tree_array = tree_array_start;

		while(!*terminator_array) { 

			char direction = *doc1_col0_ptr & (mask >>= 1);

			if (mask == 1) { 
				mask = 0x100;
				doc1_col0_ptr++;
				doc1_col0_bytes--;
			}

			terminator_array += *tree_array;
			tree_array += *tree_array;

			if (direction) {
				terminator_array++;
				tree_array++;
			}
		}

		doc1_col0_buffer[doc1_fragment_size++] = *tree_array;
	}

	if (mask != 0x100) {
		int bit_pos = mask;
		unsigned char last_byte = *doc1_col0_ptr;
		while (bit_pos > 1) {
			unsigned char bit = last_byte & (bit_pos >>= 1);
			if (bit) {
				bool* terminator_array = terminate_start;
				int* tree_array = tree_array_start;

				while (!*terminator_array) {
					char direction = *doc1_col0_ptr & (mask >>= 1);

					terminator_array += *tree_array;
					tree_array += *tree_array;

					if (direction) {
						terminator_array++;
						tree_array++;
					}
				}

				doc1_col0_buffer[doc1_fragment_size++] = *tree_array;
				bit_pos = mask;
			}
		}
	}
}

void q5_huff_0threads_5450530_term_col0_decode_Huffman(unsigned char* term_col0_ptr, uint32_t term_col0_bytes, uint32_t & term_fragment_size) {

	bool* terminate_start = &(term_col0_huffman_terminator_array[0]);
	int* tree_array_start = &(term_col0_huffman_tree_array[0]);

	int mask = 0x100;

	while (term_col0_bytes > 1) {

		bool* terminator_array = terminate_start;
		int* tree_array = tree_array_start;

		while(!*terminator_array) { 

			char direction = *term_col0_ptr & (mask >>= 1);

			if (mask == 1) { 
				mask = 0x100;
				term_col0_ptr++;
				term_col0_bytes--;
			}

			terminator_array += *tree_array;
			tree_array += *tree_array;

			if (direction) {
				terminator_array++;
				tree_array++;
			}
		}

		term_col0_buffer[term_fragment_size++] = *tree_array;
	}

	if (mask != 0x100) {
		int bit_pos = mask;
		unsigned char last_byte = *term_col0_ptr;
		while (bit_pos > 1) {
			unsigned char bit = last_byte & (bit_pos >>= 1);
			if (bit) {
				bool* terminator_array = terminate_start;
				int* tree_array = tree_array_start;

				while (!*terminator_array) {
					char direction = *term_col0_ptr & (mask >>= 1);

					terminator_array += *tree_array;
					tree_array += *tree_array;

					if (direction) {
						terminator_array++;
						tree_array++;
					}
				}

				term_col0_buffer[term_fragment_size++] = *tree_array;
				bit_pos = mask;
			}
		}
	}
}

void q5_huff_0threads_5450530_term_col1_decode_Huffman(unsigned char* term_col1_ptr, uint32_t term_fragment_size) {

	bool* terminate_start = &(term_col1_huffman_terminator_array[0]);
	int* tree_array_start = &(term_col1_huffman_tree_array[0]);

	int mask = 0x100;

	for (uint32_t i=0; i<term_fragment_size; i++) {

		bool* terminator_array = terminate_start;
		int* tree_array = tree_array_start;

		while(!*terminator_array) { 

			char direction = *term_col1_ptr & (mask >>= 1);

			if (mask == 1) { 
				mask = 0x100;
				term_col1_ptr++;
			}

			terminator_array += *tree_array;
			tree_array += *tree_array;

			if (direction) {
				terminator_array++;
				tree_array++;
			}
		}

		term_col1_buffer[i] = *tree_array;
	}
}

void q5_huff_0threads_5450530_doc2_col0_decode_Huffman(unsigned char* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) {

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

		doc2_col0_buffer[doc2_fragment_size++] = *tree_array;
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

				doc2_col0_buffer[doc2_fragment_size++] = *tree_array;
				bit_pos = mask;
			}
		}
	}
}

void q5_huff_0threads_5450530_doc2_col1_decode_Huffman(unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) {

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

		doc2_col1_buffer[i] = *tree_array;
	}
}

void q5_huff_0threads_5450530_year_col0_decode_Huffman(unsigned char* year_col0_ptr, uint32_t & year_col0_element) {

	int mask = 0x100;
	bool* terminator_array = &(year_col0_huffman_terminator_array[0]);
	int* tree_array = &(year_col0_huffman_tree_array[0]);

	while(!*terminator_array) {

		char direction = *year_col0_ptr & (mask >>= 1);

		if (mask == 1) {
			mask = 0x100;
			year_col0_ptr++;
		}

		terminator_array += *tree_array;
		tree_array += *tree_array;

		if (direction) {
			terminator_array++;
			tree_array++;
		}
	}

	year_col0_element = *tree_array;
}

void q5_huff_0threads_5450530_author2_col0_decode_Huffman(unsigned char* author2_col0_ptr, uint32_t author2_col0_bytes, uint32_t & author2_fragment_size) {

	bool* terminate_start = &(author2_col0_huffman_terminator_array[0]);
	int* tree_array_start = &(author2_col0_huffman_tree_array[0]);

	int mask = 0x100;

	while (author2_col0_bytes > 1) {

		bool* terminator_array = terminate_start;
		int* tree_array = tree_array_start;

		while(!*terminator_array) { 

			char direction = *author2_col0_ptr & (mask >>= 1);

			if (mask == 1) { 
				mask = 0x100;
				author2_col0_ptr++;
				author2_col0_bytes--;
			}

			terminator_array += *tree_array;
			tree_array += *tree_array;

			if (direction) {
				terminator_array++;
				tree_array++;
			}
		}

		author2_col0_buffer[author2_fragment_size++] = *tree_array;
	}

	if (mask != 0x100) {
		int bit_pos = mask;
		unsigned char last_byte = *author2_col0_ptr;
		while (bit_pos > 1) {
			unsigned char bit = last_byte & (bit_pos >>= 1);
			if (bit) {
				bool* terminator_array = terminate_start;
				int* tree_array = tree_array_start;

				while (!*terminator_array) {
					char direction = *author2_col0_ptr & (mask >>= 1);

					terminator_array += *tree_array;
					tree_array += *tree_array;

					if (direction) {
						terminator_array++;
						tree_array++;
					}
				}

				author2_col0_buffer[author2_fragment_size++] = *tree_array;
				bit_pos = mask;
			}
		}
	}
}

extern "C" double* q5_huff_0threads_5450530(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[0];
	doc1_col0_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[2];
	term_col0_buffer = new uint64_t[max_frag];
	term_col1_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[3];
	doc2_col0_buffer = new uint64_t[max_frag];
	doc2_col1_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[1];
	year_col0_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[4];
	author2_col0_buffer = new uint64_t[max_frag];

	RC = new int[metadata.idx_domains[4][0]]();
	R = new double[metadata.idx_domains[4][0]]();


	doc1_col0_huffman_tree_array = idx[0]->huffman_tree_array[0];
	doc1_col0_huffman_terminator_array = idx[0]->huffman_terminator_array[0];

	term_col0_huffman_tree_array = idx[2]->huffman_tree_array[0];
	term_col0_huffman_terminator_array = idx[2]->huffman_terminator_array[0];

	term_col1_huffman_tree_array = idx[2]->huffman_tree_array[1];
	term_col1_huffman_terminator_array = idx[2]->huffman_terminator_array[1];

	doc2_col0_huffman_tree_array = idx[3]->huffman_tree_array[0];
	doc2_col0_huffman_terminator_array = idx[3]->huffman_terminator_array[0];

	doc2_col1_huffman_tree_array = idx[3]->huffman_tree_array[1];
	doc2_col1_huffman_terminator_array = idx[3]->huffman_terminator_array[1];

	year_col0_huffman_tree_array = idx[1]->huffman_tree_array[0];
	year_col0_huffman_terminator_array = idx[1]->huffman_terminator_array[0];

	author2_col0_huffman_tree_array = idx[4]->huffman_tree_array[0];
	author2_col0_huffman_terminator_array = idx[4]->huffman_terminator_array[0];

	uint64_t author1_list[1];
	author1_list[0] = 5450530;

	for (int author1_it = 0; author1_it<1; author1_it++) {

		uint64_t author1_col0_element = author1_list[author1_it];

		uint32_t* row_doc1 = idx[0]->index_map[author1_col0_element];
		uint32_t doc1_col0_bytes = idx[0]->index_map[author1_col0_element+1][0] - row_doc1[0];
		if(doc1_col0_bytes) {

			unsigned char* doc1_col0_ptr = &(idx[0]->fragment_data[0][row_doc1[0]]);
			uint32_t doc1_fragment_size = 0;
			q5_huff_0threads_5450530_doc1_col0_decode_Huffman(doc1_col0_ptr, doc1_col0_bytes, doc1_fragment_size);

			for (uint32_t doc1_it = 0; doc1_it < doc1_fragment_size; doc1_it++) {

				uint32_t doc1_col0_element = doc1_col0_buffer[doc1_it];

				uint32_t* row_term = idx[2]->index_map[doc1_col0_element];
				uint32_t term_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_term[0];
				if(term_col0_bytes) {

					unsigned char* term_col0_ptr = &(idx[2]->fragment_data[0][row_term[0]]);
					uint32_t term_fragment_size = 0;
					q5_huff_0threads_5450530_term_col0_decode_Huffman(term_col0_ptr, term_col0_bytes, term_fragment_size);

					unsigned char* term_col1_ptr = &(idx[2]->fragment_data[1][row_term[1]]);
					q5_huff_0threads_5450530_term_col1_decode_Huffman(term_col1_ptr, term_fragment_size);

					for (uint32_t term_it = 0; term_it < term_fragment_size; term_it++) {

						uint32_t term_col0_element = term_col0_buffer[term_it];
						unsigned char term_col1_element = term_col1_buffer[term_it];

						uint32_t* row_doc2 = idx[3]->index_map[term_col0_element];
						uint32_t doc2_col0_bytes = idx[3]->index_map[term_col0_element+1][0] - row_doc2[0];
						if(doc2_col0_bytes) {

							unsigned char* doc2_col0_ptr = &(idx[3]->fragment_data[0][row_doc2[0]]);
							uint32_t doc2_fragment_size = 0;
							q5_huff_0threads_5450530_doc2_col0_decode_Huffman(doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

							unsigned char* doc2_col1_ptr = &(idx[3]->fragment_data[1][row_doc2[1]]);
							q5_huff_0threads_5450530_doc2_col1_decode_Huffman(doc2_col1_ptr, doc2_fragment_size);

							for (uint32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {

								uint32_t doc2_col0_element = doc2_col0_buffer[doc2_it];
								unsigned char doc2_col1_element = doc2_col1_buffer[doc2_it];

								uint32_t* row_year = idx[1]->index_map[doc2_col0_element];

								unsigned char* year_col0_ptr = &(idx[1]->fragment_data[0][row_year[0]]);
								uint32_t year_col0_element;
								q5_huff_0threads_5450530_year_col0_decode_Huffman(year_col0_ptr, year_col0_element);

								uint32_t* row_author2 = idx[4]->index_map[doc2_col0_element];
								uint32_t author2_col0_bytes = idx[4]->index_map[doc2_col0_element+1][0] - row_author2[0];
								if(author2_col0_bytes) {

									unsigned char* author2_col0_ptr = &(idx[4]->fragment_data[0][row_author2[0]]);
									uint32_t author2_fragment_size = 0;
									q5_huff_0threads_5450530_author2_col0_decode_Huffman(author2_col0_ptr, author2_col0_bytes, author2_fragment_size);

									for (uint32_t author2_it = 0; author2_it < author2_fragment_size; author2_it++) {
										uint32_t author2_col0_element = author2_col0_buffer[author2_it];

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


	delete[] doc1_col0_buffer;
	delete[] term_col0_buffer;
	delete[] term_col1_buffer;
	delete[] doc2_col0_buffer;
	delete[] doc2_col1_buffer;
	delete[] year_col0_buffer;
	delete[] author2_col0_buffer;


	*null_checks = RC;
	return R;

}

#endif

