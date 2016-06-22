#ifndef q2_array_0threads_17044542_
#define q2_array_0threads_17044542_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"


using namespace std;

static double* R;
static int* RC;

static uint64_t* year1_col0_buffer;
static uint64_t* term1_col0_buffer;
static uint64_t* term1_col1_buffer;
static uint64_t* doc2_col0_buffer;
static uint64_t* doc2_col1_buffer;
static uint64_t* year2_col0_buffer;

extern inline void q2_array_0threads_17044542_year1_col0_decode_UA(uint32_t* year1_col0_ptr, uint32_t & year1_col0_element) __attribute__((always_inline));

extern inline void q2_array_0threads_17044542_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) __attribute__((always_inline));

extern inline void q2_array_0threads_17044542_term1_col1_decode_UA(unsigned char* term1_col1_ptr, uint32_t term1_fragment_size) __attribute__((always_inline));

extern inline void q2_array_0threads_17044542_doc2_col0_decode_UA(uint32_t* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) __attribute__((always_inline));

extern inline void q2_array_0threads_17044542_doc2_col1_decode_UA(unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) __attribute__((always_inline));

extern inline void q2_array_0threads_17044542_year2_col0_decode_UA(uint32_t* year2_col0_ptr, uint32_t & year2_col0_element) __attribute__((always_inline));

void q2_array_0threads_17044542_year1_col0_decode_UA(uint32_t* year1_col0_ptr, uint32_t & year1_col0_element) {

	year1_col0_element = *year1_col0_ptr;
}

void q2_array_0threads_17044542_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) {

	term1_fragment_size = term1_col0_bytes/4;

	for (uint32_t i=0; i<term1_fragment_size; i++) {
		term1_col0_buffer[i] = *term1_col0_ptr++;
	}
}

void q2_array_0threads_17044542_term1_col1_decode_UA(unsigned char* term1_col1_ptr, uint32_t term1_fragment_size) {

	for (uint32_t i=0; i<term1_fragment_size; i++) {
		term1_col1_buffer[i] = *term1_col1_ptr++;
	}
}

void q2_array_0threads_17044542_doc2_col0_decode_UA(uint32_t* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) {

	doc2_fragment_size = doc2_col0_bytes/4;

	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		doc2_col0_buffer[i] = *doc2_col0_ptr++;
	}
}

void q2_array_0threads_17044542_doc2_col1_decode_UA(unsigned char* doc2_col1_ptr, uint32_t doc2_fragment_size) {

	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		doc2_col1_buffer[i] = *doc2_col1_ptr++;
	}
}

void q2_array_0threads_17044542_year2_col0_decode_UA(uint32_t* year2_col0_ptr, uint32_t & year2_col0_element) {

	year2_col0_element = *year2_col0_ptr;
}

extern "C" double* q2_array_0threads_17044542(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[1];
	year1_col0_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[2];
	term1_col0_buffer = new uint64_t[max_frag];
	term1_col1_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[3];
	doc2_col0_buffer = new uint64_t[max_frag];
	doc2_col1_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[1];
	year2_col0_buffer = new uint64_t[max_frag];

	RC = new int[metadata.idx_domains[3][0]]();
	R = new double[metadata.idx_domains[3][0]]();


	uint64_t doc1_list[1];
	doc1_list[0] = 17044542;

	for (int doc1_it = 0; doc1_it<1; doc1_it++) {

		uint64_t doc1_col0_element = doc1_list[doc1_it];

		uint32_t* row_year1 = idx[1]->index_map[doc1_col0_element];

		uint32_t* year1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[1]->fragment_data[0][row_year1[0]]));
		uint32_t year1_col0_element;
		q2_array_0threads_17044542_year1_col0_decode_UA(year1_col0_ptr, year1_col0_element);

		uint32_t* row_term1 = idx[2]->index_map[doc1_col0_element];
		uint32_t term1_col0_bytes = idx[2]->index_map[doc1_col0_element+1][0] - row_term1[0];
		if(term1_col0_bytes) {

			uint32_t* term1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[2]->fragment_data[0][row_term1[0]]));
			uint32_t term1_fragment_size = 0;
			q2_array_0threads_17044542_term1_col0_decode_UA(term1_col0_ptr, term1_col0_bytes, term1_fragment_size);

			unsigned char* term1_col1_ptr = &(idx[2]->fragment_data[1][row_term1[1]]);
			q2_array_0threads_17044542_term1_col1_decode_UA(term1_col1_ptr, term1_fragment_size);

			for (uint32_t term1_it = 0; term1_it < term1_fragment_size; term1_it++) {

				uint32_t term1_col0_element = term1_col0_buffer[term1_it];
				unsigned char term1_col1_element = term1_col1_buffer[term1_it];

				uint32_t* row_doc2 = idx[3]->index_map[term1_col0_element];
				uint32_t doc2_col0_bytes = idx[3]->index_map[term1_col0_element+1][0] - row_doc2[0];
				if(doc2_col0_bytes) {

					uint32_t* doc2_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[3]->fragment_data[0][row_doc2[0]]));
					uint32_t doc2_fragment_size = 0;
					q2_array_0threads_17044542_doc2_col0_decode_UA(doc2_col0_ptr, doc2_col0_bytes, doc2_fragment_size);

					unsigned char* doc2_col1_ptr = &(idx[3]->fragment_data[1][row_doc2[1]]);
					q2_array_0threads_17044542_doc2_col1_decode_UA(doc2_col1_ptr, doc2_fragment_size);

					for (uint32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {

						uint32_t doc2_col0_element = doc2_col0_buffer[doc2_it];
						unsigned char doc2_col1_element = doc2_col1_buffer[doc2_it];

						uint32_t* row_year2 = idx[1]->index_map[doc2_col0_element];

						uint32_t* year2_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[1]->fragment_data[0][row_year2[0]]));
						uint32_t year2_col0_element;
						q2_array_0threads_17044542_year2_col0_decode_UA(year2_col0_ptr, year2_col0_element);

						RC[doc2_col0_element] = 1;
						R[doc2_col0_element] += (double)(term1_col1_element*doc2_col1_element)/(ABS((int)year1_col0_element-(int)year2_col0_element)+1);
					}
				}
			}
		}
	}


	delete[] year1_col0_buffer;
	delete[] term1_col0_buffer;
	delete[] term1_col1_buffer;
	delete[] doc2_col0_buffer;
	delete[] doc2_col1_buffer;
	delete[] year2_col0_buffer;


	*null_checks = RC;
	return R;

}

#endif

