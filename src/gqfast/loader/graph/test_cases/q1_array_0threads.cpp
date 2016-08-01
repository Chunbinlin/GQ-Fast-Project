#ifndef q1_array_0threads_
#define q1_array_0threads_

#include "../graph_index.hpp"
#include "../graph_global_vars.hpp"

#include <atomic>

using namespace std;

static int* R;
static int* RC;

static uint64_t* term1_col0_buffer;
static uint64_t* doc2_col0_buffer;

extern inline void q1_array_0threads_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) __attribute__((always_inline));

extern inline void q1_array_0threads_doc2_col0_decode_UA(uint32_t* doc2_col0_ptr, uint32_t doc2_col0_bytes, uint32_t & doc2_fragment_size) __attribute__((always_inline));

void q1_array_0threads_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t & term1_fragment_size) {

    int buffer_it = 0;
	for (uint32_t i=0; i<term1_fragment_size; i++) {
		uint32_t term_candidate = *term1_col0_ptr++;

        unsigned char* nodes_frag = idx[0]->index_map[term_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == TERM_PROPERTY)
        {
            term1_col0_buffer[buffer_it++] = term_candidate;
		}
	}
	term1_fragment_size = buffer_it;
}

void q1_array_0threads_doc2_col0_decode_UA(uint32_t* doc2_col0_ptr, uint32_t & doc2_fragment_size) {

	int buffer_it = 0;
	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		uint32_t doc_candidate = *doc2_col0_ptr++;

        unsigned char* nodes_frag = idx[0]->index_map[doc_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == DOC_PROPERTY)
        {
            doc2_col0_buffer[buffer_it++] = doc_candidate;
		}
	}
    doc2_fragment_size = buffer_it;
}

extern "C" int* q1_array_0threads(int** null_checks, int doc1) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[1];
	term1_col0_buffer = new uint64_t[max_frag];

    max_frag = metadata.idx_max_fragment_sizes[1];
    doc2_col0_buffer = new uint64_t[max_frag];

	RC = new int[metadata.idx_domains[1][0]]();
	R = new int[metadata.idx_domains[1][0]]();

	uint64_t doc1_list[1];
	doc1_list[0] = doc1;

	for (int doc1_it = 0; doc1_it<1; doc1_it++) {

		uint64_t doc1_col0_element = doc1_list[doc1_it];

		unsigned char* frag_term1 = idx[1]->index_map[doc1_col0_element];
		if (frag_term1)
		{
            uint32_t* term1_frag_ptr = reinterpret_cast<uint32_t *>(&(frag_term1[0]));
            uint32_t term1_frag_size = *term1_frag_ptr;
		    term1_frag_ptr++;
            q1_array_0threads_term1_col0_decode_UA(term1_frag_ptr, term1_frag_size);

			for (uint32_t term1_it = 0; term1_it < term1_frag_size; term1_it++) {

				uint32_t term1_col0_element = term1_col0_buffer[term1_it];

				unsigned char* frag_doc2 = idx[1]->index_map[term1_col0_element];
				if (frag_doc2)
				{
                    uint32_t* doc2_frag_ptr = reinterpret_cast<uint32_t *>(&(frag_doc2[0]));
                    uint32_t doc2_frag_size = *doc2_frag_ptr;
                    doc2_frag_ptr++;

					q1_array_0threads_doc2_col0_decode_UA(doc2_frag_ptr, doc2_frag_size);

					for (uint32_t doc2_it = 0; doc2_it < doc2_frag_size; doc2_it++) {

						uint32_t doc2_col0_element = doc2_col0_buffer[doc2_it];

						RC[doc2_col0_element] = 1;
						R[doc2_col0_element] += 1;
					}
				}
			}
		}

	}


	delete[] term1_col0_buffer;
	delete[] doc2_col0_buffer;


	*null_checks = RC;
	return R;

}

#endif

