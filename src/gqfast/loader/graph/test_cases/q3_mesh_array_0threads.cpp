#ifndef q3_mesh_array_0threads_
#define q3_mesh_array_0threads_

#include "../graph_index.hpp"
#include "../graph_global_vars.hpp"

#include <atomic>

using namespace std;

static int* R;
static int* RC;

static uint64_t* doc1_0_col0_intersection_buffer;
static uint64_t* doc1_1_col0_intersection_buffer;
static uint64_t* author1_col0_buffer;

static uint64_t* q3_mesh_array_0threads_intersection_buffer;

extern inline void q3_mesh_array_0threads_doc1_0_col0_intersection0_decode_UA(uint32_t* doc1_0_col0_intersection_ptr_0, uint32_t & doc1_0_intersection0_fragment_size) __attribute__((always_inline));

extern inline void q3_mesh_array_0threads_doc1_1_col0_intersection1_decode_UA(uint32_t* doc1_1_col0_intersection_ptr_1, uint32_t & doc1_1_intersection1_fragment_size) __attribute__((always_inline));

extern inline void q3_mesh_array_0threads_intersection(uint32_t doc1_0_intersection0_fragment_size, uint32_t doc1_1_intersection1_fragment_size, uint32_t & q3_mesh_array_0threads_intersection_size) __attribute__((always_inline));

extern inline void q3_mesh_array_0threads_author1_col0_decode_UA(uint32_t* author1_col0_ptr, uint32_t & author1_fragment_size) __attribute__((always_inline));

void q3_mesh_array_0threads_doc1_0_col0_intersection0_decode_UA(uint32_t* doc1_0_col0_intersection_ptr_0, uint32_t & doc1_0_intersection0_fragment_size) {

	uint32_t buffer_it = 0;
	for (uint32_t i=0; i<doc1_0_intersection0_fragment_size; i++) {
		uint32_t doc_candidate = *doc1_0_col0_intersection_ptr_0++;

        unsigned char* nodes_frag = idx[0]->index_map[doc_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == DOC_PROPERTY)
        {
            doc1_0_col0_intersection_buffer[buffer_it++] = doc_candidate;
		}
	}
	doc1_0_intersection0_fragment_size = buffer_it;
}

void q3_mesh_array_0threads_doc1_1_col0_intersection1_decode_UA(uint32_t* doc1_1_col0_intersection_ptr_1, uint32_t & doc1_1_intersection1_fragment_size) {

	uint32_t buffer_it = 0;
	for (uint32_t i=0; i<doc1_1_intersection1_fragment_size; i++) {
		uint32_t doc_candidate = *doc1_1_col0_intersection_ptr_1++;

        unsigned char* nodes_frag = idx[0]->index_map[doc_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == DOC_PROPERTY)
        {
            doc1_1_col0_intersection_buffer[buffer_it++] = doc_candidate;
		}
	}
	doc1_1_intersection1_fragment_size = buffer_it;
}

void q3_mesh_array_0threads_intersection(uint32_t doc1_0_intersection0_fragment_size, uint32_t doc1_1_intersection1_fragment_size, uint32_t & q3_mesh_array_0threads_intersection_size) {

	if (doc1_0_intersection0_fragment_size == 0) { return; }

	if (doc1_1_intersection1_fragment_size == 0) { return; }

	uint32_t intersection_index = 0;
	uint32_t* its = new uint32_t[2]();
	bool end = false;

	while(!end) {

		bool match = true;
		while (1) {
			if (doc1_0_col0_intersection_buffer[its[0]]  != doc1_1_col0_intersection_buffer[its[1]]) {
				match = false;
				break;
			}

			break;
		}

		if (match) {
			q3_mesh_array_0threads_intersection_buffer[intersection_index++] = doc1_0_col0_intersection_buffer[its[0]];
			while(1) {
				if (++its[0] == doc1_0_intersection0_fragment_size) {
					end = true;
					break;
				}
				if (++its[1] == doc1_1_intersection1_fragment_size) {
					end = true;
					break;
				}

				break;
			}
		}
		else {

			uint64_t smallest = doc1_0_col0_intersection_buffer[its[0]];
			int index_of_smallest = 0;
			uint32_t fragment_size_of_smallest = doc1_0_intersection0_fragment_size;

			if (smallest > doc1_1_col0_intersection_buffer[its[1]]) {
				smallest = doc1_1_col0_intersection_buffer[its[1]];
				index_of_smallest = 1;
				fragment_size_of_smallest = doc1_1_intersection1_fragment_size;
			}

			if (++its[index_of_smallest] == fragment_size_of_smallest) {
				end = true;
			}
		}
	}

	delete[] its;
	q3_mesh_array_0threads_intersection_size = intersection_index;
}

void q3_mesh_array_0threads_author1_col0_decode_UA(uint32_t* author1_col0_ptr, uint32_t & author1_fragment_size) {

	uint32_t buffer_it = 0;
	for (uint32_t i=0; i<author1_fragment_size; i++) {
		uint32_t author_candidate = *author1_col0_ptr++;

        unsigned char* nodes_frag = idx[0]->index_map[author_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == AUTHOR_PROPERTY)
        {
            author1_col0_buffer[buffer_it++] = author_candidate;
		}
	}

	author1_fragment_size = buffer_it;
}

extern "C" int* q3_mesh_array_0threads(int** null_checks, int term1, int term2) {

	benchmark_t1 = chrono::steady_clock::now();

	cerr << "check1\n";
	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[1];
	doc1_0_col0_intersection_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[1];
	doc1_1_col0_intersection_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[1];
	author1_col0_buffer = new uint64_t[max_frag];

    cerr << "check2\n";
	RC = new int[metadata.idx_domains[1][0]]();
	R = new int[metadata.idx_domains[1][0]]();


	q3_mesh_array_0threads_intersection_buffer = new uint64_t[metadata.idx_max_fragment_sizes[1]];

	unsigned char* frag_doc1_0_col0_intersection0 = idx[1]->index_map[term1+MAX_DOC_ID];
	uint32_t* frag_doc1_0_col0_intersection_ptr_0 = reinterpret_cast<uint32_t *>(&(frag_doc1_0_col0_intersection0));
	uint32_t doc1_0_intersection0_fragment_size = *frag_doc1_0_col0_intersection_ptr_0++;
    cerr << "check3\n";
	q3_mesh_array_0threads_doc1_0_col0_intersection0_decode_UA(frag_doc1_0_col0_intersection_ptr_0, doc1_0_intersection0_fragment_size);
    cerr << "check4\n";
	unsigned char* frag_doc1_1_col0_intersection1 = idx[1]->index_map[term2+MAX_DOC_ID];
	uint32_t* frag_doc1_1_col0_intersection_ptr_1 = reinterpret_cast<uint32_t *>(&(frag_doc1_1_col0_intersection1));
	cerr << "check5\n";
	uint32_t doc1_1_intersection1_fragment_size = *frag_doc1_1_col0_intersection_ptr_1++;
    cerr << "check6\n";
	q3_mesh_array_0threads_doc1_1_col0_intersection1_decode_UA(frag_doc1_1_col0_intersection_ptr_1, doc1_1_intersection1_fragment_size);

	uint32_t q3_mesh_array_0threads_intersection_size = 0;
	cerr << "check7\n";
	q3_mesh_array_0threads_intersection(doc1_0_intersection0_fragment_size, doc1_1_intersection1_fragment_size, q3_mesh_array_0threads_intersection_size);
    cerr << "check8\n";
	for (uint32_t q3_mesh_array_0threads_intersection_it= 0; q3_mesh_array_0threads_intersection_it<q3_mesh_array_0threads_intersection_size; q3_mesh_array_0threads_intersection_it++) {
        cerr << "begin loop1\n";
        unsigned char* frag_author1 = idx[1]->index_map[q3_mesh_array_0threads_intersection_buffer[q3_mesh_array_0threads_intersection_it]];
        if (frag_author1)
        {
            uint32_t* author1_col0_ptr = reinterpret_cast<uint32_t *>(&(frag_author1[0]));
            uint32_t author1_fragment_size = *author1_col0_ptr++;
            cerr << "check 9\n";
			q3_mesh_array_0threads_author1_col0_decode_UA(author1_col0_ptr, author1_fragment_size);
            cerr << "check 10\n";
			for (uint32_t author1_it = 0; author1_it < author1_fragment_size; author1_it++) {
				cerr << "begin loop2\n";
				uint32_t author1_col0_element = author1_col0_buffer[author1_it];
                cerr << "check 11\n";
				RC[author1_col0_element] = 1;
				R[author1_col0_element] += 1;
				cerr << "end loop2\n";
			}
		}
		cerr << "end loop1\n";
	}

    cerr << "check 12\n";
	delete[] author1_col0_buffer;
	delete[] doc1_0_col0_intersection_buffer;
	delete[] doc1_1_col0_intersection_buffer;
	delete[] q3_mesh_array_0threads_intersection_buffer;
    cerr << "check 13\n";

	*null_checks = RC;
	return R;

}

#endif

