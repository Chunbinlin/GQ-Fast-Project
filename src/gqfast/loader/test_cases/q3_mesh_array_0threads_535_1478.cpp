#ifndef q3_mesh_array_0threads_535_1478_
#define q3_mesh_array_0threads_535_1478_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 1
#define BUFFER_POOL_SIZE 2

using namespace std;

static int* R;
static int* RC;

static uint64_t*** index3_col0_buffer;
static uint64_t*** index3_col1_buffer;
static uint64_t*** index4_col0_buffer;

static uint64_t* q3_mesh_array_0threads_535_1478_intersection_buffer;

extern inline void q3_mesh_array_0threads_535_1478_doc1_col0_intersection0_decode_UA(uint32_t* doc1_col0_intersection_ptr_0, uint32_t doc1_col0_bytes_intersection0, uint32_t & doc1_intersection0_fragment_size) __attribute__((always_inline));

extern inline void q3_mesh_array_0threads_535_1478_doc1_col0_intersection1_decode_UA(uint32_t* doc1_col0_intersection_ptr_1, uint32_t doc1_col0_bytes_intersection1, uint32_t & doc1_intersection1_fragment_size) __attribute__((always_inline));

extern inline void q3_mesh_array_0threads_535_1478_intersection(uint32_t doc1_intersection0_fragment_size, uint32_t doc1_intersection1_fragment_size, uint32_t & q3_mesh_array_0threads_535_1478_intersection_size) __attribute__((always_inline));

extern inline void q3_mesh_array_0threads_535_1478_author1_col0_decode_UA(uint32_t* author1_col0_ptr, uint32_t author1_col0_bytes, uint32_t & author1_fragment_size) __attribute__((always_inline));

void q3_mesh_array_0threads_535_1478_doc1_col0_intersection0_decode_UA(uint32_t* doc1_col0_intersection_ptr_0, uint32_t doc1_col0_bytes_intersection0, uint32_t & doc1_intersection0_fragment_size) {

	doc1_intersection0_fragment_size = doc1_col0_bytes_intersection0/4;

	for (uint32_t i=0; i<doc1_intersection0_fragment_size; i++) {
		index3_col0_buffer[0][0][i] = *doc1_col0_intersection_ptr_0++;
	}
}

void q3_mesh_array_0threads_535_1478_doc1_col0_intersection1_decode_UA(uint32_t* doc1_col0_intersection_ptr_1, uint32_t doc1_col0_bytes_intersection1, uint32_t & doc1_intersection1_fragment_size) {

	doc1_intersection1_fragment_size = doc1_col0_bytes_intersection1/4;

	for (uint32_t i=0; i<doc1_intersection1_fragment_size; i++) {
		index3_col0_buffer[0][1][i] = *doc1_col0_intersection_ptr_1++;
	}
}
void q3_mesh_array_0threads_535_1478_intersection(uint32_t doc1_intersection0_fragment_size, uint32_t doc1_intersection1_fragment_size, uint32_t & q3_mesh_array_0threads_535_1478_intersection_size) { 

	if (doc1_intersection0_fragment_size == 0) { return; }

	if (doc1_intersection1_fragment_size == 0) { return; }

	uint32_t intersection_index = 0;
	uint32_t* its = new uint32_t[2]();
	bool end = false;

	while(!end) {

		bool match = true;
		while (1) {
			if (index3_col0_buffer[0][0][its[0]]  != index3_col0_buffer[0][1][its[1]]) {
				match = false;
				break;
			}

			break;
		}

		if (match) {
			q3_mesh_array_0threads_535_1478_intersection_buffer[intersection_index++] = index3_col0_buffer[0][0][its[0]];
			while(1) {
				if (++its[0] == doc1_intersection0_fragment_size) {
					end = true;
					break;
				}
				if (++its[1] == doc1_intersection1_fragment_size) {
					end = true;
					break;
				}

				break;
			}
		}
		else {

			uint64_t smallest = index3_col0_buffer[0][0][its[0]];
			int index_of_smallest = 0;
			uint32_t fragment_size_of_smallest = doc1_intersection0_fragment_size;

			if (smallest > index3_col0_buffer[0][1][its[1]]) {
				smallest = index3_col0_buffer[0][1][its[1]];
				index_of_smallest = 1;
				fragment_size_of_smallest = doc1_intersection1_fragment_size;
			}

			if (++its[index_of_smallest] == fragment_size_of_smallest) {
				end = true;
			}
		}
	}

	delete[] its;
	q3_mesh_array_0threads_535_1478_intersection_size = intersection_index;
}

void q3_mesh_array_0threads_535_1478_author1_col0_decode_UA(uint32_t* author1_col0_ptr, uint32_t author1_col0_bytes, uint32_t & author1_fragment_size) {

	author1_fragment_size = author1_col0_bytes/4;

	for (uint32_t i=0; i<author1_fragment_size; i++) {
		index4_col0_buffer[0][0][i] = *author1_col0_ptr++;
	}
}

extern "C" int* q3_mesh_array_0threads_535_1478(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

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
	R = new int[metadata.idx_domains[4][0]]();


	q3_mesh_array_0threads_535_1478_intersection_buffer = new uint64_t[metadata.idx_max_fragment_sizes[3]];

	uint32_t* row_doc1_intersection0 = idx[3]->index_map[535];
	uint32_t doc1_col0_bytes_intersection0 = idx[3]->index_map[535+1][0] - row_doc1_intersection0[0];
	uint32_t* doc1_col0_intersection_ptr_0 = reinterpret_cast<uint32_t *>(&(idx[3]->fragment_data[0][row_doc1_intersection0[0]]));
	uint32_t doc1_intersection0_fragment_size = 0;
	q3_mesh_array_0threads_535_1478_doc1_col0_intersection0_decode_UA(doc1_col0_intersection_ptr_0, doc1_col0_bytes_intersection0, doc1_intersection0_fragment_size);

	uint32_t* row_doc1_intersection1 = idx[3]->index_map[1478];
	uint32_t doc1_col0_bytes_intersection1 = idx[3]->index_map[1478+1][0] - row_doc1_intersection1[0];
	uint32_t* doc1_col0_intersection_ptr_1 = reinterpret_cast<uint32_t *>(&(idx[3]->fragment_data[0][row_doc1_intersection1[0]]));
	uint32_t doc1_intersection1_fragment_size = 0;
	q3_mesh_array_0threads_535_1478_doc1_col0_intersection1_decode_UA(doc1_col0_intersection_ptr_1, doc1_col0_bytes_intersection1, doc1_intersection1_fragment_size);

	uint32_t q3_mesh_array_0threads_535_1478_intersection_size = 0;
	q3_mesh_array_0threads_535_1478_intersection(doc1_intersection0_fragment_size, doc1_intersection1_fragment_size, q3_mesh_array_0threads_535_1478_intersection_size);

	for (uint32_t q3_mesh_array_0threads_535_1478_intersection_it= 0; q3_mesh_array_0threads_535_1478_intersection_it<q3_mesh_array_0threads_535_1478_intersection_size; q3_mesh_array_0threads_535_1478_intersection_it++) {

		uint32_t* row_author1 = idx[4]->index_map[q3_mesh_array_0threads_535_1478_intersection_buffer[q3_mesh_array_0threads_535_1478_intersection_it]];
		uint32_t author1_col0_bytes = idx[4]->index_map[q3_mesh_array_0threads_535_1478_intersection_buffer[q3_mesh_array_0threads_535_1478_intersection_it]+1][0] - row_author1[0];
		if(author1_col0_bytes) {

			uint32_t* author1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[4]->fragment_data[0][row_author1[0]]));
			uint32_t author1_fragment_size = 0;
			q3_mesh_array_0threads_535_1478_author1_col0_decode_UA(author1_col0_ptr, author1_col0_bytes, author1_fragment_size);

			for (uint32_t author1_it = 0; author1_it < author1_fragment_size; author1_it++) {
				uint32_t author1_col0_element = index4_col0_buffer[0][0][author1_it];

				RC[author1_col0_element] = 1;
				R[author1_col0_element] += 1;
			}
		}
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
	delete[] q3_mesh_array_0threads_535_1478_intersection_buffer;


	*null_checks = RC;
	return R;

}

#endif

