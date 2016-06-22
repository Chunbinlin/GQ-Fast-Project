#ifndef q4_mesh_opt_2threads_535_1478_
#define q4_mesh_opt_2threads_535_1478_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 2

using namespace std;

static args_threading arguments[NUM_THREADS];


static int* R;
static int* RC;

static uint64_t* doc1_0_col0_intersection_buffer;
static uint64_t* doc1_1_col0_intersection_buffer;
static uint64_t** term1_col0_buffer;
static uint64_t** term1_col1_buffer;

static uint64_t* q4_mesh_opt_2threads_535_1478_intersection_buffer;

static int* term1_col1_huffman_tree_array;
static bool* term1_col1_huffman_terminator_array;

extern inline void q4_mesh_opt_2threads_535_1478_doc1_0_col0_intersection0_decode_BB(unsigned char* doc1_0_col0_intersection_ptr_0, uint32_t doc1_0_col0_bytes_intersection0, uint32_t & doc1_0_intersection0_fragment_size) __attribute__((always_inline));

extern inline void q4_mesh_opt_2threads_535_1478_doc1_1_col0_intersection1_decode_BB(unsigned char* doc1_1_col0_intersection_ptr_1, uint32_t doc1_1_col0_bytes_intersection1, uint32_t & doc1_1_intersection1_fragment_size) __attribute__((always_inline));

extern inline void q4_mesh_opt_2threads_535_1478_intersection(uint32_t doc1_0_intersection0_fragment_size, uint32_t doc1_1_intersection1_fragment_size, uint32_t & q4_mesh_opt_2threads_535_1478_intersection_size) __attribute__((always_inline));

void* pthread_q4_mesh_opt_2threads_535_1478_worker(void* arguments);

extern inline void q4_mesh_opt_2threads_535_1478_term1_col0_decode_BB_threaded(int thread_id, unsigned char* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) __attribute__((always_inline));

extern inline void q4_mesh_opt_2threads_535_1478_term1_col1_decode_Huffman_threaded(int thread_id, unsigned char* term1_col1_ptr, uint32_t term1_fragment_size) __attribute__((always_inline));

void q4_mesh_opt_2threads_535_1478_doc1_0_col0_intersection0_decode_BB(unsigned char* doc1_0_col0_intersection_ptr_0, uint32_t doc1_0_col0_bytes_intersection0, uint32_t & doc1_0_intersection0_fragment_size) {

	doc1_0_col0_intersection_buffer[0] = 0;

	int shiftbits = 0;
	do { 
		doc1_0_col0_bytes_intersection0--;
		uint32_t next_seven_bits = *doc1_0_col0_intersection_ptr_0 & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		doc1_0_col0_intersection_buffer[0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*doc1_0_col0_intersection_ptr_0++ & 128));
	doc1_0_intersection0_fragment_size++;

	while (doc1_0_col0_bytes_intersection0 > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			doc1_0_col0_bytes_intersection0--;
			uint32_t next_seven_bits = *doc1_0_col0_intersection_ptr_0 & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*doc1_0_col0_intersection_ptr_0++ & 128));
		doc1_0_col0_intersection_buffer[doc1_0_intersection0_fragment_size] = doc1_0_col0_intersection_buffer[doc1_0_intersection0_fragment_size-1]+1+result;
		doc1_0_intersection0_fragment_size++;
	}
}

void q4_mesh_opt_2threads_535_1478_doc1_1_col0_intersection1_decode_BB(unsigned char* doc1_1_col0_intersection_ptr_1, uint32_t doc1_1_col0_bytes_intersection1, uint32_t & doc1_1_intersection1_fragment_size) {

	doc1_1_col0_intersection_buffer[0] = 0;

	int shiftbits = 0;
	do { 
		doc1_1_col0_bytes_intersection1--;
		uint32_t next_seven_bits = *doc1_1_col0_intersection_ptr_1 & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		doc1_1_col0_intersection_buffer[0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*doc1_1_col0_intersection_ptr_1++ & 128));
	doc1_1_intersection1_fragment_size++;

	while (doc1_1_col0_bytes_intersection1 > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			doc1_1_col0_bytes_intersection1--;
			uint32_t next_seven_bits = *doc1_1_col0_intersection_ptr_1 & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*doc1_1_col0_intersection_ptr_1++ & 128));
		doc1_1_col0_intersection_buffer[doc1_1_intersection1_fragment_size] = doc1_1_col0_intersection_buffer[doc1_1_intersection1_fragment_size-1]+1+result;
		doc1_1_intersection1_fragment_size++;
	}
}
void q4_mesh_opt_2threads_535_1478_intersection(uint32_t doc1_0_intersection0_fragment_size, uint32_t doc1_1_intersection1_fragment_size, uint32_t & q4_mesh_opt_2threads_535_1478_intersection_size) { 

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
			q4_mesh_opt_2threads_535_1478_intersection_buffer[intersection_index++] = doc1_0_col0_intersection_buffer[its[0]];
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
	q4_mesh_opt_2threads_535_1478_intersection_size = intersection_index;
}

void* pthread_q4_mesh_opt_2threads_535_1478_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t q4_mesh_opt_2threads_535_1478_intersection_it = args->start;
	uint32_t q4_mesh_opt_2threads_535_1478_intersection_size = args->end;
	int thread_id = args->thread_id;

	for (; q4_mesh_opt_2threads_535_1478_intersection_it<q4_mesh_opt_2threads_535_1478_intersection_size; q4_mesh_opt_2threads_535_1478_intersection_it++) {

		uint32_t* row_term1 = idx[2]->index_map[q4_mesh_opt_2threads_535_1478_intersection_buffer[q4_mesh_opt_2threads_535_1478_intersection_it]];
		uint32_t term1_col0_bytes = idx[2]->index_map[q4_mesh_opt_2threads_535_1478_intersection_buffer[q4_mesh_opt_2threads_535_1478_intersection_it]+1][0] - row_term1[0];
		if(term1_col0_bytes) {

			unsigned char* term1_col0_ptr = &(idx[2]->fragment_data[0][row_term1[0]]);
			uint32_t term1_fragment_size = 0;
			q4_mesh_opt_2threads_535_1478_term1_col0_decode_BB_threaded(thread_id, term1_col0_ptr, term1_col0_bytes, term1_fragment_size);

			unsigned char* term1_col1_ptr = &(idx[2]->fragment_data[1][row_term1[1]]);
			q4_mesh_opt_2threads_535_1478_term1_col1_decode_Huffman_threaded(thread_id, term1_col1_ptr, term1_fragment_size);

			for (uint32_t term1_it = 0; term1_it < term1_fragment_size; term1_it++) {
				uint32_t term1_col0_element = term1_col0_buffer[thread_id][term1_it];
				unsigned char term1_col1_element = term1_col1_buffer[thread_id][term1_it];

				RC[term1_col0_element] = 1;

				pthread_spin_lock(&spin_locks[2][term1_col0_element]);
				R[term1_col0_element] += term1_col1_element;
				pthread_spin_unlock(&spin_locks[2][term1_col0_element]);

			}
		}
	}
	return nullptr;
}

void q4_mesh_opt_2threads_535_1478_term1_col0_decode_BB_threaded(int thread_id, unsigned char* term1_col0_ptr, uint32_t term1_col0_bytes, uint32_t & term1_fragment_size) {

	term1_col0_buffer[thread_id][0] = 0;

	int shiftbits = 0;
	do { 
		term1_col0_bytes--;
		uint32_t next_seven_bits = *term1_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		term1_col0_buffer[thread_id][0] |= next_seven_bits;
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
		term1_col0_buffer[thread_id][term1_fragment_size] = term1_col0_buffer[thread_id][term1_fragment_size-1]+1+result;
		term1_fragment_size++;
	}
}

void q4_mesh_opt_2threads_535_1478_term1_col1_decode_Huffman_threaded(int thread_id, unsigned char* term1_col1_ptr, uint32_t term1_fragment_size) {

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

		term1_col1_buffer[thread_id][i] = *tree_array;
	}
}

extern "C" int* q4_mesh_opt_2threads_535_1478(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[3];
	doc1_0_col0_intersection_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[3];
	doc1_1_col0_intersection_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[2];
	term1_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		term1_col0_buffer[i] = new uint64_t[max_frag];
	}
	term1_col1_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		term1_col1_buffer[i] = new uint64_t[max_frag];
	}

	RC = new int[metadata.idx_domains[2][0]]();
	R = new int[metadata.idx_domains[2][0]]();


	q4_mesh_opt_2threads_535_1478_intersection_buffer = new uint64_t[metadata.idx_max_fragment_sizes[3]];

	term1_col1_huffman_tree_array = idx[2]->huffman_tree_array[1];
	term1_col1_huffman_terminator_array = idx[2]->huffman_terminator_array[1];

	uint32_t* row_doc1_0_intersection0 = idx[3]->index_map[535];
	uint32_t doc1_0_col0_bytes_intersection0 = idx[3]->index_map[535+1][0] - row_doc1_0_intersection0[0];
	unsigned char* doc1_0_col0_intersection_ptr_0 = &(idx[3]->fragment_data[0][row_doc1_0_intersection0[0]]);
	uint32_t doc1_0_intersection0_fragment_size = 0;
	q4_mesh_opt_2threads_535_1478_doc1_0_col0_intersection0_decode_BB(doc1_0_col0_intersection_ptr_0, doc1_0_col0_bytes_intersection0, doc1_0_intersection0_fragment_size);

	uint32_t* row_doc1_1_intersection1 = idx[3]->index_map[1478];
	uint32_t doc1_1_col0_bytes_intersection1 = idx[3]->index_map[1478+1][0] - row_doc1_1_intersection1[0];
	unsigned char* doc1_1_col0_intersection_ptr_1 = &(idx[3]->fragment_data[0][row_doc1_1_intersection1[0]]);
	uint32_t doc1_1_intersection1_fragment_size = 0;
	q4_mesh_opt_2threads_535_1478_doc1_1_col0_intersection1_decode_BB(doc1_1_col0_intersection_ptr_1, doc1_1_col0_bytes_intersection1, doc1_1_intersection1_fragment_size);

	uint32_t q4_mesh_opt_2threads_535_1478_intersection_size = 0;
	q4_mesh_opt_2threads_535_1478_intersection(doc1_0_intersection0_fragment_size, doc1_1_intersection1_fragment_size, q4_mesh_opt_2threads_535_1478_intersection_size);

	uint32_t thread_size = q4_mesh_opt_2threads_535_1478_intersection_size/NUM_THREADS;
	uint32_t position = 0;

	for (int i=0; i<NUM_THREADS; i++) {
		arguments[i].start = position;
		position += thread_size;
		arguments[i].end = position;
		arguments[i].thread_id = i;
	}
	arguments[NUM_THREADS-1].end = q4_mesh_opt_2threads_535_1478_intersection_size;

	for (int i=0; i<NUM_THREADS; i++) {
		pthread_create(&threads[i], NULL, &pthread_q4_mesh_opt_2threads_535_1478_worker, (void *) &arguments[i]);
	}

	for (int i=0; i<NUM_THREADS; i++) {
		pthread_join(threads[i], NULL);
	}


	for (int i=0; i<NUM_THREADS; i++) {
		delete[] term1_col0_buffer[i];
	}
	delete[] term1_col0_buffer;
	for (int i=0; i<NUM_THREADS; i++) {
		delete[] term1_col1_buffer[i];
	}
	delete[] term1_col1_buffer;
	delete[] doc1_0_col0_intersection_buffer;
	delete[] doc1_1_col0_intersection_buffer;
	delete[] q4_mesh_opt_2threads_535_1478_intersection_buffer;


	*null_checks = RC;
	return R;

}

#endif

