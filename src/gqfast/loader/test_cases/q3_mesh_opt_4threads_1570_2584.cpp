#ifndef q3_mesh_opt_4threads_1570_2584_
#define q3_mesh_opt_4threads_1570_2584_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 4

using namespace std;

static args_threading arguments[NUM_THREADS];


static int* R;
static int* RC;

static uint64_t* doc1_0_col0_intersection_buffer;
static uint64_t* doc1_1_col0_intersection_buffer;
static uint64_t** author1_col0_buffer;

static uint64_t* q3_mesh_opt_4threads_1570_2584_intersection_buffer;

static uint32_t* author1_col0_bits_info;
static uint64_t author1_col0_offset;

extern inline void q3_mesh_opt_4threads_1570_2584_doc1_0_col0_intersection0_decode_BB(unsigned char* doc1_0_col0_intersection_ptr_0, uint32_t doc1_0_col0_bytes_intersection0, uint32_t & doc1_0_intersection0_fragment_size) __attribute__((always_inline));

extern inline void q3_mesh_opt_4threads_1570_2584_doc1_1_col0_intersection1_decode_BB(unsigned char* doc1_1_col0_intersection_ptr_1, uint32_t doc1_1_col0_bytes_intersection1, uint32_t & doc1_1_intersection1_fragment_size) __attribute__((always_inline));

extern inline void q3_mesh_opt_4threads_1570_2584_intersection(uint32_t doc1_0_intersection0_fragment_size, uint32_t doc1_1_intersection1_fragment_size, uint32_t & q3_mesh_opt_4threads_1570_2584_intersection_size) __attribute__((always_inline));

void* pthread_q3_mesh_opt_4threads_1570_2584_worker(void* arguments);

extern inline void q3_mesh_opt_4threads_1570_2584_author1_col0_decode_BCA_threaded(int thread_id, unsigned char* author1_col0_ptr, uint32_t author1_col0_bytes, uint32_t & author1_fragment_size) __attribute__((always_inline));

void q3_mesh_opt_4threads_1570_2584_doc1_0_col0_intersection0_decode_BB(unsigned char* doc1_0_col0_intersection_ptr_0, uint32_t doc1_0_col0_bytes_intersection0, uint32_t & doc1_0_intersection0_fragment_size) {

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

void q3_mesh_opt_4threads_1570_2584_doc1_1_col0_intersection1_decode_BB(unsigned char* doc1_1_col0_intersection_ptr_1, uint32_t doc1_1_col0_bytes_intersection1, uint32_t & doc1_1_intersection1_fragment_size) {

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
void q3_mesh_opt_4threads_1570_2584_intersection(uint32_t doc1_0_intersection0_fragment_size, uint32_t doc1_1_intersection1_fragment_size, uint32_t & q3_mesh_opt_4threads_1570_2584_intersection_size) { 

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
			q3_mesh_opt_4threads_1570_2584_intersection_buffer[intersection_index++] = doc1_0_col0_intersection_buffer[its[0]];
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
	q3_mesh_opt_4threads_1570_2584_intersection_size = intersection_index;
}

void* pthread_q3_mesh_opt_4threads_1570_2584_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t q3_mesh_opt_4threads_1570_2584_intersection_it = args->start;
	uint32_t q3_mesh_opt_4threads_1570_2584_intersection_size = args->end;
	int thread_id = args->thread_id;

	for (; q3_mesh_opt_4threads_1570_2584_intersection_it<q3_mesh_opt_4threads_1570_2584_intersection_size; q3_mesh_opt_4threads_1570_2584_intersection_it++) {

		uint32_t* row_author1 = idx[4]->index_map[q3_mesh_opt_4threads_1570_2584_intersection_buffer[q3_mesh_opt_4threads_1570_2584_intersection_it]];
		uint32_t author1_col0_bytes = idx[4]->index_map[q3_mesh_opt_4threads_1570_2584_intersection_buffer[q3_mesh_opt_4threads_1570_2584_intersection_it]+1][0] - row_author1[0];
		if(author1_col0_bytes) {

			unsigned char* author1_col0_ptr = &(idx[4]->fragment_data[0][row_author1[0]]);
			uint32_t author1_fragment_size = 0;
			q3_mesh_opt_4threads_1570_2584_author1_col0_decode_BCA_threaded(thread_id, author1_col0_ptr, author1_col0_bytes, author1_fragment_size);

			for (uint32_t author1_it = 0; author1_it < author1_fragment_size; author1_it++) {
				uint32_t author1_col0_element = author1_col0_buffer[thread_id][author1_it];

				RC[author1_col0_element] = 1;

				pthread_spin_lock(&spin_locks[4][author1_col0_element]);
				R[author1_col0_element] += 1;
				pthread_spin_unlock(&spin_locks[4][author1_col0_element]);

			}
		}
	}
	return nullptr;
}

void q3_mesh_opt_4threads_1570_2584_author1_col0_decode_BCA_threaded(int thread_id, unsigned char* author1_col0_ptr, uint32_t author1_col0_bytes, uint32_t & author1_fragment_size) {

	author1_fragment_size = author1_col0_bytes* 8 / author1_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<author1_fragment_size; i++) {
		uint32_t encoded_value = author1_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(author1_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		author1_col0_ptr += (bit_pos + author1_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + author1_col0_bits_info[0]) % 8;
		author1_col0_buffer[thread_id][i] = encoded_value + author1_col0_offset;
	}
}

extern "C" int* q3_mesh_opt_4threads_1570_2584(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[3];
	doc1_0_col0_intersection_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[3];
	doc1_1_col0_intersection_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[4];
	author1_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		author1_col0_buffer[i] = new uint64_t[max_frag];
	}

	RC = new int[metadata.idx_domains[4][0]]();
	R = new int[metadata.idx_domains[4][0]]();


	q3_mesh_opt_4threads_1570_2584_intersection_buffer = new uint64_t[metadata.idx_max_fragment_sizes[3]];

	author1_col0_bits_info = idx[4]->dict[0]->bits_info;
	author1_col0_offset = idx[4]->dict[0]->offset;

	uint32_t* row_doc1_0_intersection0 = idx[3]->index_map[1570];
	uint32_t doc1_0_col0_bytes_intersection0 = idx[3]->index_map[1570+1][0] - row_doc1_0_intersection0[0];
	unsigned char* doc1_0_col0_intersection_ptr_0 = &(idx[3]->fragment_data[0][row_doc1_0_intersection0[0]]);
	uint32_t doc1_0_intersection0_fragment_size = 0;
	q3_mesh_opt_4threads_1570_2584_doc1_0_col0_intersection0_decode_BB(doc1_0_col0_intersection_ptr_0, doc1_0_col0_bytes_intersection0, doc1_0_intersection0_fragment_size);

	uint32_t* row_doc1_1_intersection1 = idx[3]->index_map[2584];
	uint32_t doc1_1_col0_bytes_intersection1 = idx[3]->index_map[2584+1][0] - row_doc1_1_intersection1[0];
	unsigned char* doc1_1_col0_intersection_ptr_1 = &(idx[3]->fragment_data[0][row_doc1_1_intersection1[0]]);
	uint32_t doc1_1_intersection1_fragment_size = 0;
	q3_mesh_opt_4threads_1570_2584_doc1_1_col0_intersection1_decode_BB(doc1_1_col0_intersection_ptr_1, doc1_1_col0_bytes_intersection1, doc1_1_intersection1_fragment_size);

	uint32_t q3_mesh_opt_4threads_1570_2584_intersection_size = 0;
	q3_mesh_opt_4threads_1570_2584_intersection(doc1_0_intersection0_fragment_size, doc1_1_intersection1_fragment_size, q3_mesh_opt_4threads_1570_2584_intersection_size);

	uint32_t thread_size = q3_mesh_opt_4threads_1570_2584_intersection_size/NUM_THREADS;
	uint32_t position = 0;

	for (int i=0; i<NUM_THREADS; i++) {
		arguments[i].start = position;
		position += thread_size;
		arguments[i].end = position;
		arguments[i].thread_id = i;
	}
	arguments[NUM_THREADS-1].end = q3_mesh_opt_4threads_1570_2584_intersection_size;

	for (int i=0; i<NUM_THREADS; i++) {
		pthread_create(&threads[i], NULL, &pthread_q3_mesh_opt_4threads_1570_2584_worker, (void *) &arguments[i]);
	}

	for (int i=0; i<NUM_THREADS; i++) {
		pthread_join(threads[i], NULL);
	}


	for (int i=0; i<NUM_THREADS; i++) {
		delete[] author1_col0_buffer[i];
	}
	delete[] author1_col0_buffer;
	delete[] doc1_0_col0_intersection_buffer;
	delete[] doc1_1_col0_intersection_buffer;
	delete[] q3_mesh_opt_4threads_1570_2584_intersection_buffer;


	*null_checks = RC;
	return R;

}

#endif

