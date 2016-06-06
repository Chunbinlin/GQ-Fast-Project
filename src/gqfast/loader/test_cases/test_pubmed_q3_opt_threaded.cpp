#ifndef test_pubmed_q3_opt_threaded_
#define test_pubmed_q3_opt_threaded_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 4
#define BUFFER_POOL_SIZE 2

using namespace std;

static args_threading arguments[NUM_THREADS];


static int* R;
static int* RC;

static uint64_t* test_pubmed_q3_opt_threaded_intersection_buffer;

static uint32_t* author1_col0_bits_info;
static uint64_t author1_col0_offset;

extern inline void test_pubmed_q3_opt_threaded_doc1_col0_intersection0_decode_BB(unsigned char* doc1_col0_intersection_ptr_0, uint32_t doc1_col0_bytes_intersection0, uint32_t & doc1_intersection0_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q3_opt_threaded_doc1_col0_intersection1_decode_BB(unsigned char* doc1_col0_intersection_ptr_1, uint32_t doc1_col0_bytes_intersection1, uint32_t & doc1_intersection1_fragment_size) __attribute__((always_inline));

extern inline void test_pubmed_q3_opt_threaded_intersection(uint32_t doc1_intersection0_fragment_size, uint32_t doc1_intersection1_fragment_size, uint32_t & test_pubmed_q3_opt_threaded_intersection_size) __attribute__((always_inline));

void* pthread_test_pubmed_q3_opt_threaded_worker(void* arguments);

extern inline void test_pubmed_q3_opt_threaded_author1_col0_decode_BCA(uint64_t* author1_col0_ptr, uint32_t & author1_col0_element) __attribute__((always_inline));

void test_pubmed_q3_opt_threaded_doc1_col0_intersection0_decode_BB(unsigned char* doc1_col0_intersection_ptr_0, uint32_t doc1_col0_bytes_intersection0, uint32_t & doc1_intersection0_fragment_size) {

	buffer_arrays[3][0][0][0][0] = 0;

	int shiftbits = 0;
	do { 
		doc1_col0_bytes_intersection0--;
		uint32_t next_seven_bits = *doc1_col0_intersection_ptr_0 & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		buffer_arrays[3][0][0][0][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*doc1_col0_intersection_ptr_0++ & 128));
	doc1_intersection0_fragment_size++;

	while (doc1_col0_bytes_intersection0 > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			doc1_col0_bytes_intersection0--;
			uint32_t next_seven_bits = *doc1_col0_intersection_ptr_0 & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*doc1_col0_intersection_ptr_0++ & 128));
		buffer_arrays[3][0][0][0][doc1_intersection0_fragment_size] = buffer_arrays[3][0][0][0][doc1_intersection0_fragment_size-1]+1+result;
		doc1_intersection0_fragment_size++;
	}
}

void test_pubmed_q3_opt_threaded_doc1_col0_intersection1_decode_BB(unsigned char* doc1_col0_intersection_ptr_1, uint32_t doc1_col0_bytes_intersection1, uint32_t & doc1_intersection1_fragment_size) {

	buffer_arrays[3][0][0][1][0] = 0;

	int shiftbits = 0;
	do { 
		doc1_col0_bytes_intersection1--;
		uint32_t next_seven_bits = *doc1_col0_intersection_ptr_1 & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		buffer_arrays[3][0][0][1][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*doc1_col0_intersection_ptr_1++ & 128));
	doc1_intersection1_fragment_size++;

	while (doc1_col0_bytes_intersection1 > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			doc1_col0_bytes_intersection1--;
			uint32_t next_seven_bits = *doc1_col0_intersection_ptr_1 & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*doc1_col0_intersection_ptr_1++ & 128));
		buffer_arrays[3][0][0][1][doc1_intersection1_fragment_size] = buffer_arrays[3][0][0][1][doc1_intersection1_fragment_size-1]+1+result;
		doc1_intersection1_fragment_size++;
	}
}
void test_pubmed_q3_opt_threaded_intersection(uint32_t doc1_intersection0_fragment_size, uint32_t doc1_intersection1_fragment_size, uint32_t & test_pubmed_q3_opt_threaded_intersection_size) { 

	if (doc1_intersection0_fragment_size == 0) { return; }

	if (doc1_intersection1_fragment_size == 0) { return; }

	uint32_t intersection_index = 0;
	uint32_t* its = new uint32_t[2]();
	bool end = false;

	while(!end) {

		bool match = true;
		while (1) {
			if (buffer_arrays[3][0][0][0][its[0]]  != buffer_arrays[3][0][0][1][its[1]]) {
				match = false;
				break;
			}

			break;
		}

		if (match) {
			test_pubmed_q3_opt_threaded_intersection_buffer[intersection_index++] = buffer_arrays[3][0][0][0][its[0]];
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

			uint64_t smallest = buffer_arrays[3][0][0][0][its[0]];
			int index_of_smallest = 0;
			uint32_t fragment_size_of_smallest = doc1_intersection0_fragment_size;

			if (smallest > buffer_arrays[3][0][0][1][its[1]]) {
				smallest = buffer_arrays[3][0][0][1][its[1]];
				index_of_smallest = 1;
				fragment_size_of_smallest = doc1_intersection1_fragment_size;
			}

			if (++its[index_of_smallest] == fragment_size_of_smallest) {
				end = true;
			}
		}
	}

	delete[] its;
	test_pubmed_q3_opt_threaded_intersection_size = intersection_index;
}

void* pthread_test_pubmed_q3_opt_threaded_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t test_pubmed_q3_opt_threaded_intersection_it = args->start;
	uint32_t test_pubmed_q3_opt_threaded_intersection_size = args->end;
	int thread_id = args->thread_id;

	for (; test_pubmed_q3_opt_threaded_intersection_it<test_pubmed_q3_opt_threaded_intersection_size; test_pubmed_q3_opt_threaded_intersection_it++) {

		uint32_t* row_author1 = idx[4]->index_map[test_pubmed_q3_opt_threaded_intersection_buffer[test_pubmed_q3_opt_threaded_intersection_it]];

		uint64_t* author1_col0_ptr = reinterpret_cast<uint64_t *>(&(idx[4]->fragment_data[0][row_author1[0]]));
		uint32_t author1_col0_element;
		test_pubmed_q3_opt_threaded_author1_col0_decode_BCA(author1_col0_ptr, author1_col0_element);

		RC[author1_col0_element] = 1;

		pthread_spin_lock(&spin_locks[4][author1_col0_element]);
		R[author1_col0_element] += 1;
		pthread_spin_unlock(&spin_locks[4][author1_col0_element]);

	}
	return nullptr;
}

void test_pubmed_q3_opt_threaded_author1_col0_decode_BCA(uint64_t* author1_col0_ptr, uint32_t & author1_col0_element) {

	author1_col0_element = author1_col0_bits_info[1];
	author1_col0_element &= *author1_col0_ptr;
	author1_col0_element += author1_col0_offset;
}

extern "C" int* test_pubmed_q3_opt_threaded(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[3];
	for(int i=0; i<metadata.idx_num_encodings[3]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[3][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[3][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[4];
	for(int i=0; i<metadata.idx_num_encodings[4]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[4][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[4][i][j][k] = new int[max_frag];
			}
		}
	}

	RC = new int[metadata.idx_domains[4][0]]();
	R = new int[metadata.idx_domains[4][0]]();


	test_pubmed_q3_opt_threaded_intersection_buffer = new uint64_t[metadata.idx_max_fragment_sizes[3]];

	author1_col0_bits_info = idx[4]->dict[0]->bits_info;
	author1_col0_offset = idx[4]->dict[0]->offset;

	uint32_t* row_doc1_intersection0 = idx[3]->index_map[1];
	uint32_t doc1_col0_bytes_intersection0 = idx[3]->index_map[1+1][0] - row_doc1_intersection0[0];
	unsigned char* doc1_col0_intersection_ptr_0 = &(idx[3]->fragment_data[0][row_doc1_intersection0[0]]);
	uint32_t doc1_intersection0_fragment_size = 0;
	test_pubmed_q3_opt_threaded_doc1_col0_intersection0_decode_BB(doc1_col0_intersection_ptr_0, doc1_col0_bytes_intersection0, doc1_intersection0_fragment_size);

	uint32_t* row_doc1_intersection1 = idx[3]->index_map[2];
	uint32_t doc1_col0_bytes_intersection1 = idx[3]->index_map[2+1][0] - row_doc1_intersection1[0];
	unsigned char* doc1_col0_intersection_ptr_1 = &(idx[3]->fragment_data[0][row_doc1_intersection1[0]]);
	uint32_t doc1_intersection1_fragment_size = 0;
	test_pubmed_q3_opt_threaded_doc1_col0_intersection1_decode_BB(doc1_col0_intersection_ptr_1, doc1_col0_bytes_intersection1, doc1_intersection1_fragment_size);

	uint32_t test_pubmed_q3_opt_threaded_intersection_size = 0;
	test_pubmed_q3_opt_threaded_intersection(doc1_intersection0_fragment_size, doc1_intersection1_fragment_size, test_pubmed_q3_opt_threaded_intersection_size);

	uint32_t thread_size = test_pubmed_q3_opt_threaded_intersection_size/NUM_THREADS;
	uint32_t position = 0;

	for (int i=0; i<NUM_THREADS; i++) {
		arguments[i].start = position;
		position += thread_size;
		arguments[i].end = position;
		arguments[i].thread_id = i;
	}
	arguments[NUM_THREADS-1].end = test_pubmed_q3_opt_threadedintersection_size;

	for (int i=0; i<NUM_THREADS; i++) {
		pthread_create(&threads[i], NULL, &pthread_test_pubmed_q3_opt_threaded_worker, (void *) &arguments[i]);
	}

	for (int i=0; i<NUM_THREADS; i++) {
		pthread_join(threads[i], NULL);
	}


	for (int j=0; j<metadata.idx_num_encodings[3]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[3][j][k][l];
			}
			delete[] buffer_arrays[3][j][k];
		}
	}
	for (int j=0; j<metadata.idx_num_encodings[4]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[4][j][k][l];
			}
			delete[] buffer_arrays[4][j][k];
		}
	}
	delete[] test_pubmed_q3_opt_threaded_intersection_buffer;


	*null_checks = RC;
	return R;

}

#endif

