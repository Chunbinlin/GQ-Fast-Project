#ifndef smdb_optimal_4threads_
#define smdb_optimal_4threads_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 4

using namespace std;

static args_threading arguments[NUM_THREADS];


static int* R;
static int* RC;

static pthread_spinlock_t* r_spin_locks;

static uint64_t** concept_semtype1_col0_buffer;
static uint64_t** predication1_col0_buffer;
static uint64_t** sentence1_col0_buffer;
static uint64_t** predication2_col0_buffer;
static uint64_t** concept_semtype2_col0_buffer;
static uint64_t** concept2_col0_buffer;

static bool* sentence1_bool_array;

static int* concept_semtype2_col0_huffman_tree_array;
static bool* concept_semtype2_col0_huffman_terminator_array;

extern inline void smdb_optimal_4threads_concept_semtype1_col0_decode_BB(unsigned char* concept_semtype1_col0_ptr, uint32_t concept_semtype1_col0_bytes, uint32_t & concept_semtype1_fragment_size) __attribute__((always_inline));

void* pthread_smdb_optimal_4threads_worker(void* arguments);

extern inline void smdb_optimal_4threads_predication1_col0_decode_BB_threaded(int thread_id, unsigned char* predication1_col0_ptr, uint32_t predication1_col0_bytes, uint32_t & predication1_fragment_size) __attribute__((always_inline));

extern inline void smdb_optimal_4threads_sentence1_col0_decode_BB_threaded(int thread_id, unsigned char* sentence1_col0_ptr, uint32_t sentence1_col0_bytes, uint32_t & sentence1_fragment_size) __attribute__((always_inline));

extern inline void smdb_optimal_4threads_predication2_col0_decode_BB_threaded(int thread_id, unsigned char* predication2_col0_ptr, uint32_t predication2_col0_bytes, uint32_t & predication2_fragment_size) __attribute__((always_inline));

extern inline void smdb_optimal_4threads_concept_semtype2_col0_decode_Huffman_threaded(int thread_id, unsigned char* concept_semtype2_col0_ptr, uint32_t concept_semtype2_col0_bytes, uint32_t & concept_semtype2_fragment_size) __attribute__((always_inline));

extern inline void smdb_optimal_4threads_concept2_col0_decode_BB_threaded(int thread_id, unsigned char* concept2_col0_ptr, uint32_t concept2_col0_bytes, uint32_t & concept2_fragment_size) __attribute__((always_inline));

void smdb_optimal_4threads_concept_semtype1_col0_decode_BB(unsigned char* concept_semtype1_col0_ptr, uint32_t concept_semtype1_col0_bytes, uint32_t & concept_semtype1_fragment_size) {

	concept_semtype1_col0_buffer[0][0] = 0;

	int shiftbits = 0;
	do { 
		concept_semtype1_col0_bytes--;
		uint32_t next_seven_bits = *concept_semtype1_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		concept_semtype1_col0_buffer[0][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*concept_semtype1_col0_ptr++ & 128));
	concept_semtype1_fragment_size++;

	while (concept_semtype1_col0_bytes > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			concept_semtype1_col0_bytes--;
			uint32_t next_seven_bits = *concept_semtype1_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*concept_semtype1_col0_ptr++ & 128));
		concept_semtype1_col0_buffer[0][concept_semtype1_fragment_size] = concept_semtype1_col0_buffer[0][concept_semtype1_fragment_size-1]+1+result;
		concept_semtype1_fragment_size++;
	}
}

void* pthread_smdb_optimal_4threads_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t concept_semtype1_it = args->start;
	uint32_t concept_semtype1_fragment_size = args->end;
	int thread_id = args->thread_id;

	for (; concept_semtype1_it < concept_semtype1_fragment_size; concept_semtype1_it++) {

		uint32_t concept_semtype1_col0_element = concept_semtype1_col0_buffer[0][concept_semtype1_it];

		uint32_t* row_predication1 = idx[1]->index_map[concept_semtype1_col0_element];
		uint32_t predication1_col0_bytes = idx[1]->index_map[concept_semtype1_col0_element+1][0] - row_predication1[0];
		if(predication1_col0_bytes) {

			unsigned char* predication1_col0_ptr = &(idx[1]->fragment_data[0][row_predication1[0]]);
			uint32_t predication1_fragment_size = 0;
			smdb_optimal_4threads_predication1_col0_decode_BB_threaded(thread_id, predication1_col0_ptr, predication1_col0_bytes, predication1_fragment_size);

			for (uint32_t predication1_it = 0; predication1_it < predication1_fragment_size; predication1_it++) {

				uint32_t predication1_col0_element = predication1_col0_buffer[thread_id][predication1_it];

				uint32_t* row_sentence1 = idx[2]->index_map[predication1_col0_element];
				uint32_t sentence1_col0_bytes = idx[2]->index_map[predication1_col0_element+1][0] - row_sentence1[0];
				if(sentence1_col0_bytes) {

					unsigned char* sentence1_col0_ptr = &(idx[2]->fragment_data[0][row_sentence1[0]]);
					uint32_t sentence1_fragment_size = 0;
					smdb_optimal_4threads_sentence1_col0_decode_BB_threaded(thread_id, sentence1_col0_ptr, sentence1_col0_bytes, sentence1_fragment_size);

					for (uint32_t sentence1_it = 0; sentence1_it < sentence1_fragment_size; sentence1_it++) {


						if (!(sentence1_bool_array[sentence1_col0_buffer[thread_id][sentence1_it]])) {
							sentence1_bool_array[sentence1_col0_buffer[thread_id][sentence1_it]] = true;
							uint32_t sentence1_col0_element = sentence1_col0_buffer[thread_id][sentence1_it];

							uint32_t* row_predication2 = idx[3]->index_map[sentence1_col0_element];
							uint32_t predication2_col0_bytes = idx[3]->index_map[sentence1_col0_element+1][0] - row_predication2[0];
							if(predication2_col0_bytes) {

								unsigned char* predication2_col0_ptr = &(idx[3]->fragment_data[0][row_predication2[0]]);
								uint32_t predication2_fragment_size = 0;
								smdb_optimal_4threads_predication2_col0_decode_BB_threaded(thread_id, predication2_col0_ptr, predication2_col0_bytes, predication2_fragment_size);

								for (uint32_t predication2_it = 0; predication2_it < predication2_fragment_size; predication2_it++) {

									uint32_t predication2_col0_element = predication2_col0_buffer[thread_id][predication2_it];

									uint32_t* row_concept_semtype2 = idx[4]->index_map[predication2_col0_element];
									uint32_t concept_semtype2_col0_bytes = idx[4]->index_map[predication2_col0_element+1][0] - row_concept_semtype2[0];
									if(concept_semtype2_col0_bytes) {

										unsigned char* concept_semtype2_col0_ptr = &(idx[4]->fragment_data[0][row_concept_semtype2[0]]);
										uint32_t concept_semtype2_fragment_size = 0;
										smdb_optimal_4threads_concept_semtype2_col0_decode_Huffman_threaded(thread_id, concept_semtype2_col0_ptr, concept_semtype2_col0_bytes, concept_semtype2_fragment_size);

										for (uint32_t concept_semtype2_it = 0; concept_semtype2_it < concept_semtype2_fragment_size; concept_semtype2_it++) {

											uint32_t concept_semtype2_col0_element = concept_semtype2_col0_buffer[thread_id][concept_semtype2_it];

											uint32_t* row_concept2 = idx[5]->index_map[concept_semtype2_col0_element];
											uint32_t concept2_col0_bytes = idx[5]->index_map[concept_semtype2_col0_element+1][0] - row_concept2[0];
											if(concept2_col0_bytes) {

												unsigned char* concept2_col0_ptr = &(idx[5]->fragment_data[0][row_concept2[0]]);
												uint32_t concept2_fragment_size = 0;
												smdb_optimal_4threads_concept2_col0_decode_BB_threaded(thread_id, concept2_col0_ptr, concept2_col0_bytes, concept2_fragment_size);

												for (uint32_t concept2_it = 0; concept2_it < concept2_fragment_size; concept2_it++) {
													uint32_t concept2_col0_element = concept2_col0_buffer[thread_id][concept2_it];

													RC[concept2_col0_element] = 1;

													pthread_spin_lock(&r_spin_locks[concept2_col0_element]);
													R[concept2_col0_element] += 1;
													pthread_spin_unlock(&r_spin_locks[concept2_col0_element]);

												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	return nullptr;
}

void smdb_optimal_4threads_predication1_col0_decode_BB_threaded(int thread_id, unsigned char* predication1_col0_ptr, uint32_t predication1_col0_bytes, uint32_t & predication1_fragment_size) {

	predication1_col0_buffer[thread_id][0] = 0;

	int shiftbits = 0;
	do { 
		predication1_col0_bytes--;
		uint32_t next_seven_bits = *predication1_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		predication1_col0_buffer[thread_id][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*predication1_col0_ptr++ & 128));
	predication1_fragment_size++;

	while (predication1_col0_bytes > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			predication1_col0_bytes--;
			uint32_t next_seven_bits = *predication1_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*predication1_col0_ptr++ & 128));
		predication1_col0_buffer[thread_id][predication1_fragment_size] = predication1_col0_buffer[thread_id][predication1_fragment_size-1]+1+result;
		predication1_fragment_size++;
	}
}

void smdb_optimal_4threads_sentence1_col0_decode_BB_threaded(int thread_id, unsigned char* sentence1_col0_ptr, uint32_t sentence1_col0_bytes, uint32_t & sentence1_fragment_size) {

	sentence1_col0_buffer[thread_id][0] = 0;

	int shiftbits = 0;
	do { 
		sentence1_col0_bytes--;
		uint32_t next_seven_bits = *sentence1_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		sentence1_col0_buffer[thread_id][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*sentence1_col0_ptr++ & 128));
	sentence1_fragment_size++;

	while (sentence1_col0_bytes > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			sentence1_col0_bytes--;
			uint32_t next_seven_bits = *sentence1_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*sentence1_col0_ptr++ & 128));
		sentence1_col0_buffer[thread_id][sentence1_fragment_size] = sentence1_col0_buffer[thread_id][sentence1_fragment_size-1]+1+result;
		sentence1_fragment_size++;
	}
}

void smdb_optimal_4threads_predication2_col0_decode_BB_threaded(int thread_id, unsigned char* predication2_col0_ptr, uint32_t predication2_col0_bytes, uint32_t & predication2_fragment_size) {

	predication2_col0_buffer[thread_id][0] = 0;

	int shiftbits = 0;
	do { 
		predication2_col0_bytes--;
		uint32_t next_seven_bits = *predication2_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		predication2_col0_buffer[thread_id][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*predication2_col0_ptr++ & 128));
	predication2_fragment_size++;

	while (predication2_col0_bytes > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			predication2_col0_bytes--;
			uint32_t next_seven_bits = *predication2_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*predication2_col0_ptr++ & 128));
		predication2_col0_buffer[thread_id][predication2_fragment_size] = predication2_col0_buffer[thread_id][predication2_fragment_size-1]+1+result;
		predication2_fragment_size++;
	}
}

void smdb_optimal_4threads_concept_semtype2_col0_decode_Huffman_threaded(int thread_id, unsigned char* concept_semtype2_col0_ptr, uint32_t concept_semtype2_col0_bytes, uint32_t & concept_semtype2_fragment_size) {

	bool* terminate_start = &(concept_semtype2_col0_huffman_terminator_array[0]);
	int* tree_array_start = &(concept_semtype2_col0_huffman_tree_array[0]);

	int mask = 0x100;

	while (concept_semtype2_col0_bytes > 1) {

		bool* terminator_array = terminate_start;
		int* tree_array = tree_array_start;

		while(!*terminator_array) { 

			char direction = *concept_semtype2_col0_ptr & (mask >>= 1);

			if (mask == 1) { 
				mask = 0x100;
				concept_semtype2_col0_ptr++;
				concept_semtype2_col0_bytes--;
			}

			terminator_array += *tree_array;
			tree_array += *tree_array;

			if (direction) {
				terminator_array++;
				tree_array++;
			}
		}

		concept_semtype2_col0_buffer[thread_id][concept_semtype2_fragment_size++] = *tree_array;
	}

	if (mask != 0x100) {
		int bit_pos = mask;
		unsigned char last_byte = *concept_semtype2_col0_ptr;
		while (bit_pos > 1) {
			unsigned char bit = last_byte & (bit_pos >>= 1);
			if (bit) {
				bool* terminator_array = terminate_start;
				int* tree_array = tree_array_start;

				while (!*terminator_array) {
					char direction = *concept_semtype2_col0_ptr & (mask >>= 1);

					terminator_array += *tree_array;
					tree_array += *tree_array;

					if (direction) {
						terminator_array++;
						tree_array++;
					}
				}

				concept_semtype2_col0_buffer[thread_id][concept_semtype2_fragment_size++] = *tree_array;
				bit_pos = mask;
			}
		}
	}
}

void smdb_optimal_4threads_concept2_col0_decode_BB_threaded(int thread_id, unsigned char* concept2_col0_ptr, uint32_t concept2_col0_bytes, uint32_t & concept2_fragment_size) {

	concept2_col0_buffer[thread_id][0] = 0;

	int shiftbits = 0;
	do { 
		concept2_col0_bytes--;
		uint32_t next_seven_bits = *concept2_col0_ptr & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		concept2_col0_buffer[thread_id][0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(*concept2_col0_ptr++ & 128));
	concept2_fragment_size++;

	while (concept2_col0_bytes > 0) {
		shiftbits = 0;
		uint32_t result = 0;

		do {

			concept2_col0_bytes--;
			uint32_t next_seven_bits = *concept2_col0_ptr & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(*concept2_col0_ptr++ & 128));
		concept2_col0_buffer[thread_id][concept2_fragment_size] = concept2_col0_buffer[thread_id][concept2_fragment_size-1]+1+result;
		concept2_fragment_size++;
	}
}

extern "C" int* smdb_optimal_4threads(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[0];
	concept_semtype1_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		concept_semtype1_col0_buffer[i] = new uint64_t[max_frag];
	}

	max_frag = metadata.idx_max_fragment_sizes[1];
	predication1_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		predication1_col0_buffer[i] = new uint64_t[max_frag];
	}

	max_frag = metadata.idx_max_fragment_sizes[2];
	sentence1_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		sentence1_col0_buffer[i] = new uint64_t[max_frag];
	}

	max_frag = metadata.idx_max_fragment_sizes[3];
	predication2_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		predication2_col0_buffer[i] = new uint64_t[max_frag];
	}

	max_frag = metadata.idx_max_fragment_sizes[4];
	concept_semtype2_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		concept_semtype2_col0_buffer[i] = new uint64_t[max_frag];
	}

	max_frag = metadata.idx_max_fragment_sizes[5];
	concept2_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		concept2_col0_buffer[i] = new uint64_t[max_frag];
	}

	RC = new int[metadata.idx_domains[5][0]]();
	R = new int[metadata.idx_domains[5][0]]();

	r_spin_locks = spin_locks[5];

	sentence1_bool_array = new bool[metadata.idx_domains[2][0]]();

	concept_semtype2_col0_huffman_tree_array = idx[4]->huffman_tree_array[0];
	concept_semtype2_col0_huffman_terminator_array = idx[4]->huffman_terminator_array[0];

	uint64_t concept1_list[1];
	concept1_list[0] = 2019;

	for (int concept1_it = 0; concept1_it<1; concept1_it++) {

		uint64_t concept1_col0_element = concept1_list[concept1_it];

		uint32_t* row_concept_semtype1 = idx[0]->index_map[concept1_col0_element];
		uint32_t concept_semtype1_col0_bytes = idx[0]->index_map[concept1_col0_element+1][0] - row_concept_semtype1[0];
		if(concept_semtype1_col0_bytes) {

			unsigned char* concept_semtype1_col0_ptr = &(idx[0]->fragment_data[0][row_concept_semtype1[0]]);
			uint32_t concept_semtype1_fragment_size = 0;
			smdb_optimal_4threads_concept_semtype1_col0_decode_BB(concept_semtype1_col0_ptr, concept_semtype1_col0_bytes, concept_semtype1_fragment_size);

			uint32_t thread_size = concept_semtype1_fragment_size/NUM_THREADS;
			uint32_t position = 0;

			for (int i=0; i<NUM_THREADS; i++) {
				arguments[i].start = position;
				position += thread_size;
				arguments[i].end = position;
				arguments[i].thread_id = i;
			}
			arguments[NUM_THREADS-1].end = concept_semtype1_fragment_size;

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_create(&threads[i], NULL, &pthread_smdb_optimal_4threads_worker, (void *) &arguments[i]);
			}

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_join(threads[i], NULL);
			}
		}
	}


	for (int i=0; i<NUM_THREADS; i++) {
		delete[] concept_semtype1_col0_buffer[i];
	}
	delete[] concept_semtype1_col0_buffer;
	for (int i=0; i<NUM_THREADS; i++) {
		delete[] predication1_col0_buffer[i];
	}
	delete[] predication1_col0_buffer;
	for (int i=0; i<NUM_THREADS; i++) {
		delete[] sentence1_col0_buffer[i];
	}
	delete[] sentence1_col0_buffer;
	for (int i=0; i<NUM_THREADS; i++) {
		delete[] predication2_col0_buffer[i];
	}
	delete[] predication2_col0_buffer;
	for (int i=0; i<NUM_THREADS; i++) {
		delete[] concept_semtype2_col0_buffer[i];
	}
	delete[] concept_semtype2_col0_buffer;
	for (int i=0; i<NUM_THREADS; i++) {
		delete[] concept2_col0_buffer[i];
	}
	delete[] concept2_col0_buffer;

	delete[] sentence1_bool_array;


	*null_checks = RC;
	return R;

}

#endif

