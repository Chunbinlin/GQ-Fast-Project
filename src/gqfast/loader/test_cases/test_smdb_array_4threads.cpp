#ifndef test_smdb_array_4threads_
#define test_smdb_array_4threads_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 4
#define NUM_BUFFERS 6
#define BUFFER_POOL_SIZE 1

using namespace std;

static args_threading arguments[NUM_THREADS];

static int* R;
static int* RC;

static bool* sentence1_bool_array;

extern inline void test_smdb_array_4threads_concept_semtype1_col0_decode_UA(uint32_t* concept_semtype1_col0_ptr, uint32_t concept_semtype1_col0_bytes, uint32_t & concept_semtype1_fragment_size) __attribute__((always_inline));

void* pthread_test_smdb_array_4threads_worker(void* arguments);

extern inline void test_smdb_array_4threads_predication1_col0_decode_UA_threaded(int thread_id, uint32_t* predication1_col0_ptr, uint32_t predication1_col0_bytes, uint32_t & predication1_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_array_4threads_sentence1_col0_decode_UA_threaded(int thread_id, uint32_t* sentence1_col0_ptr, uint32_t sentence1_col0_bytes, uint32_t & sentence1_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_array_4threads_predication2_col0_decode_UA_threaded(int thread_id, uint32_t* predication2_col0_ptr, uint32_t predication2_col0_bytes, uint32_t & predication2_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_array_4threads_concept_semtype2_col0_decode_UA_threaded(int thread_id, uint32_t* concept_semtype2_col0_ptr, uint32_t concept_semtype2_col0_bytes, uint32_t & concept_semtype2_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_array_4threads_concept2_col0_decode_UA_threaded(int thread_id, uint32_t* concept2_col0_ptr, uint32_t concept2_col0_bytes, uint32_t & concept2_fragment_size) __attribute__((always_inline));

void test_smdb_array_4threads_concept_semtype1_col0_decode_UA(uint32_t* concept_semtype1_col0_ptr, uint32_t concept_semtype1_col0_bytes, uint32_t & concept_semtype1_fragment_size) {

	concept_semtype1_fragment_size = concept_semtype1_col0_bytes/4;

	for (uint32_t i=0; i<concept_semtype1_fragment_size; i++) {
		buffer_arrays[0][0][0][0][i] = *concept_semtype1_col0_ptr++;
	}
}

void* pthread_test_smdb_array_4threads_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t concept_semtype1_it = args->start;
	uint32_t concept_semtype1_fragment_size = args->end;
	int thread_id = args->thread_id;

	for (; concept_semtype1_it < concept_semtype1_fragment_size; concept_semtype1_it++) {

		uint32_t concept_semtype1_col0_element = buffer_arrays[0][0][0][0][concept_semtype1_it];

		uint32_t* row_op3 = idx[1]->index_map[concept_semtype1_col0_element];
		uint32_t predication1_col0_bytes = idx[1]->index_map[concept_semtype1_col0_element+1][0] - row_op3[0];
		if(predication1_col0_bytes) {

			uint32_t* predication1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[1]->fragment_data[0][row_op3[0]]));
			uint32_t predication1_fragment_size = 0;
			test_smdb_array_4threads_predication1_col0_decode_UA_threaded(thread_id, predication1_col0_ptr, predication1_col0_bytes, predication1_fragment_size);

			for (uint32_t predication1_it = 0; predication1_it < predication1_fragment_size; predication1_it++) {

				uint32_t predication1_col0_element = buffer_arrays[1][0][thread_id][0][predication1_it];

				uint32_t* row_op4 = idx[2]->index_map[predication1_col0_element];
				uint32_t sentence1_col0_bytes = idx[2]->index_map[predication1_col0_element+1][0] - row_op4[0];
				if(sentence1_col0_bytes) {

					uint32_t* sentence1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[2]->fragment_data[0][row_op4[0]]));
					uint32_t sentence1_fragment_size = 0;
					test_smdb_array_4threads_sentence1_col0_decode_UA_threaded(thread_id, sentence1_col0_ptr, sentence1_col0_bytes, sentence1_fragment_size);

					for (uint32_t sentence1_it = 0; sentence1_it < sentence1_fragment_size; sentence1_it++) {


						if (!(sentence1_bool_array[buffer_arrays[2][0][thread_id][0][sentence1_it]])) {
							sentence1_bool_array[buffer_arrays[2][0][thread_id][0][sentence1_it]] = true;
							uint32_t sentence1_col0_element = buffer_arrays[2][0][thread_id][0][sentence1_it];

							uint32_t* row_op5 = idx[3]->index_map[sentence1_col0_element];
							uint32_t predication2_col0_bytes = idx[3]->index_map[sentence1_col0_element+1][0] - row_op5[0];
							if(predication2_col0_bytes) {

								uint32_t* predication2_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[3]->fragment_data[0][row_op5[0]]));
								uint32_t predication2_fragment_size = 0;
								test_smdb_array_4threads_predication2_col0_decode_UA_threaded(thread_id, predication2_col0_ptr, predication2_col0_bytes, predication2_fragment_size);

								for (uint32_t predication2_it = 0; predication2_it < predication2_fragment_size; predication2_it++) {

									uint32_t predication2_col0_element = buffer_arrays[3][0][thread_id][0][predication2_it];

									uint32_t* row_op6 = idx[4]->index_map[predication2_col0_element];
									uint32_t concept_semtype2_col0_bytes = idx[4]->index_map[predication2_col0_element+1][0] - row_op6[0];
									if(concept_semtype2_col0_bytes) {

										uint32_t* concept_semtype2_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[4]->fragment_data[0][row_op6[0]]));
										uint32_t concept_semtype2_fragment_size = 0;
										test_smdb_array_4threads_concept_semtype2_col0_decode_UA_threaded(thread_id, concept_semtype2_col0_ptr, concept_semtype2_col0_bytes, concept_semtype2_fragment_size);

										for (uint32_t concept_semtype2_it = 0; concept_semtype2_it < concept_semtype2_fragment_size; concept_semtype2_it++) {

											uint32_t concept_semtype2_col0_element = buffer_arrays[4][0][thread_id][0][concept_semtype2_it];

											uint32_t* row_op7 = idx[5]->index_map[concept_semtype2_col0_element];
											uint32_t concept2_col0_bytes = idx[5]->index_map[concept_semtype2_col0_element+1][0] - row_op7[0];
											if(concept2_col0_bytes) {

												uint32_t* concept2_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[5]->fragment_data[0][row_op7[0]]));
												uint32_t concept2_fragment_size = 0;
												test_smdb_array_4threads_concept2_col0_decode_UA_threaded(thread_id, concept2_col0_ptr, concept2_col0_bytes, concept2_fragment_size);

												for (uint32_t concept2_it = 0; concept2_it < concept2_fragment_size; concept2_it++) {
													uint32_t concept2_col0_element = buffer_arrays[5][0][thread_id][0][concept2_it];

													RC[concept2_col0_element] = 1;

													pthread_spin_lock(&spin_locks[5][concept2_col0_element]);
													R[concept2_col0_element] += 1;
													pthread_spin_unlock(&spin_locks[5][concept2_col0_element]);

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

void test_smdb_array_4threads_predication1_col0_decode_UA_threaded(int thread_id, uint32_t* predication1_col0_ptr, uint32_t predication1_col0_bytes, uint32_t & predication1_fragment_size) {

	predication1_fragment_size = predication1_col0_bytes/4;

	for (uint32_t i=0; i<predication1_fragment_size; i++) {
		buffer_arrays[1][0][thread_id][0][i] = *predication1_col0_ptr++;
	}
}

void test_smdb_array_4threads_sentence1_col0_decode_UA_threaded(int thread_id, uint32_t* sentence1_col0_ptr, uint32_t sentence1_col0_bytes, uint32_t & sentence1_fragment_size) {

	sentence1_fragment_size = sentence1_col0_bytes/4;

	for (uint32_t i=0; i<sentence1_fragment_size; i++) {
		buffer_arrays[2][0][thread_id][0][i] = *sentence1_col0_ptr++;
	}
}

void test_smdb_array_4threads_predication2_col0_decode_UA_threaded(int thread_id, uint32_t* predication2_col0_ptr, uint32_t predication2_col0_bytes, uint32_t & predication2_fragment_size) {

	predication2_fragment_size = predication2_col0_bytes/4;

	for (uint32_t i=0; i<predication2_fragment_size; i++) {
		buffer_arrays[3][0][thread_id][0][i] = *predication2_col0_ptr++;
	}
}

void test_smdb_array_4threads_concept_semtype2_col0_decode_UA_threaded(int thread_id, uint32_t* concept_semtype2_col0_ptr, uint32_t concept_semtype2_col0_bytes, uint32_t & concept_semtype2_fragment_size) {

	concept_semtype2_fragment_size = concept_semtype2_col0_bytes/4;

	for (uint32_t i=0; i<concept_semtype2_fragment_size; i++) {
		buffer_arrays[4][0][thread_id][0][i] = *concept_semtype2_col0_ptr++;
	}
}

void test_smdb_array_4threads_concept2_col0_decode_UA_threaded(int thread_id, uint32_t* concept2_col0_ptr, uint32_t concept2_col0_bytes, uint32_t & concept2_fragment_size) {

	concept2_fragment_size = concept2_col0_bytes/4;

	for (uint32_t i=0; i<concept2_fragment_size; i++) {
		buffer_arrays[5][0][thread_id][0][i] = *concept2_col0_ptr++;
	}
}

extern "C" int* test_smdb_array_4threads(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[0];
	for(int i=0; i<metadata.idx_num_encodings[0]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[0][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[0][i][j][k] = new int[max_frag];
			}
		}
	}

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

	max_frag = metadata.idx_max_fragment_sizes[4];
	for(int i=0; i<metadata.idx_num_encodings[4]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[4][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[4][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[5];
	for(int i=0; i<metadata.idx_num_encodings[5]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[5][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[5][i][j][k] = new int[max_frag];
			}
		}
	}

	RC = new int[metadata.idx_domains[5][0]]();
	R = new int[metadata.idx_domains[5][0]]();

	sentence1_bool_array = new bool[metadata.idx_domains[2][0]]();

	uint64_t* concept1_list = new uint64_t[1];
	concept1_list[0] = 2019;

	for (int concept1_it = 0; concept1_it<1; concept1_it++) {

		uint64_t concept1_col0_element = concept1_list[concept1_it];

		uint32_t* row_op1 = idx[0]->index_map[concept1_col0_element];
		uint32_t concept_semtype1_col0_bytes = idx[0]->index_map[concept1_col0_element+1][0] - row_op1[0];
		if(concept_semtype1_col0_bytes) {

			uint32_t* concept_semtype1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[0]->fragment_data[0][row_op1[0]]));
			uint32_t concept_semtype1_fragment_size = 0;
			test_smdb_array_4threads_concept_semtype1_col0_decode_UA(concept_semtype1_col0_ptr, concept_semtype1_col0_bytes, concept_semtype1_fragment_size);

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
				pthread_create(&threads[i], NULL, &pthread_test_smdb_array_4threads_worker, (void *) &arguments[i]);
			}

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_join(threads[i], NULL);
			}
		}
	}


	for (int j=0; j<metadata.idx_num_encodings[0]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[0][j][k][l];
			}
			delete[] buffer_arrays[0][j][k];
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
	for (int j=0; j<metadata.idx_num_encodings[4]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[4][j][k][l];
			}
			delete[] buffer_arrays[4][j][k];
		}
	}
	for (int j=0; j<metadata.idx_num_encodings[5]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[5][j][k][l];
			}
			delete[] buffer_arrays[5][j][k];
		}
	}

	delete[] sentence1_bool_array;


	*null_checks = RC;
	return R;

}

#endif

