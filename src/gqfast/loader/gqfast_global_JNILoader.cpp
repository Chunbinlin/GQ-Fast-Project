#ifndef gqfast_global_jniloadindex_
#define gqfast_global_jniloadindex_

#include <jni.h>
#include <stdio.h>
#include "../../gqfast_global_JNILoader.h"
#include "load_index.hpp"
#include "run_query.hpp"
#include "global_vars.hpp"

// Clock variables
struct timespec start;
struct timespec finish;

chrono::steady_clock::time_point benchmark_t1;
chrono::steady_clock::time_point benchmark_t2;

// Pre-declared index pointers
fastr_index<uint32_t>* idx[MAX_INDICES];

// Metadata wrapper
Metadata metadata;

// Thread defs
pthread_t threads[MAX_THREADS];
pthread_spinlock_t * spin_locks[MAX_INDICES];


// Buffers
uint64_t**** buffer_arrays[MAX_INDICES];


void init_globals()
{

    // Globals are initially null/0
    for (int i=0; i<MAX_INDICES; i++)
    {
        idx[i] = nullptr;
        metadata.idx_map_byte_sizes[i] = 0;
        metadata.idx_max_fragment_sizes[i] = 0;
        metadata.idx_num_encodings[i] = 0;

        buffer_arrays[i] = nullptr;
        idx_position_in_use[i] = false;
    }


}

void delete_globals()
{
    cerr << "Deleting globals\n";
    for (int i=0; i<MAX_INDICES; i++)
    {

        if (idx[i])
        {

            int num_encodings = idx[i]->num_fragment_data;
            // Free the associated buffer
            for (int j=0; j<num_encodings; j++)
            {
                delete[] buffer_arrays[i][j];
            }

            delete[] buffer_arrays[i];
            delete idx[i];
            delete[] spin_locks[i];
        }
    }
}



JNIEXPORT void JNICALL Java_gqfast_global_JNILoadIndex_cpp_1open_1loader
  (JNIEnv *env, jobject thisObj)
{
    init_globals();
}


JNIEXPORT void JNICALL Java_gqfast_global_JNILoadIndex_cpp_1close_1loader
  (JNIEnv *env, jobject thisObj);
{
    delete_globals();
}

JNIEXPORT void JNICALL Java_gqfast_global_JNILoadIndex_cpp_1load_1index
  (JNIEnv *env, jobject thisObj, jstring pathName, jint numEncs, jintArray colEncs)
{
    // First parameter is string
    const char *cPathName = (*env)->GetStringUTFChars(env, pathName, NULL);
    if (NULL == cPathName) return NULL;
    string pathName(cPathName);
    (*env)->ReleaseStringUTFChars(env, pathName, cPathName);  // release resources
    cerr << "Path set to " << pathName << "\n";

    // Second parameter
    int numEncodings = (int) numEncs;
    cerr << "Num encodings set to " << numEncodings << "\n";


    // Third parameter is an array
    jint *cColEncs = (*env)->GetIntArrayElements(env, colEncs, NULL);
    if (NULL == cColEncs) return NULL;
    jsize length = (*env)->GetArrayLength(env, colEncs);
    if (length != numEncodings)
    {
        cerr << "Error: unexpected derived length of colEnc array\n";
        return NULL;
    }

    Encodings encodingsArray[numEncodings];
    for (int i=0; i<numEncodings; i++) {
        string tempString = "col" + i;
        Encodings temp = new Encodings(tempString, cColEncs[i]);
        encodingsArray[i] = temp;
    }

    (*env)->ReleaseIntArrayElements(env, colEncs, cColEncs, 0); // release resources

    // Calls function in load_index.hpp to build index
    int newIndexID = load(pathName, encodingsArray, numEncodings);
    if (newIndexID < 0)
    {
        cerr << "index id not generated properly.\n";
        return;
    }


    // Update Java vars
    jclass thisClass = (*env)->GetObjectClass(env, thisObj);

    jfieldID fidIndexID = (*env)->GetFieldID(env, thisClass, "loaderIndexID", "I");
    if (NULL == fidIndexID) return;

    jint jIndexID = (*env)->GetIntField(env, thisObj, fidIndexID);
    jIndexID = (jint) newIndexID;
    (*env)->SetIntField(env, thisObj, fidIndexID, jIndexID);


    jfieldID fidIndexDomain = (*env)->GetFieldID(env, thisClass, "discoveredIndexDomain", "J");
    if (NULL == fidIndexDomain) return;

    jlong jIndexDomain = (*env)->GetLongField(env, thisObj, fidIndexDomain);
    jIndexDomain = (jlong) idx[newIndexID]->domain_size;
    (*env)->SetLongField(env, thisObj, fidIndexDomain, jIndexDomain);


    jfieldID fidIndexMapByteSize = (*env)->GetFieldID(env, thisClass, "discoveredIndexMapByteSize", "I");
    if (NULL == fidIndexMapByteSize) return;

    jint jIndexMapByteSize = (*env)->GetIntField(env, thisObj, fidIndexMapByteSize);
    jIndexMapByteSize = (jint) metadata.idx_map_byte_sizes[newIndexID];
    (*env)->SetIntField(env, thisObj, fidIndexMapByteSize, jIndexMapByteSize);


    jfieldID fidColDomains = (*env)->GetFieldID(env, thisClass, "discoveredColDomains", "[J");
    if (NULL == fidColDomains) return;

    // Get the object field, returns JObject (because Array is instance of Object)
    jobject objColDomains = (*env)->GetObjectField(env, thisObj, fidColDomains);

    // Cast it to a jdoublearray
    jlongArray* jArrayColDomains = reinterpret_cast<jlongArray*>(&objColDomains);

    // Get the elements (you probably have to fetch the length of the array as well
    jlong* jColDomains = (*env)->GetLongArrayElements(*jArrayColDomains, NULL);
    for (int i=0; i<numEncodings; i++) {
        jColDomains[i] = (jlong) metadata.idx_domains[newIndexID][i];
    }

    // Don't forget to release it
    env->ReleaseLongArrayElements(*jlongArray, jColDomains, 0);




    jfieldID fidPath = (*env)->GetFieldID(env, thisClass, "pathAndFileName", "Ljava/lang/String;");
    if (NULL == fidPath) return;



    jfieldID fidNumEncodings = (*env)->GetFieldID(env, thisClass, "numEncodings", "I");
    if (NULL == fidNumEncodings) return;



    jfieldID fidColEncodings = (*env)->GetFieldID(env, thisClass, "colEncodings", "[I");


}

JNIEXPORT jintArray JNICALL Java_gqfast_global_JNILoader_run_1query_1aggregate_1int
  (JNIEnv *env, jobject thisObj, jstring funcName, jint resultIndexID)
{

    jintArray j[]1];

    return j;

}

/*
 * Class:     gqfast_global_JNILoader
 * Method:    run_query_aggregate_double
 * Signature: (Ljava/lang/String;I)[D
 */
JNIEXPORT jdoubleArray JNICALL Java_gqfast_global_JNILoader_run_1query_1aggregate_1double
  (JNIEnv *env, jobject thisObj, jstring funcName, jint resultIndexID)
{


    jdoubeArray j[1];

    return j;





}




#endif
