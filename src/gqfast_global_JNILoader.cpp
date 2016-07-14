#ifndef gqfast_global_jniloader_
#define gqfast_global_jniloader_

#include <jni.h>
#include <stdio.h>
#include "gqfast_global_JNILoader.h"
#include "./gqfast/loader/load_index.hpp"
#include "./gqfast/loader/run_query.hpp"
#include "./gqfast/loader/global_vars.hpp"

// Clock variables
struct timespec start;
struct timespec finish;

chrono::steady_clock::time_point benchmark_t1;
chrono::steady_clock::time_point benchmark_t2;

// Pre-declared index pointers
fastr_index<uint32_t>* idx_32[MAX_INDICES];
fastr_index<uint64_t>* idx_64[MAX_INDICES];

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



JNIEXPORT void JNICALL Java_gqfast_global_JNILoader_cppOpenLoader
(JNIEnv *env, jobject thisObj)
{
    init_globals();
}


JNIEXPORT void JNICALL Java_gqfast_global_JNILoader_cppCloseLoader
(JNIEnv *env, jobject thisObj)
{
    delete_globals();
}

JNIEXPORT void JNICALL Java_gqfast_global_JNILoader_cppLoadIndex
(JNIEnv *env, jobject thisObj, jstring pathAndFileName, jint numEncs, jintArray colEncs)
{
    // First parameter is string
    const char *cPathName = env->GetStringUTFChars(pathAndFileName, NULL);
    if (NULL == cPathName) return;
    string pathName(cPathName);
    env->ReleaseStringUTFChars(pathAndFileName, cPathName);  // release resources
    cerr << "Path set to " << pathName << "\n";

    // Second parameter
    int numEncodings = (int) numEncs;
    cerr << "Num encodings set to " << numEncodings << "\n";


    // Third parameter is an array
    jint *cColEncs = env->GetIntArrayElements(colEncs, NULL);
    if (NULL == cColEncs) return;
    jsize length = env->GetArrayLength(colEncs);
    if (length != numEncodings)
    {
        cerr << "Error: unexpected derived length of colEnc array\n";
        return;
    }

    int encodingInts[numEncodings];
    for (int i=0; i<numEncodings; i++)
    {
        cerr << "Encoding " << i << " is " << cColEncs[i] << "\n";
        encodingInts[i] = (int) cColEncs[i];
    }

    env->ReleaseIntArrayElements(colEncs, cColEncs, 0); // release resources

    cerr << "check0\n";
    Encodings* encodingsArray[numEncodings];
    for (int i=0; i<numEncodings; i++)
    {
        string tempString = "col" + i;
        cerr << "test\n";
        Encodings* temp = new Encodings(tempString, encodingInts[i]);
        encodingsArray[i]=temp;
        cerr << "test2\n";
    }

    cerr << "check1\n";

    // Calls function in load_index.hpp to build index
    int newIndexID = load(pathName, encodingsArray, numEncodings);
    if (newIndexID < 0)
    {
        cerr << "index id not generated properly.\n";
        return;
    }

    //Clean-up
    for (int i=0; i<numEncodings; i++)
    {
        delete encodingsArray[i];
    }

    cerr << "check2\n";

    // Update Java vars
    jclass thisClass = env->GetObjectClass(thisObj);

    jfieldID fidIndexID = env->GetFieldID(thisClass, "loaderIndexID", "I");
    if (NULL == fidIndexID) return;

    jint jIndexID = env->GetIntField(thisObj, fidIndexID);
    jIndexID = (jint) newIndexID;
    env->SetIntField(thisObj, fidIndexID, jIndexID);


    jfieldID fidIndexDomain = env->GetFieldID(thisClass, "discoveredIndexDomain", "J");
    if (NULL == fidIndexDomain) return;

    jlong jIndexDomain = env->GetLongField(thisObj, fidIndexDomain);
    jIndexDomain = (jlong) idx[newIndexID]->domain_size;
    env->SetLongField(thisObj, fidIndexDomain, jIndexDomain);


    jfieldID fidIndexMapByteSize = env->GetFieldID(thisClass, "discoveredIndexMapByteSize", "I");
    if (NULL == fidIndexMapByteSize) return;

    jint jIndexMapByteSize = env->GetIntField(thisObj, fidIndexMapByteSize);
    jIndexMapByteSize = (jint) metadata.idx_map_byte_sizes[newIndexID];
    env->SetIntField(thisObj, fidIndexMapByteSize, jIndexMapByteSize);


    jfieldID fidColDomains = env->GetFieldID(thisClass, "discoveredColDomains", "[J");
    if (NULL == fidColDomains) return;

    // Get the object field, returns JObject (because Array is instance of Object)
    jobject objColDomains = env->GetObjectField(thisObj, fidColDomains);
    jlongArray* jArrayColDomains = reinterpret_cast<jlongArray*>(&objColDomains);
    // Get the elements (you probably have to fetch the length of the array as well
    jlong* jColDomains = env->GetLongArrayElements(*jArrayColDomains, NULL);
    for (int i=0; i<numEncodings; i++)
    {
        jColDomains[i] = (jlong) metadata.idx_domains[newIndexID][i];
    }
    // Don't forget to release it
    env->ReleaseLongArrayElements(*jArrayColDomains, jColDomains, 0);


    jfieldID fidColByteSizes = env->GetFieldID(thisClass, "discoveredColByteSizes", "[I");
    if (NULL == fidColByteSizes) return;

    // Get the object field, returns JObject (because Array is instance of Object)
    jobject objColByteSizes = env->GetObjectField(thisObj, fidColByteSizes);
    jintArray* jArrayColByteSizes = reinterpret_cast<jintArray*>(&objColByteSizes);
    // Get the elements (you probably have to fetch the length of the array as well
    jint* jColByteSizes = env->GetIntArrayElements(*jArrayColByteSizes, NULL);
    for (int i=0; i<numEncodings; i++)
    {
        jColByteSizes[i] = (jint) metadata.idx_cols_byte_sizes[newIndexID][i];
    }
    // Don't forget to release it
    env->ReleaseIntArrayElements(*jArrayColByteSizes, jColByteSizes, 0);

}

JNIEXPORT jintArray JNICALL Java_gqfast_global_JNILoader_runQueryAggregateInt
(JNIEnv *env, jobject thisObj, jstring jFuncName, jint resultIndexID, jint resultCol)
{
    // First parameter is string
    const char *cFuncName = env->GetStringUTFChars(jFuncName, NULL);
    if (NULL == cFuncName) return NULL;
    string funcName(cFuncName);
    env->ReleaseStringUTFChars(jFuncName, cFuncName);  // release resources
    cerr << "Function name set to " << funcName << "\n";

     // Second parameter
    int resIndexID = (int) resultIndexID;
    cerr << "Result index id " << resIndexID << "\n";

    int resCol = (int) resultCol;
    cerr << "Result col is " << resCol << "\n";

    int* result = handle_input<int>(funcName, resIndexID, resCol);
    jint* jresult = reinterpret_cast<jint*>(result);

    uint64_t result_domain = metadata.idx_domains[resIndexID][resCol];
    jintArray outJNIArray = env->NewIntArray(result_domain);  // allocate

    if (NULL == outJNIArray) return NULL;
    env->SetIntArrayRegion(outJNIArray, 0 , result_domain, jresult);  // copy

    return outJNIArray;

}

/*
 * Class:     gqfast_global_JNILoader
 * Method:    run_query_aggregate_double
 * Signature: (Ljava/lang/String;I)[D
 */
JNIEXPORT jdoubleArray JNICALL Java_gqfast_global_JNILoader_runQueryAggregateDouble
(JNIEnv *env, jobject thisObj, jstring jFuncName, jint resultIndexID, jint resultCol)
{

    // First parameter is string
    const char *cFuncName = env->GetStringUTFChars(jFuncName, NULL);
    if (NULL == cFuncName) return NULL;
    string funcName(cFuncName);
    env->ReleaseStringUTFChars(jFuncName, cFuncName);  // release resources
    cerr << "Function name set to " << funcName << "\n";

     // Second parameter
    int resIndexID = (int) resultIndexID;
    cerr << "Result index id " << resIndexID << "\n";

    int resCol = (int) resultCol;
    cerr << "Result col is " << resCol << "\n";

    double* result = handle_input<double>(funcName, resIndexID, resCol);
    jdouble* jresult = reinterpret_cast<jdouble*>(result);

    uint64_t result_domain = metadata.idx_domains[resIndexID][resCol];
    jdoubleArray outJNIArray = env->NewDoubleArray(result_domain);  // allocate

    if (NULL == outJNIArray) return NULL;
    env->SetDoubleArrayRegion(outJNIArray, 0 , result_domain, jresult);  // copy

    return outJNIArray;

}




#endif
