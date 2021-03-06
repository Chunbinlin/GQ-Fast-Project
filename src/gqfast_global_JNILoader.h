/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class gqfast_global_JNILoader */

#ifndef _Included_gqfast_global_JNILoader
#define _Included_gqfast_global_JNILoader
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     gqfast_global_JNILoader
 * Method:    cppOpenLoader
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_gqfast_global_JNILoader_cppOpenLoader
  (JNIEnv *, jobject);

/*
 * Class:     gqfast_global_JNILoader
 * Method:    cppCloseLoader
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_gqfast_global_JNILoader_cppCloseLoader
  (JNIEnv *, jobject);

/*
 * Class:     gqfast_global_JNILoader
 * Method:    cppLoadIndex
 * Signature: (Ljava/lang/String;I[I)V
 */
JNIEXPORT void JNICALL Java_gqfast_global_JNILoader_cppLoadIndex
  (JNIEnv *, jobject, jstring, jint, jintArray);

/*
 * Class:     gqfast_global_JNILoader
 * Method:    runQueryAggregateInt
 * Signature: (Ljava/lang/String;II)[I
 */
JNIEXPORT jintArray JNICALL Java_gqfast_global_JNILoader_runQueryAggregateInt
  (JNIEnv *, jobject, jstring, jint, jint);

/*
 * Class:     gqfast_global_JNILoader
 * Method:    runQueryAggregateDouble
 * Signature: (Ljava/lang/String;II)[D
 */
JNIEXPORT jdoubleArray JNICALL Java_gqfast_global_JNILoader_runQueryAggregateDouble
  (JNIEnv *, jobject, jstring, jint, jint);

#ifdef __cplusplus
}
#endif
#endif
