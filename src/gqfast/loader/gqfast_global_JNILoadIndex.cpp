#ifndef gqfast_global_jniloadindex_
#define gqfast_global_jniloadindex_

#include <jni.h>
#include <stdio.h>
#include "../../gqfast_global_JNILoadIndex.h"
#include "load.hpp"
#include "input_handling.hpp"
#include "global_vars.hpp"

int findEmptyIndexPosition()
{
    for (int i=0; i<MAX_INDICES; i++)
    {
        if (!idx_position_in_use[i]) {
            return i;
        }
    }

    cerr << "Error: No index slots available\n";
    return -1;

}



JNIEXPORT void JNICALL Java_gqfast_global_JNILoadIndex_cpp_1load_1index
(JNIEnv *env, jobject thisObj)
{

    jclass thisClass = (*env)->GetObjectClass(env, thisObj);

    jfieldID fidPath = (*env)->GetFieldID(env, thisClass, "pathAndFileName", "Ljava/lang/String;");
    if (NULL == fidPath) return;

    jstring jPathName = (*env)->GetObjectField(env, thisObj, fidPath);

    const char *cPathName = (*env)->GetStringUTFChars(env, jPathName, NULL);
    if (NULL == cPathName) return;

    string pathName(cPathName);

    cerr << "Path set to " << pathName << "\n";

    jfieldID fidNumEncodings = (*env)->GetFieldID(env, thisClass, "numEncodings", "I");
    if (NULL == fidNumEncodings) return;

    jint jNumEncodings = (*env)->GetIntField(env, thisObj, fidNumEncodings);

    int numEncodings = (int) jNumEncodings;
    cerr << "Num encodings set to " << numEncodings << "\n";


    jfieldID fidColEncodings = (*env)->GetFieldID(env, thisClass, "colEncodings", "[I");

    indexID = findEmptyIndexPosition();




}

#endif
