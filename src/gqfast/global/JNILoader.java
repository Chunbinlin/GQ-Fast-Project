package gqfast.global;

import gqfast.global.Global.Encodings;

import java.util.ArrayList;
import java.util.List;

public class JNILoader {    
    
	private long[] discoveredColDomains;
	private int discoveredIndexMapByteSize;
	private int[] discoveredColByteSizes;
	private int loaderIndexID;
	private long discoveredIndexDomain;
	
	public JNILoader() { }
	
	public native void cppOpenLoader();
	public native void cppCloseLoader();
	public native void cppLoadIndex(String path, int nEnc, int[] colEncs);    
    public native int[] runQueryAggregateInt(String queryName, int resultArrayGQFastIndexID);
    public native double[] runQueryAggregateDouble(String queryName, int resultarraGQFastIndexID);
	
	static {
		System.loadLibrary("gqfast_global_JNILoader");
	}        
	
	public void openLoader() {
		cppOpenLoader();
	}
	
	public void closeLoader() {
		cppCloseLoader();
	}
	
	public MetaIndex loadIndex(String pathAndFileName, int numEncodings, Encodings[] colEncodings) {
		
		discoveredColDomains = new long[numEncodings];
		discoveredIndexMapByteSize = -1;
		discoveredColByteSizes = new int[numEncodings];
		loaderIndexID = -1;
    	discoveredIndexDomain = -1;
		
    	int[] colEncodingInts = new int[numEncodings];
    	for (int i=0; i<numEncodings; i++) {
    		
    		Encodings currEnc = colEncodings[i];
    		int encodingInt = 0;
    		
    		switch (currEnc) {
    		case Uncompress_Array:
    			encodingInt = 1;
    			break;
    		case Bit_Compress_Array:
    			encodingInt = 2;
    			break;
    		case Byte_Align_Bitmap:
    			encodingInt = 3;
    			break;
    		case Huffman:
    			encodingInt = 4;
    			break;
    		case Uncompress_Bitmap: // TODO: implement in C++ loader
    			encodingInt = 5;
    			break;
    		}
    		colEncodingInts[i] = encodingInt;
    		
    	}
    	
    	
    	cppLoadIndex(pathAndFileName, numEncodings, colEncodingInts);   		
    	
    	if (loaderIndexID >= 0) {
    		MetaIndex myMetaIndex;
    		List<Integer> colEncodingsList = new ArrayList<Integer>();
    		List<Integer> colEncodedByteSizeList = new ArrayList<Integer>();
    		List<Long> colDomains = new ArrayList<Long>();
    			
    		for (int i=0; i<numEncodings; i++) {
    			colEncodingsList.add(colEncodingInts[i]);
    			colEncodedByteSizeList.add(discoveredColByteSizes[i]);
    			colDomains.add(discoveredColDomains[i]);  			
    		}
  
    		myMetaIndex = new MetaIndex(loaderIndexID, numEncodings, discoveredIndexMapByteSize, 
    				discoveredIndexDomain, colEncodingsList, colEncodedByteSizeList, colDomains);
    		return myMetaIndex;
    	}
    	else {
    		System.err.println("Error! index ID not initialized properly");
    		return null;
    	}
			
	}
	
	
	
	
	
	/*
    public void print (String pathAndFileName, int numEncodings, int[] colEncodings) {
    	
		discoveredColDomains = new long[numEncodings];
		discoveredIndexMapByteSize = -1;
		discoveredColByteSizes = new int[numEncodings];
		loaderIndexID = -1;
    	discoveredIndexDomain = -1;
    	
    	cppOpenLoader();
    	cppLoadIndex(pathAndFileName, numEncodings, colEncodings);   		
    	MetaIndex myMetaIndex;
    	if (loaderIndexID >= 0) {
    		List<Integer> colEncodingsList = new ArrayList<Integer>();
    		List<Integer> colEncodedByteSizeList = new ArrayList<Integer>();
    		List<Long> colDomains = new ArrayList<Long>();
    			
    		for (int i=0; i<numEncodings; i++) {
    			colEncodingsList.add(colEncodings[i]);
    			colEncodedByteSizeList.add(discoveredColByteSizes[i]);
    			colDomains.add(discoveredColDomains[i]);  			
    		}
  
    		myMetaIndex = new MetaIndex(loaderIndexID, numEncodings, discoveredIndexMapByteSize, 
    				discoveredIndexDomain, colEncodingsList, colEncodedByteSizeList, colDomains);
    		myMetaIndex.print();
    	}
    	else {
    		System.err.println("Error! index ID not initialized properly");
    	}
    	
      	cppCloseLoader();
    }
    */
}