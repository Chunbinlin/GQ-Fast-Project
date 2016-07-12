package gqfast.global;

import java.util.ArrayList;
import java.util.List;

public class JNILoader {    
    
	//private String pathAndFileName;
	//private int numEncodings;
	//private int[] colEncodings;
	
	private long[] discoveredColDomains;
	private int discoveredIndexMapByteSize;
	private int[] discoveredColByteSizes;
	private int loaderIndexID;
	private long discoveredIndexDomain;
	
	public JNILoader() {
		//this.pathAndFileName = pathAndFileName;
		//this.numEncodings = numEncodings;
		//this.colEncodings = colEncodings;
		
		
		//discoveredColDomains = new long[numEncodings];
		//discoveredIndexMapByteSize = -1;
		//discoveredColByteSizes = new int[numEncodings];
		//loaderIndexID = -1;
	}
	
	public native void cpp_open_loader();
	public native void cpp_close_loader();
	public native void cpp_load_index(String path, int nEnc, int[] colEncs);    
    public native int[] run_query_aggregate_int(String queryName, int resultArrayGQFastIndexID);
    public native double[] run_query_aggregate_double(String queryName, int resultarraGQFastIndexID);
	
	static {
        System.loadLibrary("cpploadindex");
    }        

    public void print (String pathAndFileName, int numEncodings, int[] colEncodings) {
    	
		discoveredColDomains = new long[numEncodings];
		discoveredIndexMapByteSize = -1;
		discoveredColByteSizes = new int[numEncodings];
		loaderIndexID = -1;
    	discoveredIndexDomain = -1;
    	
    	cpp_load_index(pathAndFileName, numEncodings, colEncodings);
    	   		
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
    	
      	
    }
    
    public static void main(String[] args) {
    	
    	String p = "./pubmed/DA1.csv";
    	int n = 1;
    	int[] c = {1};
    	
    	JNILoader newIndex = new JNILoader();
    	newIndex.print(p, n, c);
    	return;
    }
}