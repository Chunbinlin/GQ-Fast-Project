package gqfast.global;

public class JNILoadIndex {    
    
	//private String pathAndFileName;
	//private int numEncodings;
	//private int[] colEncodings;
	
	private long[] discoveredColDomains;
	private int discoveredIndexMapByteSize;
	private int[] discoveredColByteSizes;
	private int loaderIndexID;
	
	public JNILoadIndex() {
		//this.pathAndFileName = pathAndFileName;
		//this.numEncodings = numEncodings;
		//this.colEncodings = colEncodings;
		
		
		//discoveredColDomains = new long[numEncodings];
		//discoveredIndexMapByteSize = -1;
		//discoveredColByteSizes = new int[numEncodings];
		//loaderIndexID = -1;
	}
	
	
	public native void cpp_load_index(String path, int nEnc, int[] colEncs);    
    
	static {
        System.loadLibrary("cpploadindex");
    }        

    public void print (String pathAndFileName, int numEncodings, int[] colEncodings) {
    	
		discoveredColDomains = new long[numEncodings];
		discoveredIndexMapByteSize = -1;
		discoveredColByteSizes = new int[numEncodings];
		loaderIndexID = -1;
    	
    	cpp_load_index(pathAndFileName, numEncodings, colEncodings);
    	MetaIndex myMetaIndex;
    	
    	
    	
    	
    	
    }
    
    public static void main(String[] args) {
    	
    	String p = "./pubmed/DA1.csv";
    	int n = 1;
    	int[] c = {1};
    	
    	JNILoadIndex newIndex = new JNILoadIndex();
    	newIndex.print(p, n, c);
    	return;
    }
}