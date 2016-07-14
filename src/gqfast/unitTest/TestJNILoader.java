package gqfast.unitTest;

import java.util.HashMap;

import gqfast.global.Global.Encodings;
import gqfast.global.JNILoader;
import gqfast.global.MetaData;
import gqfast.global.MetaIndex;

public class TestJNILoader {

    public static void main(String[] args) {
    	
    	
    	JNILoader newLoader = new JNILoader();
    	newLoader.openLoader();
    	String file0Path = "./gqfast/loader/pubmed/da1.csv";
    	int file0Cols = 1;
    	Encodings[] file0Encs = {Encodings.Uncompress_Array};
    	
    	MetaIndex idx0 = newLoader.loadIndex(file0Path, file0Cols, file0Encs);
    	idx0.print();  	
    	
    	String file1Path = "./gqfast/loader/pubmed/dy.csv";
    	int file1Cols = 1;
    	Encodings[] file1Encs = {Encodings.Uncompress_Array};
    	
    	MetaIndex idx1 = newLoader.loadIndex(file1Path, file1Cols, file1Encs);
    	idx1.print();  	
    	
    	String file2Path = "./gqfast/loader/pubmed/dt1.csv";
    	int file2Cols = 2;
    	Encodings[] file2Encs = {Encodings.Uncompress_Array,Encodings.Uncompress_Array};
    	
    	MetaIndex idx2 = newLoader.loadIndex(file2Path, file2Cols, file2Encs);
    	idx2.print();  	
    	
    	String file3Path = "./gqfast/loader/pubmed/dt2.csv";
    	int file3Cols = 2;
    	Encodings[] file3Encs = {Encodings.Uncompress_Array,Encodings.Uncompress_Array};
    	
    	MetaIndex idx3 = newLoader.loadIndex(file3Path, file3Cols, file3Encs);
    	idx3.print();  	
    	
    	String file4Path = "./gqfast/loader/pubmed/da2.csv";
    	int file4Cols = 1;
    	Encodings[] file4Encs = {Encodings.Uncompress_Array};
    	
    	MetaIndex idx4 = newLoader.loadIndex(file4Path, file4Cols, file4Encs);
    	idx4.print();
    	
    	HashMap<Integer, MetaIndex> indexMap = new HashMap<Integer, MetaIndex>();
    	indexMap.put(idx0.getGQFastIndexID(), idx0);
    	indexMap.put(idx1.getGQFastIndexID(), idx1);
    	indexMap.put(idx2.getGQFastIndexID(), idx2);
    	indexMap.put(idx3.getGQFastIndexID(), idx3);
    	indexMap.put(idx4.getGQFastIndexID(), idx4);
    	    	
    	MetaData metadata = new MetaData();
    	metadata.setIndexMap(indexMap);
    	
    	return;
    	  	
    }
	
	
}
