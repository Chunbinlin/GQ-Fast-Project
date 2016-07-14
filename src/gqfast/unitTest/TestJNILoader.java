package gqfast.unitTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import gqfast.codeGenerator.AggregationOperator;
import gqfast.global.Global.Encodings;
import gqfast.global.Alias;
import gqfast.global.JNILoader;
import gqfast.global.MetaData;
import gqfast.global.MetaIndex;
import gqfast.global.MetaQuery;

public class TestJNILoader {

	private static void initQ5Queries(MetaData metadata, String queryName, int numThreads) {

		List<Alias> aliases = new ArrayList<Alias>();
		
		Alias alias0 = new Alias(0, "author1");
		Alias alias1 = new Alias(1, "doc1", metadata.getIndexMap().get(0));
		Alias alias2 = new Alias(2, "term", metadata.getIndexMap().get(2));
		Alias alias3 = new Alias(3, "doc2", metadata.getIndexMap().get(3));
		Alias alias4 = new Alias(4, "author2", metadata.getIndexMap().get(4));
		Alias alias5 = new Alias(5, "year", metadata.getIndexMap().get(1));
		
		aliases.add(alias0);
		aliases.add(alias1);
		aliases.add(alias2);
		aliases.add(alias3);
		aliases.add(alias4);
		aliases.add(alias5);
		
		// public MetaQuery(int queryID, String queryName, int numThreads,
		// int numBuffers, int bufferPoolSize, List<String> aliases)
		MetaQuery q5Optimal = new MetaQuery(0, queryName, numThreads, aliases);
			
		metadata.getQueryList().add(q5Optimal);
		metadata.setCurrentQueryID(metadata.getQueryList().size()-1);
		
	}
	
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
    	
    	String file2Path = "./gqfast/loader/pubmed/dt1_mesh.csv";
    	int file2Cols = 2;
    	Encodings[] file2Encs = {Encodings.Uncompress_Array,Encodings.Uncompress_Array};
    	
    	MetaIndex idx2 = newLoader.loadIndex(file2Path, file2Cols, file2Encs);
    	idx2.print();  	
    	
    	String file3Path = "./gqfast/loader/pubmed/dt2_mesh.csv";
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
    	
    	String queryName = "q5_array_4threads_2981669";
    	int numThreads = 4;
    	initQ5Queries(metadata, queryName, numThreads);
    	newLoader.runQuery(queryName, AggregationOperator.AGGREGATION_DOUBLE, idx4.getGQFastIndexID(), 0);
    	    	
    	newLoader.closeLoader();   	
    	
    	  	
    }
	
	
}
