package codegenerator;

import java.util.ArrayList;
import java.util.List;

public class MetaQuery {

	final int queryID;
	final String queryName; 
	final int numThreads;
	final int numBuffers;
	final int bufferPoolSize;
	final int threadingCutOffPoint;
	
	//List<Integer> selectionDataTypes;
	List<String> aliases;
	int[][] aliasBufferPoolIDs;
	
	public MetaQuery(int queryID, String queryName, int numThreads,
			int numBuffers, int bufferPoolSize, int threadingCutOffPoint, List<String> aliases) {
	
		this.queryID = queryID;
		this.queryName = queryName;
		this.numThreads = numThreads;
		this.numBuffers = numBuffers;
		this.bufferPoolSize = bufferPoolSize;
		this.threadingCutOffPoint = threadingCutOffPoint;
		//this.selectionDataTypes = selectionDataTypes;
		this.aliases = aliases;
		
		aliasBufferPoolIDs = new int[aliases.size()][];
		
	}

	public void initBufferPoolArray(int aliasID, int num_encodings) {
		aliasBufferPoolIDs[aliasID] = new int[num_encodings];
	}
	
	
	public int getBufferPoolID(int aliasID, int colID) {
		return aliasBufferPoolIDs[aliasID][colID];
	}
	
	public void setBufferPoolID(int aliasID, int colID, int poolID) {
		aliasBufferPoolIDs[aliasID][colID] = poolID;
	}
	
	
}
