package codegenerator;

import java.util.ArrayList;
import java.util.List;

public class MetaQuery {

	final int queryID;
	final String queryName; 
	final int numThreads;
	final int numBuffers;
	final int bufferPoolSize;

	List<String> aliases;
	int[][] aliasBufferPoolIDs;
	boolean[] preThreading;
	
	public MetaQuery(int queryID, String queryName, int numThreads,
			int numBuffers, int bufferPoolSize, List<String> aliases) {
	
		this.queryID = queryID;
		this.queryName = queryName;
		this.numThreads = numThreads;
		this.numBuffers = numBuffers;
		this.bufferPoolSize = bufferPoolSize;
		this.aliases = aliases;
		
		aliasBufferPoolIDs = new int[aliases.size()][];
		preThreading = new boolean[aliases.size()];
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
