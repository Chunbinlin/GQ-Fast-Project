package codegenerator;

import java.util.ArrayList;
import java.util.List;

public class MetaQuery {

	private int queryID;
	private String queryName; 
	private int numThreads;
	private int numBuffers;
	private int bufferPoolSize;

	private List<String> aliases;
	private int[][] aliasBufferPoolIDs;
	private boolean[] preThreading;
	
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

	public int getQueryID() {
		return queryID;
	}

	public String getQueryName() {
		return queryName;
	}

	public int getNumThreads() {
		return numThreads;
	}

	public int getNumBuffers() {
		return numBuffers;
	}

	public int getBufferPoolSize() {
		return bufferPoolSize;
	}

	public List<String> getAliases() {
		return aliases;
	}

	public void initBufferPoolArray(int aliasID, int num_encodings) {
		aliasBufferPoolIDs[aliasID] = new int[num_encodings];
	}
	
	public boolean getPreThreading(int i) {
		return preThreading[i];
	}
	
	public void setPreThreading(int i, boolean value) {
		preThreading[i] = value;
	}
	
	public int getBufferPoolID(int aliasID, int colID) {
		return aliasBufferPoolIDs[aliasID][colID];
	}
	
	public void setBufferPoolID(int aliasID, int colID, int poolID) {
		aliasBufferPoolIDs[aliasID][colID] = poolID;
	}
	
	
	
}
