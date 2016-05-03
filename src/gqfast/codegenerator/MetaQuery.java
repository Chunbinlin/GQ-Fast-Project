package codegenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetaQuery {

	private int queryID;
	private String queryName; 
	private int numThreads;
	private int numBuffers;
	private int bufferPoolSize;

	private Set<Integer> indexIDs;
	private List<String> aliases;
	private int[][] aliasBufferPoolIDs;
	private boolean[] preThreading;
	private Map<Integer, Integer> aliasIndexIDMap;
	
	
	public MetaQuery(int queryID, String queryName, int numThreads,
			int numBuffers, int bufferPoolSize, List<String> aliases, Map<Integer,Integer> aliasIndexIDMap) {
	
		this.queryID = queryID;
		this.queryName = queryName;
		this.numThreads = numThreads;
		this.numBuffers = numBuffers;
		this.bufferPoolSize = bufferPoolSize;
		this.aliases = aliases;
		this.aliasIndexIDMap = aliasIndexIDMap;
		indexIDs = new HashSet<Integer>();
		aliasBufferPoolIDs = new int[aliases.size()+1][];
		preThreading = new boolean[aliases.size()+1];
	}

	public Set<Integer> getIndexIDs() {
		return indexIDs;
	}

	public void setIndexID(int indexID) {
		indexIDs.add(indexID);
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
		aliasBufferPoolIDs[aliasID] = new int[num_encodings+1];
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

	public int getAliasIndexID(int index) {
		return aliasIndexIDMap.get(index);
	}
	
	
	
}
