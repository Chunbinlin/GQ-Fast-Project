package gqfast.global;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetaQuery {

	private int queryID;
	private String queryName; 
	private int numThreads;
	private int bufferPoolSize;
	private List<Alias> aliases;
	private int[][] aliasBufferPoolIDs;
	private boolean[] preThreading;
	
	public MetaQuery(int queryID, String queryName, int numThreads,
			int bufferPoolSize, List<Alias> aliases) {
	
		this.queryID = queryID;
		this.queryName = queryName;
		this.numThreads = numThreads;
		this.bufferPoolSize = bufferPoolSize;
		this.aliases = aliases;	
		aliasBufferPoolIDs = new int[aliases.size()+1][];
		preThreading = new boolean[aliases.size()+1];
	}

	public Set<Integer> getIndexIDs() {
		Set<Integer> indexIDs = new HashSet<Integer>();
		for (Alias nextAlias : aliases) {
			if (nextAlias.getAssociatedIndex() != null) {
				int nextIndexID = nextAlias.getAssociatedIndex().getGQFastIndexID();
				indexIDs.add(nextIndexID);
			}
		}
		return indexIDs;
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

	public int getBufferPoolSize() {
		return bufferPoolSize;
	}

	public List<Alias> getAliases() {
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
	
}
