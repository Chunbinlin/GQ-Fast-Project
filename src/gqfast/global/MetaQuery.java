package gqfast.global;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetaQuery {

	private String queryName; 
	private int numThreads; //fix to 4
	private List<Alias> aliases; 
	private boolean[] preThreading;//?
	
	public MetaQuery(String queryName, int numThreads,
			 List<Alias> aliases) {
	
		
		this.queryName = queryName;
		this.numThreads = numThreads;
		this.aliases = aliases;	
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

	public String getQueryName() {
		return queryName;
	}

	public int getNumThreads() {
		return numThreads;
	}


	public List<Alias> getAliases() {
		return aliases;
	}
	
	
	public boolean getPreThreading(int i) {
		return preThreading[i];
	}
	
	public void setPreThreading(int i, boolean value) {
		preThreading[i] = value;
	}
	

	public HashMap<Integer, Integer> getNumColumns(Set<Integer> indexSet) {
		
		HashMap<Integer, Integer> numColumnsMap = new HashMap<Integer, Integer>();
		for (int gqIndex : indexSet) {
			
			for (Alias nextAlias : aliases) {
				
				if (nextAlias.getAssociatedIndex() != null) {
					MetaIndex tempIndex = nextAlias.getAssociatedIndex();
					if (tempIndex.getGQFastIndexID() == gqIndex) {
						numColumnsMap.put(gqIndex, tempIndex.getNumColumns());
						break;
					}
				}
			}
			
		}
		
		return numColumnsMap;
	}	
	
}
