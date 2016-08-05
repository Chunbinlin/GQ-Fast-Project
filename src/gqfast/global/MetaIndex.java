package gqfast.global;

import java.util.List;

public class MetaIndex {

	//private int indexID;
	private int gqFastIndexID; //0
	private int numColumns;
	private int indexMapByteSize;
	private long indexDomain;
	private int maxFragmentSize;
	
	private List<Integer> columnEncodingsList;
	private List<Integer> columnEncodedByteSizesList;
	private List<Long> columnDomains;
	
	public MetaIndex(int gqFastIndexID, int numColumns, int indexMapByteSize,
			long indexDomain, int maxFragmentSize, List<Integer> columnEncodingsList,
			List<Integer> columnEncodedByteSizesList, List<Long> columnDomains) {
		//this.indexID = indexID;
		this.gqFastIndexID = gqFastIndexID;
		this.numColumns = numColumns;
		this.indexMapByteSize = indexMapByteSize;
		this.indexDomain = indexDomain;
		this.maxFragmentSize = maxFragmentSize;
		this.columnEncodingsList = columnEncodingsList;
		this.columnEncodedByteSizesList = columnEncodedByteSizesList;
		this.columnDomains = columnDomains;
	}

	
	
	public int getGQFastIndexID() {
		return gqFastIndexID;
	}

	public int getNumColumns() {
		return numColumns;
	}

	public int getIndexMapByteSize() {
		return indexMapByteSize;
	}

	public List<Integer> getColumnEncodingsList() {
		return columnEncodingsList;
	}

	public List<Integer> getColumnEncodedByteSizesList() {
		return columnEncodedByteSizesList;
	}


	public long getIndexDomain() {
		return indexDomain;
	}


	public int getMaxFragmentSize() {
		return maxFragmentSize;
	}



	public List<Long> getColumnDomains() {
		return columnDomains;
	}	
	
	public void print() {
		System.out.println("...print MetaIndex... ");
		System.out.println("gqFastIndexID = " + gqFastIndexID);
		System.out.println("numColumns = " + numColumns);
		System.out.println("indexMapByteSize = " + indexMapByteSize);
		System.out.println("indexDomain = " + indexDomain);
		
		for (int i=0; i<numColumns; i++) {
			System.out.println("colEncoding " + i + " = " + columnEncodingsList.get(i));
			System.out.println("colEncodedByteSize " + i + " = " + columnEncodedByteSizesList.get(i));
			System.out.println("colDomain " + i + " = " + columnDomains.get(i));
		}
		System.out.println("...end print MetaIndex...");
		
	}
	
	
}
