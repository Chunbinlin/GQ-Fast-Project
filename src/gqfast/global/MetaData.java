package gqfast.global;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class MetaData {
	
	public static final int ENCODING_UA = 1;
	public static final int ENCODING_BCA = 2;
	public static final int ENCODING_BB = 3;
	public static final int ENCODING_HUFFMAN = 4;

	public static final int BYTES_1 = 1;
	public static final int BYTES_2 = 2;
	public static final int BYTES_4 = 4;
	public static final int BYTES_8 = 8;
	
	private HashMap<Integer, MetaIndex> indexMap;
	private MetaQuery query;

	private int aggregation_domain_index_id;
	
	public MetaData() {
		indexMap = new HashMap<Integer, MetaIndex>();
	
	}
	
	public void setIndexMap(HashMap<Integer, MetaIndex> indexList)
	{
		this.indexMap = indexList;
	}

	public HashMap<Integer, MetaIndex> getIndexMap() {
		return indexMap;
	}

	public MetaQuery getQuery() {
		return query;
	}

	public void setQuery(MetaQuery query) {
		this.query = query;
	}

	public int getAggregation_domain_index_id() {
		return aggregation_domain_index_id;
	}

	public void setAggregation_domain_index_id(int aggregation_domain_index_id) {
		this.aggregation_domain_index_id = aggregation_domain_index_id;
	}


/*	public int getMaxIndexID() {
		int max = 0;
		for (MetaIndex index : indexList) {
			if (max < index.getGQFastIndexID()) {
				max = index.getGQFastIndexID();
			}
		}
		return max;
	}
	
	
	public int getMaxColID() {
		int max = 0;
		for (MetaIndex index : indexList) {
			if (max < index.getNumColumns()-1) {
				max = index.getNumColumns()-1;
			}
		}
		return max;
	}
	*/
}
