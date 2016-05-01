package codegenerator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Metadata {
	
	public static final int ENCODING_UA = 1;
	public static final int ENCODING_BCA = 2;
	public static final int ENCODING_BB = 3;
	public static final int ENCODING_HUFFMAN = 4;

	public static final int BYTES_1 = 1;
	public static final int BYTES_2 = 2;
	public static final int BYTES_4 = 4;
	public static final int BYTES_8 = 8;
	List<MetaIndex> indexList;
	List<MetaQuery> queryList;
	
	int currentQueryID;
	
	public Metadata() {
		indexList = new ArrayList<MetaIndex>();
		queryList = new LinkedList<MetaQuery>();
		currentQueryID = 0;
	}
	
	public int getMaxIndexID() {
		int max = 0;
		for (MetaIndex index : indexList) {
			if (max < index.indexID) {
				max = index.indexID;
			}
		}
		return max;
	}
}
