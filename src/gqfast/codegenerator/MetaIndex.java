package codegenerator;

import java.util.List;

public class MetaIndex {

	int indexID;
	int numColumns;
	int indexMapByteSize;
	List<Integer> columnEncodingsList;
	List<Integer> columnEncodedByteSizesList;
	
	public MetaIndex(int indexID, int numColumns, int indexMapByteSize,
			List<Integer> columnEncodingsList,
			List<Integer> columnEncodedByteSizesList) {
		this.indexID = indexID;
		this.numColumns = numColumns;
		this.indexMapByteSize = indexMapByteSize;
		this.columnEncodingsList = columnEncodingsList;
		this.columnEncodedByteSizesList = columnEncodedByteSizesList;
	}

	public int getMaxColumnID() {
		int max = 0;
		for (Integer col : columnEncodingsList) {
			if (max < col) {
				max = col;
			}
		}
		return max;
	}
}
