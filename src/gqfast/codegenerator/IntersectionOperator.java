package codegenerator;

import java.util.List;

class IntersectionOperator extends Operator {

	private boolean bitwiseFlag;
	private List<Integer> indexIDs;
	private List<Integer> columnIDs; 
	
	public IntersectionOperator(boolean bitwiseFlag, List<Integer> indexIDs, 
		List<Integer> columnIDs) {
		super(Operator.INTERSECTION_OPERATOR);
		this.bitwiseFlag = bitwiseFlag;
		this.indexIDs = indexIDs;
		this.columnIDs = columnIDs;

		
	}

	public boolean isBitwiseFlag() {
		return bitwiseFlag;
	}

	public List<Integer> getIndexIDs() {
		return indexIDs;
	}

	public List<Integer> getColumnIDs() {
		return columnIDs;
	}

	
}
