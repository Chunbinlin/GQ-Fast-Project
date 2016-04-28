package codegenerator;

import java.util.List;

class IntersectionOperator extends Operator {

	final boolean bitwiseFlag;
	List<Integer> indexIDs;
	List<Integer> columnIDs; 
	
	public IntersectionOperator(boolean bitwiseFlag, List<Integer> indexIDs, 
		List<Integer> columnIDs) {
		super(Operator.INTERSECTION_OPERATOR);
		this.bitwiseFlag = bitwiseFlag;
		this.indexIDs = indexIDs;
		this.columnIDs = columnIDs;

		
	}

	
}
