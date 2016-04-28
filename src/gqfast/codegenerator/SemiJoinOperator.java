package codegenerator;

import java.util.List;

public class SemiJoinOperator extends Operator {
	
	final int indexID;
	final boolean entityFlag;
	List<Integer> columnIDs; 

	final int alias;
	final int loopColumn;
	
	final int drivingAliasID;
	final int drivingAliasColumn;
	final int drivingAliasIndexID;

	
	public SemiJoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs, int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn, int drivingAliasIndexID) {
		super(Operator.SEMIJOIN_OPERATOR);
		this.indexID = indexID;
		this.entityFlag = entityFlag;
		this.columnIDs = columnIDs;
		this.alias = alias;
		this.loopColumn = loopColumn;
		this.drivingAliasID = drivingAliasID;
		this.drivingAliasColumn = drivingAliasColumn;
		this.drivingAliasIndexID = drivingAliasIndexID;
	}
	
}
