package codegenerator;

import java.util.List;

public class SemiJoinOperator extends Operator {
	
	private int indexID;
	private boolean entityFlag;
	private List<Integer> columnIDs; 

	private int aliasID;
	private int loopColumn;
	
	private int drivingAliasID;
	private int drivingAliasColumn;
	private int drivingAliasIndexID;

	
	public SemiJoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs, int aliasID, int loopColumn, int drivingAliasID, int drivingAliasColumn, int drivingAliasIndexID) {
		super(Operator.SEMIJOIN_OPERATOR);
		this.indexID = indexID;
		this.entityFlag = entityFlag;
		this.columnIDs = columnIDs;
		this.aliasID = aliasID;
		this.loopColumn = loopColumn;
		this.drivingAliasID = drivingAliasID;
		this.drivingAliasColumn = drivingAliasColumn;
		this.drivingAliasIndexID = drivingAliasIndexID;
	}


	public int getIndexID() {
		return indexID;
	}


	public boolean isEntityFlag() {
		return entityFlag;
	}


	public List<Integer> getColumnIDs() {
		return columnIDs;
	}


	public int getAliasID() {
		return aliasID;
	}


	public int getLoopColumn() {
		return loopColumn;
	}


	public int getDrivingAliasID() {
		return drivingAliasID;
	}


	public int getDrivingAliasColumn() {
		return drivingAliasColumn;
	}


	public int getDrivingAliasIndexID() {
		return drivingAliasIndexID;
	}
	
}
