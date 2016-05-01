package codegenerator;

import java.util.List;

public class JoinOperator extends Operator {
	
	final int indexID;
	
	final boolean entityFlag;
	List<Integer> columnIDs; 
	
	final int alias;
	final int loopColumn;
	
	final int drivingAliasID;
	final int drivingAliasColumn;
	

	
	public JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn) {
		super(Operator.JOIN_OPERATOR);
		this.indexID = indexID;
		this.entityFlag = entityFlag;
		this.columnIDs = columnIDs;
		this.alias = alias;
		this.loopColumn = loopColumn;
		this.drivingAliasID = drivingAliasID;
		this.drivingAliasColumn = drivingAliasColumn;
		
	}



}
