package codegenerator;

import java.util.List;

public class SemiJoinOperator extends Operator {
	
	private boolean entityFlag;
	private List<Integer> columnIDs; 

	private Alias alias;
	
	private Alias drivingAlias;
	private int drivingAliasColumn;


	
	public SemiJoinOperator(boolean entityFlag, List<Integer> columnIDs, Alias alias, Alias drivingAlias, int drivingAliasColumn) {
		super(Operator.SEMIJOIN_OPERATOR);
		this.entityFlag = entityFlag;
		this.columnIDs = columnIDs;
		this.alias = alias;
		this.drivingAlias = drivingAlias;
		this.drivingAliasColumn = drivingAliasColumn;
	
	}

	public boolean isEntityFlag() {
		return entityFlag;
	}


	public List<Integer> getColumnIDs() {
		return columnIDs;
	}


	public Alias getAlias() {
		return alias;
	}

	public Alias getDrivingAlias() {
		return drivingAlias;
	}


	public int getDrivingAliasColumn() {
		return drivingAliasColumn;
	}


		
}
