package gqfast.codeGenerator;

import gqfast.global.Alias;
import gqfast.global.Global.Optypes;

import java.util.List;

public class SemiJoinOperator extends Operator {
	
	private boolean entityFlag;//always set to 0 for now
	private List<Integer> columnIDs; 

	private Alias alias;
	
	private Alias drivingAlias;
	private int drivingAliasColumn;


	
	public SemiJoinOperator(boolean entityFlag, List<Integer> columnIDs, Alias alias, Alias drivingAlias, int drivingAliasColumn) {
		super(Optypes.SEMIJOIN_OPERATOR);
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
