package codegenerator;

import java.util.List;

public class JoinOperator extends Operator {
	
	private  boolean entityFlag;
	private List<Integer> columnIDs; 
	
	private Alias alias;
	private int loopColumn;
	
	private Alias drivingAlias;
	private int drivingAliasColumn;


	
	public JoinOperator(boolean entityFlag, List<Integer> columnIDs,  Alias alias, int loopColumn, Alias drivingAlias, int drivingAliasColumn) {
		super(Operator.JOIN_OPERATOR);
		this.entityFlag = entityFlag;
		this.columnIDs = columnIDs;
		this.alias = alias;
		this.loopColumn = loopColumn;
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



	public int getLoopColumn() {
		return loopColumn;
	}



	public Alias getDrivingAlias() {
		return drivingAlias;
	}



	public int getDrivingAliasColumn() {
		return drivingAliasColumn;
	}
	



}
