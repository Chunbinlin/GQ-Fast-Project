package codegenerator;

import java.util.List;

public class AggregationOperator extends Operator {
	
	public static final int AGGREGATION_INT = 1;
	public static final int AGGREGATION_DOUBLE = 2;
	
	
	private int dataType;
	private int indexID;

	private String aggregationString;


	private List<Integer> aggregationVariablesAliases;
	private List<Integer> aggregationVariablesColumns;
	
	private int drivingAlias;
	private int drivingAliasColumn;
	private int drivingOperator;
	private int drivingAliasIndexID;
	
	public AggregationOperator(int indexID, 
			int dataType, String aggregationString, 
			List<Integer> aggregationVariablesOperators, List<Integer> aggregationVariablesColumns, int drivingAlias, 
			int drivingAliasColumn, int drivingOperator, int drivingAliasIndexID) {
		super(Operator.AGGREGATION_OPERATOR);
		this.indexID = indexID;
		this.dataType = dataType;
		this.aggregationString = aggregationString;
		this.aggregationVariablesAliases = aggregationVariablesOperators;		
		this.aggregationVariablesColumns = aggregationVariablesColumns;
		this.drivingAlias = drivingAlias;
		this.drivingAliasColumn = drivingAliasColumn;
		this.drivingOperator = drivingOperator;
		this.drivingAliasIndexID = drivingAliasIndexID;
	}
	
	
	public int getDataType() {
		return dataType;
	}

	public int getIndexID() {
		return indexID;
	}

	public String getAggregationString() {
		return aggregationString;
	}

	public List<Integer> getAggregationVariablesAliases() {
		return aggregationVariablesAliases;
	}

	public List<Integer> getAggregationVariablesColumns() {
		return aggregationVariablesColumns;
	}

	public int getDrivingAlias() {
		return drivingAlias;
	}

	public int getDrivingAliasColumn() {
		return drivingAliasColumn;
	}

	public int getDrivingOperator() {
		return drivingOperator;
	}

	public int getDrivingAliasIndexID() {
		return drivingAliasIndexID;
	}
	
}
