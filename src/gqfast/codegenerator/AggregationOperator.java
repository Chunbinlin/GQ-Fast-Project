package codegenerator;

import java.util.List;

class AggregationOperator extends Operator {
	
	public static final int AGGREGATION_INT = 1;
	public static final int AGGREGATION_DOUBLE = 2;
	
	
	final int dataType;
	final int indexID;

	String aggregationString;
	List<Integer> aggregationVariablesAliases;
	List<Integer> aggregationVariablesColumns;
	
	int drivingAlias;
	int drivingAliasColumn;
	int drivingOperator;
	
	
	public AggregationOperator(int indexID, 
			int dataType, String aggregationString, 
			List<Integer> aggregationVariablesOperators, List<Integer> aggregationVariablesColumns, int drivingAlias, int drivingAliasColumn, int drivingOperator) {
		super(Operator.AGGREGATION_OPERATOR);
		this.indexID = indexID;
		this.dataType = dataType;
		this.aggregationString = aggregationString;
		this.aggregationVariablesAliases = aggregationVariablesOperators;		
		this.aggregationVariablesColumns = aggregationVariablesColumns;
		this.drivingAlias = drivingAlias;
		this.drivingAliasColumn = drivingAliasColumn;
		this.drivingOperator = drivingOperator;
	}
	
}
