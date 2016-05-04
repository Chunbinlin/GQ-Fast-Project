package codegenerator;

import java.util.List;

public class AggregationOperator extends Operator {
	
	public static final int AGGREGATION_INT = 1;
	public static final int AGGREGATION_DOUBLE = 2;
	
	private int indexID;
	private int dataType;

	private String aggregationString;


	private List<Integer> aggregationVariablesAliases;
	private List<Integer> aggregationVariablesColumns;
	
	private Alias drivingAlias;
	private int drivingAliasColumn;
	private int drivingOperator;
	
	
	public AggregationOperator(int indexID, 
			int dataType, String aggregationString, 
			List<Integer> aggregationVariablesOperators, List<Integer> aggregationVariablesColumns, Alias drivingAlias, 
			int drivingAliasColumn, int drivingOperator) {
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

	public Alias getDrivingAlias() {
		return drivingAlias;
	}

	public int getDrivingAliasColumn() {
		return drivingAliasColumn;
	}

	public int getDrivingOperator() {
		return drivingOperator;
	}	
}
