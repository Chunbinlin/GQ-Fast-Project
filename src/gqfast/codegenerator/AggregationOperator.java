package codegenerator;

import java.util.List;

public class AggregationOperator extends Operator {
	
	public static final int AGGREGATION_INT = 1;
	public static final int AGGREGATION_DOUBLE = 2;
	
	private int gqFastIndexID;
	private int dataType;

	private String aggregationString;


	private List<Alias> aggregationVariablesAliases;
	private List<Integer> aggregationVariablesColumns;
	
	private Alias drivingAlias;
	private int drivingAliasColumn;
	//private int drivingOperator;
	
	
	public AggregationOperator(int gqFastIndexID, int dataType, String aggregationString, 
			List<Alias> aggregationVariablesAliases, List<Integer> aggregationVariablesColumns, Alias drivingAlias, 
			int drivingAliasColumn) {
		super(Operator.AGGREGATION_OPERATOR);
		this.gqFastIndexID = gqFastIndexID;
		this.dataType = dataType;
		this.aggregationString = aggregationString;
		this.aggregationVariablesAliases = aggregationVariablesAliases;		
		this.aggregationVariablesColumns = aggregationVariablesColumns;
		this.drivingAlias = drivingAlias;
		this.drivingAliasColumn = drivingAliasColumn;

		
	}
	
	
	public int getDataType() {
		return dataType;
	}

	public int getGQFastIndexID() {
		return gqFastIndexID;
	}

	public String getAggregationString() {
		return aggregationString;
	}

	public List<Alias> getAggregationVariablesAliases() {
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
}
