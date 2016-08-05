package gqfast.codeGenerator;

import gqfast.global.Alias;
import gqfast.global.Global.Optypes;

import java.util.List;

public class AggregationOperator extends Operator {
	
	public static final int AGGREGATION_INT = 1;
	public static final int AGGREGATION_DOUBLE = 2;
	
	public static final int FUNCTION_COUNT = 1;
	public static final int FUNCTION_SUM = 2;
	
	
	private int dataType; // Always INT for now	
	private int aggregationFunction; // COUNT or SUM for now
	private Alias drivingAlias;
	private int drivingAliasColumn;
	
	private Alias aggregationAlias;
	private int aggregationAliasColumn;
	
	public AggregationOperator(int dataType, int aggregationFunction, Alias drivingAlias, 
			int drivingAliasColumn, Alias aggregationAlias, int aggregationAliasColumn) {
		super(Optypes.AGGREGATION_OPERATOR);
		this.dataType = AGGREGATION_INT; // Ignores incoming argument
		this.aggregationFunction = aggregationFunction;
		this.drivingAlias = drivingAlias;
		this.drivingAliasColumn = drivingAliasColumn;
		this.aggregationAlias = aggregationAlias;
		this.aggregationAliasColumn = aggregationAliasColumn;
		
	}
	
	
	public int getDataType() {
		return dataType;
	}

	
	
	public int getAggregationFunction() {
		return aggregationFunction;
	}


	public Alias getDrivingAlias() {
		return drivingAlias;
	}

	public int getDrivingAliasColumn() {
		return drivingAliasColumn;
	}


	public Alias getAggregationAlias() {
		return aggregationAlias;
	}


	public int getAggregationAliasColumn() {
		return aggregationAliasColumn;
	}
}
