package codegenerator;

import java.util.ArrayList;
import java.util.List;


abstract class Operator {
	
	public static final int JOIN_OPERATOR = 1;
	public static final int SEMIJOIN_OPERATOR = 2;
	public static final int INTERSECTION_OPERATOR = 3;
	public static final int AGGREGATION_OPERATOR = 4;
	public static final int SELECTION_OPERATOR = 5;

	final int type;
	
	
	public Operator(int type) {
		this.type = type;
		
	}

}
