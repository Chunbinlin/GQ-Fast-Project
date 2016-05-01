package codegenerator;

import java.util.ArrayList;
import java.util.List;


public abstract class Operator {
	
	public static final int JOIN_OPERATOR = 1;
	public static final int SEMIJOIN_OPERATOR = 2;
	public static final int INTERSECTION_OPERATOR = 3;
	public static final int AGGREGATION_OPERATOR = 4;
	public static final int SELECTION_OPERATOR = 5;
	public static final int THREADING_OPERATOR = 6;
	
	private int type;
	
	
	public Operator(int type) {
		this.type = type;
		
	}


	public int getType() {
		return type;
	}

}
