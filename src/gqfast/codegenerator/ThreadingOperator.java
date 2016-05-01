package codegenerator;

public class ThreadingOperator extends Operator {

	int drivingAliasID;
	
	public ThreadingOperator(int drivingAliasID) {
		super(Operator.THREADING_OPERATOR);
		this.drivingAliasID = drivingAliasID;
	}

}
