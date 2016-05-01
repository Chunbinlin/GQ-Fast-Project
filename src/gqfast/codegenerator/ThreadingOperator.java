package codegenerator;

public class ThreadingOperator extends Operator {

	private int drivingAliasID;
	
	public ThreadingOperator(int drivingAliasID) {
		super(Operator.THREADING_OPERATOR);
		this.drivingAliasID = drivingAliasID;
	}

	public int getDrivingAliasID() {
		return drivingAliasID;
	}

}
