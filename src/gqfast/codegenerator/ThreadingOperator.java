package codegenerator;

public class ThreadingOperator extends Operator {

	private Alias drivingAlias;
	
	public ThreadingOperator(Alias drivingAlias) {
		super(Operator.THREADING_OPERATOR);
		this.drivingAlias = drivingAlias;
	}

	public Alias getDrivingAlias() {
		return drivingAlias;
	}

}
