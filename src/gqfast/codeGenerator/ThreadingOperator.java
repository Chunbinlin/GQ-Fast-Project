package gqfast.codeGenerator;

import gqfast.global.Alias;
import gqfast.global.Global.Optypes;

public class ThreadingOperator extends Operator {

	private Alias drivingAlias;
	private boolean threadingAfterIntersection;
	
	public ThreadingOperator(Alias drivingAlias, boolean threadingAfterIntersection) {
		super(Optypes.THREADING_OPERATOR);
		this.drivingAlias = drivingAlias;
		this.threadingAfterIntersection = threadingAfterIntersection;
	}

	public Alias getDrivingAlias() {
		return drivingAlias;
	}

	public boolean isThreadingAfterIntersection() {
		return threadingAfterIntersection;
	}

	
	
}
