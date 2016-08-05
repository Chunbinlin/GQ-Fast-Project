package gqfast.codeGenerator;

import gqfast.global.Alias;
import gqfast.global.Global.Optypes;
//insert the thread directly after the join operator
public class ThreadingOperator extends Operator {

	private Alias drivingAlias;//dt1, first join. 
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
