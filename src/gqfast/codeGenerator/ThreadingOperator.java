package gqfast.codeGenerator;

import gqfast.global.Alias;
import gqfast.global.Global.Optypes;

public class ThreadingOperator extends Operator {

	private Alias drivingAlias;
	
	public ThreadingOperator(Alias drivingAlias) {
		super(Optypes.THREADING_OPERATOR);
		this.drivingAlias = drivingAlias;
	}

	public Alias getDrivingAlias() {
		return drivingAlias;
	}

}
