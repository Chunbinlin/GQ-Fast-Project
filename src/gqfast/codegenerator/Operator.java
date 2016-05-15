package gqfast.codeGenerator;

import gqfast.global.Global.Optypes;

public abstract class Operator {
	

	private Optypes type;
	
	
	public Operator(Optypes type) {
		this.type = type;
		
	}


	public Optypes getType() {
		return type;
	}

}
