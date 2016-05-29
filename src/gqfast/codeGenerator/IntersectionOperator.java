package gqfast.codeGenerator;

import gqfast.global.Global.Optypes;
import gqfast.global.Alias;

import java.util.List;

class IntersectionOperator extends Operator {

	private boolean bitwiseFlag;
	private List<Alias> aliases;
	private List<Integer> columnIDs; 
	private List<Integer> selections;
	
	public IntersectionOperator(boolean bitwiseFlag, List<Alias> aliases, 
		List<Integer> columnIDs, List<Integer> selections) {
		super(Optypes.INTERSECTION_OPERATOR);
		this.bitwiseFlag = bitwiseFlag;
		this.aliases = aliases;
		this.columnIDs = columnIDs;
		this.selections = selections;
		
	}

	public boolean isBitwiseFlag() {
		return bitwiseFlag;
	}

	public List<Alias> getAliases() {
		return aliases;
	}

	public List<Integer> getColumnIDs() {
		return columnIDs;
	}

	public List<Integer> getSelections() {
		return selections;
	}


}
