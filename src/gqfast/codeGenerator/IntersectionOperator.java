package gqfast.codeGenerator;

import gqfast.global.Global.Optypes;
import gqfast.global.Alias;

import java.util.List;

public class IntersectionOperator extends Operator {

	private boolean bitwiseFlag;//0: no bitmap, 1: bitmap. set to 0 for mow
	private List<Alias> aliases; //dt1 for each selection
	private List<Integer> columnIDs; //0: term column in dt1
	private List<Integer> selections;//5,7
	
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
