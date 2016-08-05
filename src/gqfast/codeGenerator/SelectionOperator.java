package gqfast.codeGenerator;

import gqfast.global.Alias;
import gqfast.global.Global.Optypes;

import java.util.List;

public class SelectionOperator extends Operator {

	private List<Integer> selectionsList; //85
	private Alias alias;//for SD query, alias = d0
	
	// TODO: private List<Integer> columnIDs; // When aggregation takes a value from a column of Selection alias
	public SelectionOperator(List<Integer> selectionsList, Alias alias) {
		super(Optypes.SELECTION_OPERATOR);
		this.selectionsList = selectionsList;
		this.alias = alias;
	}

	public List<Integer> getSelectionsList() {
		return selectionsList;
	}

	public Alias getAlias() {
		return alias;
	}

}
