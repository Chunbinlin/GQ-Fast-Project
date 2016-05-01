package codegenerator;

import java.util.List;

public class SelectionOperator extends Operator {

	private List<Integer> selectionsList;
	private int aliasID;
	
	public SelectionOperator(List<Integer> selectionsList, int aliasID) {
		super(Operator.SELECTION_OPERATOR);
		this.selectionsList = selectionsList;
		this.aliasID = aliasID;
	}

	public List<Integer> getSelectionsList() {
		return selectionsList;
	}

	public int getAliasID() {
		return aliasID;
	}

}
