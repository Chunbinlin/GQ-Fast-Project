package codegenerator;

import java.util.List;

public class SelectionOperator extends Operator {

	private List<Integer> selectionsList;
	private Alias alias;
	
	public SelectionOperator(List<Integer> selectionsList, Alias alias) {
		super(Operator.SELECTION_OPERATOR);
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
