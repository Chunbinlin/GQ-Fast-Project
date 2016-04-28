package codegenerator;

import java.util.List;

public class SelectionOperator extends Operator {

	List<Integer> selectionsList;
	int alias;
	
	public SelectionOperator(List<Integer> selectionsList, int alias) {
		super(Operator.SELECTION_OPERATOR);
		this.selectionsList = selectionsList;
		this.alias = alias;
	}

}
