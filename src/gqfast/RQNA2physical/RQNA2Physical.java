package gqfast.RQNA2physical;

import java.util.List;

import gqfast.global.Property;
import gqfast.global.Term;
import gqfast.global.TreeNode;
import gqfast.global.Global.Optypes;
import gqfast.logical2RQNA.RelationalAlgebra2RQNA;
import gqfast.unitTest.TestTree_logical2RQNA;

public class RQNA2Physical
{
	public static void main(String[] args)
	{
        TestTree_logical2RQNA test = new TestTree_logical2RQNA();
        test.TreeAS();
        System.out.println("\n---------------------------------------------------------------------------------------");
        RelationalAlgebra2RQNA ra = new RelationalAlgebra2RQNA(test.getroot()); 
        TreeNode RQNA = ra.RA2RQNA(true);
        RQNA2Physical rqna2physical = new RQNA2Physical();
        rqna2physical.travel(RQNA);
	}
	public void travel(TreeNode t) {
		if (t != null) {
			if (t.get_Optype() == Optypes.RENAME_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					//get all the renames
					System.out.println("[rename]:"+prop.get(i).getTerm2().get_variable()+"->"+prop.get(i).getTerm1().get_variable());
				}
			}
			if(t.get_Optype() == Optypes.AGGREGATION_OPERATOR)
			{
				List<Term> aggr_term = t.get_AggrTerm();
				List<String> aggr= t.get_AggrList();
				for (int i = 0; i < aggr_term.size(); i++){
					System.out.print("[aggregation]"+aggr_term.get(i).get_variable()+","+aggr_term.get(i).get_column()+";");
				}
				for (int i = 0; i < aggr.size(); i++){
					System.out.println(aggr.get(i)+",");
				}
			}
			if(t.get_Optype() == Optypes.JOIN_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					//get all the renames
					System.out.println("[join]:"+prop.get(i).getTerm2().get_variable()+"."+prop.get(i).getTerm2().get_column()
							+" eq "+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column());
				}
			}
			if(t.get_Optype() == Optypes.PROJECTION_OPERATOR)
			{
				List<Term> terms = t.get_TermList();
				System.out.print("[Projection]");
				for (int i = 0; i < terms.size(); i++){
					System.out.print(terms.get(i).get_variable()+"."+terms.get(i).get_column()+";");
				}
				System.out.println("");
			}
			if(t.get_Optype() == Optypes.SELECTION_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					//get all the renames
					System.out.println("[Selection]:"+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column()
							+" eq "+prop.get(i).getTerm2().get_constant());
				}
			}
			travel(t.left);
			travel(t.right);
		}
	}
}
