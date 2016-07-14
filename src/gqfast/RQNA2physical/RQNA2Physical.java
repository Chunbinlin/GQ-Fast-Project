package gqfast.RQNA2physical;

import gqfast.codeGenerator.AggregationOperator;
import gqfast.codeGenerator.JoinOperator;
import gqfast.codeGenerator.Operator;
import gqfast.codeGenerator.SelectionOperator;
import gqfast.global.Alias;
import gqfast.global.Global.Optypes;
import gqfast.global.MetaData;
import gqfast.global.MetaIndex;
import gqfast.global.MetaQuery;
import gqfast.global.Property;
import gqfast.global.Term;
import gqfast.global.TreeNode;
import gqfast.logical2RQNA.RelationalAlgebra2RQNA;
import gqfast.unitTest.TestTree_logical2RQNA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RQNA2Physical
{
	public List<Alias> aliases  = new ArrayList<Alias>();
	public List<Operator> operators = new ArrayList<Operator>();
	public int alias_id = 0;
	public static void main(String[] args)
	{
        TestTree_logical2RQNA test = new TestTree_logical2RQNA();
        test.TreeAS();
        System.out.println("\n---------------------------------------------------------------------------------------");
        RelationalAlgebra2RQNA ra = new RelationalAlgebra2RQNA(test.getroot()); 
        TreeNode RQNA = ra.RA2RQNA(true);
        RQNA2Physical rqna2physical = new RQNA2Physical();
        rqna2physical.RQNA2Physical(null,RQNA);
	}
	public R2P_Output RQNA2Physical(HashMap<Integer, MetaIndex> indexList, TreeNode RQNA)
	{
		R2P_Output output = new R2P_Output();
		//input of code generator
		MetaData metadata = new MetaData();
		
		
		//set meta query
		MetaQuery query = null;
		int queryID = 0;
		String queryName = "Q0"; 
		int numThreads = 4;
		//int bufferPoolSize = 1000;
		getRename(RQNA);
		travel(RQNA);
		query = new  MetaQuery(queryID, queryName, numThreads,
				 aliases);
		//add meatQuery into metaData
		metadata.getQueryList().add(query);
		//add meatIndex into metaData
		metadata.setCurrentQueryID(queryID);
		//add queryID into metaData
		metadata.setIndexList(indexList);
		
		output.setMetaData(metadata);
		output.setOperators(operators);
		return output;
	}
	public void getRename(TreeNode t) {
		if (t != null) {
			//rename
			if (t.get_Optype() == Optypes.RENAME_OPERATOR)
			{
				Alias alias = null;
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					//get all the renames
					System.out.println("[rename]:"+prop.get(i).getTerm2().get_variable()+"->"+prop.get(i).getTerm1().get_variable());
					//TODO: change metaIndex from null to real value
					alias =  new Alias(alias_id++, prop.get(i).getTerm1().get_variable(), null);
					aliases.add(alias);
				}
			}
			
			getRename(t.left);
			getRename(t.right);
		}
	}
	public void travel(TreeNode t) {
		if (t != null) {
			//aggregation
			if(t.get_Optype() == Optypes.AGGREGATION_OPERATOR)
			{
				List<Term> aggr_term = t.get_AggrTerm();
				List<String> aggr= t.get_AggrList();
				//for code generator
				int aggregationindexID = 4;
				List<Alias> aggAliasList = new ArrayList<Alias>();
				List<Integer> aggOpColList = new ArrayList<Integer>();
				
				for (int i = 0; i < aggr_term.size(); i++){
					System.out.print("[aggregation]"+aggr_term.get(i).get_variable()+","+aggr_term.get(i).get_column()+";");
					aggAliasList.add(aliases.get(2));
				}
				for (int i = 0; i < aggr.size(); i++){
					System.out.println(aggr.get(i)+",");
					Operator agg = new AggregationOperator(aggregationindexID, 
							AggregationOperator.AGGREGATION_DOUBLE, aggr.get(i), aggAliasList, aggOpColList, aliases.get(4), 0);
					operators.add(agg);
				}
			}
			//join
			if(t.get_Optype() == Optypes.JOIN_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					//get all the renames
					System.out.println("[join]:"+prop.get(i).getTerm2().get_variable()+"."+prop.get(i).getTerm2().get_column()
							+" eq "+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column());
					//for code generator
					List<Integer> column3IDs = new ArrayList<Integer>();
					column3IDs.add(0);
					column3IDs.add(1);
					Operator join3 = new JoinOperator(false, column3IDs, aliases.get(3), aliases.get(2), 0);
					operators.add(join3);
				}
				
			}
			//projection
			if(t.get_Optype() == Optypes.PROJECTION_OPERATOR)
			{
				List<Term> terms = t.get_TermList();
				System.out.print("[Projection]");
				for (int i = 0; i < terms.size(); i++){
					System.out.print(terms.get(i).get_variable()+"."+terms.get(i).get_column()+";");
				}
				System.out.println("");
			}
			//selection
			if(t.get_Optype() == Optypes.SELECTION_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				//for code generator 
				List<Integer> selectionsList = new ArrayList<Integer>();
				for (int i = 0; i <prop.size(); i++){
					//get all the selection
					System.out.println("[Selection]:"+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column()
							+" eq "+prop.get(i).getTerm2().get_constant());
					//1 is the input value, which is a=1 (here 1 is a example value)
					selectionsList.add(1);
//					System.out.println("======================"+aliases.size());
					Operator selection1 = new SelectionOperator(selectionsList, aliases.get(0));
					operators.add(selection1);
				}
				
			}
			travel(t.left);
			travel(t.right);
		}
	}
}
