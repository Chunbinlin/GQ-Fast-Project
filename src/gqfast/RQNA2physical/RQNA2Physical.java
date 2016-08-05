package gqfast.RQNA2physical;

import gqfast.codeGenerator.AggregationOperator;
import gqfast.codeGenerator.IntersectionOperator;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RQNA2Physical
{
	public List<Operator> operators = new ArrayList<Operator>();
	public HashMap<String, String> alias_2_table_name  = new HashMap<String, String>();
	public HashMap<String, String> alias_2_index_name  = new HashMap<String, String>();
	public int number_of_joins = 0;
	public int number_of_operators = 0;
	public boolean is_intersection_query = false;//0: no intersection. 1: has intersection
	List<Alias> aliases_4_intersection = new ArrayList<Alias>(); //dt1 for each selection
	public HashMap<String, Integer> alias_clumn_name_2_id = new HashMap<String, Integer>();
	public static void main(String[] args)
	{
		String query_name = "SD";
        TestTree_logical2RQNA test = new TestTree_logical2RQNA();
        test.TreeSD();
        test.print(test.getroot());
        System.out.println("\n---------------------------------------------------------------------------------------");
        RelationalAlgebra2RQNA ra = new RelationalAlgebra2RQNA(test.getroot()); 
        TreeNode RQNA = ra.RA2RQNA(true);
        RQNA2Physical rqna2physical = new RQNA2Physical();
        rqna2physical.RQNA2Physical(null,RQNA,query_name);
	}
	public R2P_Output RQNA2Physical(HashMap<Integer, MetaIndex> indexList, TreeNode RQNA, String query_name)
	{
		R2P_Output output = new R2P_Output();
		//input of code generator
		MetaData metadata = new MetaData();
		HashMap<Integer, MetaIndex> indexMap = new HashMap<Integer, MetaIndex>();
		
		//set MetaQuery
		MetaQuery meta_query = null;
		int queryID = 0; // no need to use it now, set to 0
		boolean[] preThreading = null;//always set to null
		int numThreads = 4;//fixed
		String queryName = query_name; 
		List<Alias> aliases = new ArrayList<Alias>();
		
		//set indexMap 
		System.out.println("Begin parsing all the aliases in the query...");
		getRename(RQNA);
		System.out.println("---------------------------------------------");
		for(String alias:alias_2_table_name.keySet())
		{
			System.out.println(alias+" --> "+alias_2_table_name.get(alias));
		}
		System.out.println("---------------------------------------------");
		System.out.println("Finish parsing all the aliases in the query...");
		System.out.println("Begin analyzing query relevant metadata...");
		getIndexName(RQNA);
		String path="";
		int index_id=0;
		MetaIndex meta_index=null;
		Alias alias_class = null;
		for(String alias:alias_2_index_name.keySet())
		{
			path="GQFast/MetaData/meta_"+alias_2_index_name.get(alias).toLowerCase()+".gqfast";
			meta_index=getMetaIndexFromDisk(path, alias, index_id);
//			System.out.println(alias+" --> "+alias_2_index_name.get(alias));
			alias_class = new Alias(index_id, alias, meta_index);
			aliases.add(alias_class);
			indexMap.put(index_id, meta_index);//it is correct
			index_id++;
		}
		meta_query = new MetaQuery(queryID, queryName, numThreads,aliases);
		metadata.setIndexMap(indexMap);
		metadata.setQuery(meta_query);
		System.out.println("Finish analyzing query relevant metadata...");
		System.out.println("Begin generating physical operators...");
		System.out.println("/////////////////////////////////////");
		number_of_operators = number_of_joins+1+1; //not include aggregation here. +1 is for selection/intersection. +1 is for thread_operator
		getOperators(RQNA);
		
		
		
//		travel(RQNA);
//		query = new  MetaQuery(queryID, queryName, numThreads,
//				 aliases);
//		//add meatQuery into metaData
//		metadata.getQueryList().add(query);
//		//add meatIndex into metaData
//		metadata.setCurrentQueryID(queryID);
//		//add queryID into metaData
//		metadata.setIndexMap(indexList);
		
		output.setMetaData(metadata);
		output.setOperators(operators);
		return output;
	}
	public void getRename(TreeNode t) {
		if (t != null) {
			//rename
			if (t.get_Optype() == Optypes.RENAME_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					//System.out.println("[rename]:"+prop.get(i).getTerm2().get_variable()+"->"+prop.get(i).getTerm1().get_variable());
					//get alias2tablename mapping
					alias_2_table_name.put(prop.get(i).getTerm1().get_variable(), prop.get(i).getTerm2().get_variable());
					
				}
			}
			getRename(t.left);
			getRename(t.right);
		}
	}
	public void getIndexName(TreeNode t) {
		if (t != null) {
			//selection [deal selection first here, since selection after intersection in the tree, so selection should be done before intersection]
			if(t.get_Optype() == Optypes.SELECTION_OPERATOR)
			{
				//for code generator
				List<Integer> selectionsList = new ArrayList<Integer>(); //85
				Alias alias = null;
				
				boolean bitwiseFlag = false;
				
				List<Integer> columnIDs = new ArrayList<Integer>(); //0: term column in dt1
				List<Integer> selections =new ArrayList<Integer>();//5,7
				
				
				List<Property> prop = t.get_Property();
				String index_name = "";
				for (int i = 0; i <prop.size(); i++){
//					System.out.println("[Selection]:"+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column()
//					+" eq "+prop.get(i).getTerm2().get_constant());
					index_name=alias_2_table_name.get(prop.get(i).getTerm1().get_variable())+"_"+prop.get(i).getTerm1().get_column();
					alias_2_index_name.put(prop.get(i).getTerm1().get_variable(), index_name);
					index_name="";
					alias = new Alias(0,"d0");//dummy alias
					selectionsList.add(prop.get(i).getTerm2().get_constant());
					
					columnIDs.add(alias_clumn_name_2_id.get(prop.get(i).getTerm1().get_variable()+"_"+prop.get(i).getTerm1().get_column().toLowerCase()));
					selections.add(prop.get(i).getTerm2().get_constant());
				}
				
				if(is_intersection_query==false)//selection operator goes to final operators
				{
					System.out.println("is_intersection_query:"+is_intersection_query);
					Operator selection_operator = new SelectionOperator(selectionsList, alias);
					operators.add(0, selection_operator);
				}
				else // else goes to intersection
				{
					System.out.println("is_intersection_query:"+is_intersection_query);
					Operator intersection_operator = new IntersectionOperator(bitwiseFlag, aliases_4_intersection, columnIDs, selections);
					operators.add(0, intersection_operator);
				}
			}
			//join
			if(t.get_Optype() == Optypes.JOIN_OPERATOR || t.get_Optype() == Optypes.SEMIJOIN_OPERATOR)
			{
				String index_name = "";
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					//System.out.println("[join/semi-join]:"+prop.get(i).getTerm2().get_variable()+"."+prop.get(i).getTerm2().get_column()
					//		+" eq "+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column());
					index_name=alias_2_table_name.get(prop.get(i).getTerm2().get_variable())+"_"+prop.get(i).getTerm2().get_column();
					alias_2_index_name.put(prop.get(i).getTerm2().get_variable(), index_name);
					index_name="";
				}
				number_of_joins++;
			}
			//intersection
			if(t.get_Optype() == Optypes.INTERSECTION_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					System.out.println("[intersection]:"+prop.get(i).getTerm2().get_variable()+"."+prop.get(i).getTerm2().get_column()
							+" eq "+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column());
					aliases_4_intersection.add(new Alias(i*2, prop.get(i).getTerm2().get_variable()));
					aliases_4_intersection.add(new Alias(i*2+1, prop.get(i).getTerm1().get_variable()));
				}
				is_intersection_query = true;
			}
					
			getIndexName(t.left);
			getIndexName(t.right);
		}
	}
	public MetaIndex getMetaIndexFromDisk(String path, String alias, int index_id)
	{
//		line 1: table name
//		line 2: lookup col name
//		line 3: num encodings (total cols -1)
//		line 4: column names
//		line 5: column domains
//		line 6: column min values
//		line 7: column byte sizes
//		line 8: column encoding flags
//		line 9: max fragment size
		String table_name="";
		String lookup_column_name="";
		String[] column_names, column_domains, column_mins, column_bytes, column_encodings;
		
		MetaIndex meta_index = null;
		int gqFastIndexID= index_id; //0
		int numColumns=0;
		long indexDomain=0;//lookup domain
		int indexMapByteSize=0;//lookup byte size
		int maxFragmentSize=0;
		
		List<Long> columnDomains = new ArrayList<Long>();//exclude lookup column
		List<Integer> columnEncodedByteSizesList =  new ArrayList<Integer>();//exclude lookup column
		List<Integer> columnEncodingsList = new ArrayList<Integer>();//exclude lookup column
		
		try
		{
			  File f = new File(path);
			  FileInputStream fr = new FileInputStream(f);
			  BufferedReader br = new BufferedReader(new InputStreamReader(fr,"UTF-8"));
			  String curr_line="";
			  table_name=br.readLine();//line 1
			  lookup_column_name = br.readLine();//line 2
			  numColumns =Integer.parseInt(br.readLine());//line 3
			  curr_line = br.readLine();//line 4
			  column_names = curr_line.split(",");
			  for(int i=0;i<column_names.length;i++)
			  {
				  alias_clumn_name_2_id.put(alias+"_"+column_names[i], i);
			  }
			  
			  curr_line = br.readLine();//line 5
			  column_domains = curr_line.split(",");
			  indexDomain = Long.parseLong(column_domains[0]);
			  for(int i=1;i<numColumns;i++)
			  {
				  columnDomains.add(Long.parseLong(column_domains[i]));
			  }
			  
			  curr_line = br.readLine();//line 6
			  column_mins = curr_line.split(",");
			  
			  
			  curr_line = br.readLine();//line 7
			  column_bytes = curr_line.split(",");
			  indexMapByteSize=Integer.parseInt(column_bytes[0]);
			  for(int i=1;i<numColumns;i++)
			  {
				  columnEncodedByteSizesList.add(Integer.parseInt(column_bytes[i]));
			  }
			  
			  curr_line = br.readLine();//line 8
			  column_encodings = curr_line.split(",");
			  for(int i=1;i<numColumns;i++)
			  {
				  columnEncodingsList.add(Integer.parseInt(column_encodings[i]));
			  }
			  
			  maxFragmentSize = Integer.parseInt( br.readLine());
			  
			
			  meta_index  = new MetaIndex(gqFastIndexID, numColumns, indexMapByteSize, indexDomain, maxFragmentSize, 
					  columnEncodingsList, columnEncodedByteSizesList, columnDomains);
			  
			  fr.close();
			  br.close();
		}
		catch(Exception e)
		{
			System.out.println("//// error reading ////////");
		}
		return meta_index;
	}
	
	public void getOperators(TreeNode t) {
		if (t != null) {
			//join
			if(t.get_Optype() == Optypes.JOIN_OPERATOR)
			{
				//for code generator
				boolean entityFlag = false; // ENTITY = 1; RELATIONSHIP = 0
				List<Integer> columnIDs;  //for SD query, 0 (for term)
				Alias alias; // (dt2)
				Alias drivingAlias; //(dt1)
				int drivingAliasColumn; //(term column id in dt1)
				
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					//get all the renames
					System.out.println("[join]:"+prop.get(i).getTerm2().get_variable()+"."+prop.get(i).getTerm2().get_column()
							+" eq "+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column());
				}
				
			}
			//semi-join
			if(t.get_Optype() == Optypes.SEMIJOIN_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					//get all the renames
					System.out.println("[semi-join]:"+prop.get(i).getTerm2().get_variable()+"."+prop.get(i).getTerm2().get_column()
							+" eq "+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column());
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
			//aggregation
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
			getOperators(t.left);
			getOperators(t.right);
		}
	}
}
