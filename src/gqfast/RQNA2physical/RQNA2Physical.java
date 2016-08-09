package gqfast.RQNA2physical;

import gqfast.codeGenerator.AggregationOperator;
import gqfast.codeGenerator.IntersectionOperator;
import gqfast.codeGenerator.JoinOperator;
import gqfast.codeGenerator.Operator;
import gqfast.codeGenerator.SelectionOperator;
import gqfast.codeGenerator.SemiJoinOperator;
import gqfast.codeGenerator.ThreadingOperator;
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
	//input of code generator (operators, metadata)
	public List<Operator> operators = new ArrayList<Operator>();
	public MetaData metadata = new MetaData();

	//alias mapping
	public HashMap<String, String> alias_2_table_name  = new HashMap<String, String>();//dt1->DT
	public HashMap<String, String> alias_2_index_name  = new HashMap<String, String>();//dt1->dt_doc
	public HashMap<String, Integer> alias_clumn_name_2_id = new HashMap<String, Integer>();
	public HashMap<String, Alias> alias_2_AliasClass = new HashMap<String, Alias>();
	HashMap<String, MetaIndex> alias_2_meta_index = new HashMap<String, MetaIndex>();
	HashMap<String, List<Integer>> alias_2_all_columns = new HashMap<String, List<Integer>>();
	List<Alias> aliases = new ArrayList<Alias>();
	
	
	
	public int number_of_joins = 0;
	public int number_of_operators = 0;
	public int current_operator_number = 0;
	public boolean has_intersection = false;//0: no intersection. 1: has intersection
	public boolean has_join = true;// 0: no join. 1: has join
	List<Alias> aliases_4_intersection = new ArrayList<Alias>(); //dt1 for each selection
	List<Integer> columnIDs_4_intersection = new ArrayList<Integer>();; //0: term column in dt1
	List<Integer> selections_4_intersection = new ArrayList<Integer>();;//5,7
	
//	List<Integer>
	public int global_alias_id = 0;
	public static void main(String[] args)
	{
		String query_name = "AD";
        TestTree_logical2RQNA test = new TestTree_logical2RQNA();
        test.TreeAD();
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
		
		//indexMap and meta_query are for meta-data
		HashMap<Integer, MetaIndex> indexMap = new HashMap<Integer, MetaIndex>();
		MetaQuery meta_query = null;
		
		//set MetaQuery
		int numThreads = 4;//fixed
		String queryName = query_name; 
		
		
		//set alias_2_table_name and alias_2_index_name
		System.out.println("Begin parsing all the aliases in the query...");
		getAlias2TableName(RQNA);
		getAlias2IndexName(RQNA);
		System.out.println("---------------------------------------------");
		System.out.println("alias 2 table_name:");
		for(String alias:alias_2_table_name.keySet())
		{
			System.out.println(alias+" --> "+alias_2_table_name.get(alias));
		}
		System.out.println("alias 2 index_name:");
		for(String alias:alias_2_index_name.keySet())
		{
			System.out.println(alias+" --> "+alias_2_index_name.get(alias));
		}
		System.out.println("---------------------------------------------");
		System.out.println("Finish parsing all the aliases in the query...");
		System.out.println("Begin analyzing query relevant metadata...");
		String path="";
		
		//set meta_index, alias_name_2_meta_index, aliases-list 
		MetaIndex meta_index=null;
		Alias alias_class = null;
		alias_class = new Alias(0, "d0" ); //I guess ben need this dummy alias, let me double check
		aliases.add(alias_class);
		int index_id=1;
		for(String alias:alias_2_index_name.keySet())
		{
			path="/home/ben/git/GQ-Fast-Project/src/gqfast/gqfast_loader/MetaData/meta_"+alias_2_index_name.get(alias).toLowerCase()+".gqfast";
			meta_index=getMetaIndexFromDisk(path, alias, index_id);
			alias_class = new Alias(index_id, alias, meta_index);
			aliases.add(alias_class);
			alias_2_AliasClass.put(alias, alias_class);
			indexMap.put(index_id, meta_index);//it is correct
			alias_2_meta_index.put(alias, meta_index);
			index_id++;
		}
		
		//set meta_query
		meta_query = new MetaQuery(queryName, numThreads,aliases);
		
		//set meta-data
		metadata.setIndexMap(indexMap);
		metadata.setQuery(meta_query);
		
		
		
		
		System.out.println("Finish analyzing query relevant metadata...");
		System.out.println("Begin generating physical operators...");
		System.out.println("/////////////////////////////////////");
		number_of_operators = number_of_joins+1+1; //not include aggregation here. +1 is for selection/intersection. +1 is for thread_operator
		System.out.println("---------------------------------------------");
		System.out.println("alias_column 2 id:");
		for(String alias_column: alias_clumn_name_2_id.keySet())
		{
			System.out.println(alias_column+" --> "+alias_clumn_name_2_id.get(alias_column));
		}
		System.out.println("---------------------------------------------");
		
		//set column_id;
		getColumnID4alias(RQNA);
		
		
		
		
		//set operators
		prepareSelectionOperator(RQNA);
		prepareIntersectionOperator(RQNA);
		prepareJoinOperator(RQNA);
		prepareAggregationOperator(RQNA);

		//set output
		output.setMetaData(metadata);
		output.setOperators(operators);
		return output;
	}
	
	//set to alias_2_table_name
	public void getAlias2TableName(TreeNode t) {
		if (t != null) {
			//rename
			if (t.get_Optype() == Optypes.RENAME_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					System.out.println("[rename]:"+prop.get(i).getTerm2().get_variable()+"->"+prop.get(i).getTerm1().get_variable());
//					get alias2tablename mapping
					alias_2_table_name.put(prop.get(i).getTerm1().get_variable(), prop.get(i).getTerm2().get_variable());
					List<Integer> columns_per_table = new ArrayList<Integer>();
					alias_2_all_columns.put(prop.get(i).getTerm1().get_variable(), columns_per_table);
				}
			}
			
			getAlias2TableName(t.left);
			getAlias2TableName(t.right);
		}
	}
	//set to alias_2_index_name
	public void getAlias2IndexName(TreeNode t) {
		if (t != null) {
			//selection
			if(t.get_Optype() == Optypes.SELECTION_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				String index_name = "";
				for (int i = 0; i <prop.size(); i++){
//					System.out.println("[Selection]:"+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column()
//					+" eq "+prop.get(i).getTerm2().get_constant());
					index_name=alias_2_table_name.get(prop.get(i).getTerm1().get_variable())+"_"+prop.get(i).getTerm1().get_column();
					alias_2_index_name.put(prop.get(i).getTerm1().get_variable(), index_name);
					index_name="";
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
				has_join = true;
			}
			//intersection
			if(t.get_Optype() == Optypes.INTERSECTION_OPERATOR)
			{
				has_intersection = true;
			}
			
			getAlias2IndexName(t.left);
			getAlias2IndexName(t.right);
		}
	}
	public void getColumnID4alias(TreeNode t)
	{
		if (t != null) {
			//selection
			if(t.get_Optype() == Optypes.SELECTION_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				String alias_name = "";
				alias_name=prop.get(0).getTerm1().get_variable();
				List<Integer> columns_in_one_table = alias_2_all_columns.get(alias_name);
				for (int i = 0; i <prop.size(); i++){
//					System.out.println("[Selection]:"+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column()
//					+" eq "+prop.get(i).getTerm2().get_constant());
					if(alias_clumn_name_2_id.get(alias_name+"_"+prop.get(i).getTerm1().get_column().toLowerCase())!=null)
					{
						columns_in_one_table.add(alias_clumn_name_2_id.get(alias_name+"_"+prop.get(i).getTerm1().get_column().toLowerCase()));
					}
				}
				
			}
			//projection
			if(t.get_Optype() == Optypes.PROJECTION_OPERATOR)
			{
				List<Term> terms = t.get_TermList();
				String alias_name = terms.get(0).get_variable();
				List<Integer> columns_in_one_table = alias_2_all_columns.get(alias_name);
				System.out.print("[Projection]");
				for (int i = 0; i < terms.size(); i++){
					System.out.print(terms.get(i).get_variable()+"."+terms.get(i).get_column()+";");
					if(alias_clumn_name_2_id.get(alias_name+"_"+terms.get(i).get_column().toLowerCase())!=null)
					{
						columns_in_one_table.add(alias_clumn_name_2_id.get(alias_name+"_"+terms.get(i).get_column().toLowerCase()));
					}
//					System.out.println("alias_name:"+alias_name+"-->"+alias_clumn_name_2_id.get(alias_name+"_"+terms.get(i).get_column().toLowerCase())+" , for column:"+alias_name+"_"+terms.get(i).get_column().toLowerCase());
				}
				System.out.println("");
			}
			getColumnID4alias(t.left);
			getColumnID4alias(t.right);
		}
	}
	public void prepareSelectionOperator(TreeNode t) {
		if (t != null) {
			//selection [deal selection first here, since selection after intersection in the tree, so selection should be done before intersection]
			if(t.get_Optype() == Optypes.SELECTION_OPERATOR)
			{
				
				
//				//for intersection_operator
//				boolean bitwiseFlag = false;
//				List<Integer> columnIDs = new ArrayList<Integer>(); //0: term column in dt1
//				List<Integer> selections =new ArrayList<Integer>();//5,7
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					System.out.println("[Selection]:"+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column()
					+" eq "+prop.get(i).getTerm2().get_constant());

					//for seelction_operator
					List<Integer> selectionsList = new ArrayList<Integer>(); //85
					selectionsList.add(prop.get(i).getTerm2().get_constant());
					Operator selection_operator = new SelectionOperator(selectionsList, aliases.get(0));
					String alias_name = prop.get(i).getTerm1().get_variable();
					if(has_intersection==false)//selection operator goes to final operators
					{
						//TODO: change it when there are multiple selections
						operators.add(0, selection_operator);
						current_operator_number = 1;
						if(has_join ==  true)
						{
							
							//for join_operator
							boolean entityFlag = false; // ENTITY = 1; RELATIONSHIP = 0
							Alias driving_alias = aliases.get(0); //(dt1)
							int driving_alias_column = 0; //(term column id in dt1)
							Alias join_alias = alias_2_AliasClass.get(alias_name); // (dt2)
							List<Integer> join_columnIDs = alias_2_all_columns.get(alias_name);
							
							Operator join_operator = new JoinOperator(entityFlag, join_columnIDs, join_alias, driving_alias, driving_alias_column) ;
							operators.add(1, join_operator);
							
							//threading operator
							Operator thread_operator = new ThreadingOperator(join_alias, false); 
							operators.add(2, thread_operator);
							current_operator_number = 3;
						}
					}
					else
					{
						aliases_4_intersection.add(alias_2_AliasClass.get(alias_name));
						columnIDs_4_intersection.add(alias_clumn_name_2_id.get(alias_name+"_"+prop.get(i).getTerm1().get_column().toLowerCase()));
						selections_4_intersection.add(prop.get(i).getTerm2().get_constant());
					}
				}
			}
			prepareSelectionOperator(t.left);
			prepareSelectionOperator(t.right);
		}
	}
	public void prepareIntersectionOperator(TreeNode t) {
		if (t != null) {
			//intersection
			if(t.get_Optype() == Optypes.INTERSECTION_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					System.out.println("[intersection]:"+prop.get(i).getTerm2().get_variable()+"."+prop.get(i).getTerm2().get_column()
							+" eq "+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column());
					Operator intersection_operator = new IntersectionOperator(false, aliases_4_intersection, 
							columnIDs_4_intersection, selections_4_intersection);
					operators.add(0,intersection_operator);
					
					//threading operator
					Operator thread_operator = new ThreadingOperator(alias_2_AliasClass.get(prop.get(i).getTerm1().get_variable()), false); 
					operators.add(1, thread_operator);
					current_operator_number = 2;
				}
			}
			prepareIntersectionOperator(t.left);
			prepareIntersectionOperator(t.right);
		}
	}
	public void prepareJoinOperator(TreeNode t) {
		if (t != null) {
			//join
			if(t.get_Optype() == Optypes.JOIN_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					System.out.println("[join]:"+prop.get(i).getTerm2().get_variable()+"."+prop.get(i).getTerm2().get_column()
							+" eq "+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column());
					
					String previous_alias_name = prop.get(i).getTerm1().get_variable();
					String current_alias_name = prop.get(i).getTerm2().get_variable();
					//for join_operator
					boolean entityFlag = false; // ENTITY = 1; RELATIONSHIP = 0
					Alias driving_alias = alias_2_AliasClass.get(previous_alias_name); //(dt1)
					int driving_alias_column = alias_clumn_name_2_id.get(previous_alias_name+"_"+prop.get(i).getTerm1().get_column().toLowerCase()); //(term column id in dt1)
					Alias join_alias = alias_2_AliasClass.get(current_alias_name); // (dt2)
					List<Integer> join_columnIDs = alias_2_all_columns.get(current_alias_name);
					
					Operator join_operator = new JoinOperator(entityFlag, join_columnIDs, join_alias, driving_alias, driving_alias_column) ;
					operators.add(current_operator_number++, join_operator);
				}
			}
			//semi-join
			if(t.get_Optype() == Optypes.SEMIJOIN_OPERATOR)
			{
				List<Property> prop = t.get_Property();
				for (int i = 0; i <prop.size(); i++){
					System.out.println("[semi-join]:"+prop.get(i).getTerm2().get_variable()+"."+prop.get(i).getTerm2().get_column()
							+" eq "+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column());
					
					String previous_alias_name = prop.get(i).getTerm1().get_variable();
					String current_alias_name = prop.get(i).getTerm2().get_variable();
					//for join_operator
					boolean entityFlag = false; // ENTITY = 1; RELATIONSHIP = 0
					Alias driving_alias = alias_2_AliasClass.get(previous_alias_name); //(dt1)
					int driving_alias_column = alias_clumn_name_2_id.get(previous_alias_name+"_"+prop.get(i).getTerm1().get_column().toLowerCase()); //(term column id in dt1)
					Alias join_alias = alias_2_AliasClass.get(current_alias_name); // (dt2)
					List<Integer> join_columnIDs = alias_2_all_columns.get(current_alias_name);
					
					Operator semi_join_operator = new SemiJoinOperator(entityFlag, join_columnIDs, join_alias, driving_alias, driving_alias_column) ;
					operators.add(current_operator_number++, semi_join_operator);
				}
			}
			prepareJoinOperator(t.left);
			prepareJoinOperator(t.right);
		}
	}
	public void prepareAggregationOperator(TreeNode t) {
		if (t != null) {
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
			prepareAggregationOperator(t.left);
			prepareAggregationOperator(t.right);
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
			  for(int i=1;i<column_names.length;i++)
			  {
				  alias_clumn_name_2_id.put(alias+"_"+column_names[i].toLowerCase(), i-1);
				  System.out.println("[alias_clumn_name_2_id]:"+alias+"_"+column_names[i]+","+(i-1));
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
			System.err.println("//// error reading ////////");
		}
		return meta_index;
	}
	
}
