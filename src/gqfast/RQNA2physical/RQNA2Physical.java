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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
	public HashMap<String,Long> alias_clumn_name_2_domain_size  = new HashMap<String, Long>();//dt1.doc = 50001
	public HashMap<String, Alias> alias_2_AliasClass = new HashMap<String, Alias>();
	HashMap<String, MetaIndex> alias_2_meta_index = new HashMap<String, MetaIndex>();
	HashMap<String, List<Integer>> alias_2_all_columns = new HashMap<String, List<Integer>>();
	List<Alias> aliases = new ArrayList<Alias>();
	public HashMap<String, Integer> alias_2_table_type  = new HashMap<String, Integer>();//dt1->0; d->1
	public boolean has_entity_table = false; 
	public boolean is_join_operator_changed = false;
	public long aggregation_domain_size = 0;
	
	public int number_of_operators = 0;
	public int number_of_joins = 0;
	public int current_operator_number = 0;
	public int current_join_operator_number = 0;
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
		String setting_path = "GQFast/MetaData/"+ query_name +".setting";
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
			path="GQFast/MetaData/meta_"+alias_2_index_name.get(alias).toLowerCase()+".gqfast";
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
		
		System.out.println("---------------------------------------------");
		System.out.println("entity table, relationship table checking:");
		for(String alias: alias_2_table_type.keySet())
		{
			System.out.println(alias+" --> "+alias_2_table_type.get(alias));
		}
		System.out.println("---------------------------------------------");
		
		
		
		System.out.println("Finish analyzing query relevant metadata...");
		System.out.println("Begin generating physical operators...");
		System.out.println("/////////////////////////////////////");
		
		System.out.println("---------------------------------------------");
		System.out.println("alias_column 2 id:");
		for(String alias_column: alias_clumn_name_2_id.keySet())
		{
			System.out.println(alias_column+" --> "+alias_clumn_name_2_id.get(alias_column));
		}
		System.out.println("---------------------------------------------");
		
		//set query-related column ids;
		getColumnID4alias(RQNA);
		
		System.out.println("---------------------------------------------");
		System.out.println("alias 2 all needed ids:");
		List<Integer> columns_in_one_table = null;
		for(String alias: alias_2_all_columns.keySet())
		{
			columns_in_one_table = alias_2_all_columns.get(alias);
			System.out.print(alias+" --> ");
			for(int i=0;i<columns_in_one_table.size();i++)
			{
				System.out.print(columns_in_one_table.get(i)+",");
			}
			System.out.println("");
		}
		System.out.println("---------------------------------------------");
		
		//calcualte number of operators
		if(has_intersection==true)//has intersection
		{
			number_of_operators = number_of_joins +3;//1 for intersection, 1 for threading, 1 for aggregation
		}
		else
		{
			number_of_operators = number_of_joins+4;////1 for selection, 1 for dummy join, 1 for threading, 1 for aggregation
		}
		for(int i=0;i<number_of_operators;i++)
		{
			operators.add(new SelectionOperator(null, null));
		}
		
		
		
		//set operators
		prepareSelectionOperator(RQNA);
		if(has_intersection==true)//has intersection
		{
			prepareIntersectionOperator(RQNA);
		}
		if(has_join ==  true)//has join
		{
			prepareJoinOperator(RQNA);
		}
		prepareAggregationOperator(RQNA);

		metadata.setAggregationDomain(aggregation_domain_size);
		//set output
		output.setMetaData(metadata);
		output.setOperators(operators);
		
		//write setting file
		System.out.println("///////////////////////////////////");
		BufferedWriter out = null;    
		try 
		{                                                                        
        	out = new BufferedWriter(new OutputStreamWriter( 
        		new FileOutputStream(setting_path, true)));                              
        	out.write(alias_2_index_name.size()+"");
        	out.newLine();
        	out.write(aggregation_domain_size+"");
        	out.newLine();
        	for(String alias:alias_2_index_name.keySet())
    		{
        		out.write(alias_2_index_name.get(alias).toLowerCase());
        		out.newLine();
    		}
        	out.flush();
      	   	out.close();
      	 }
		catch (Exception e) 
        {                                                     
            e.printStackTrace();                                                    
        }
		
		System.out.println("# of indices:"+alias_2_index_name.size());
		System.out.println("aggregation_domain_size:"+aggregation_domain_size);
		for(String alias:alias_2_index_name.keySet())
		{
			System.out.println(alias_2_index_name.get(alias).toLowerCase());
		}
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
				String column_name = "";
				int column_id= 0;
				List<Property> prop = t.get_Property();
				String alias_name = "";
				alias_name=prop.get(0).getTerm1().get_variable();
				List<Integer> columns_in_one_table = alias_2_all_columns.get(alias_name);
				for (int i = 0; i <prop.size(); i++){
//					System.out.println("[Selection]:"+prop.get(i).getTerm1().get_variable()+"."+prop.get(i).getTerm1().get_column()
//					+" eq "+prop.get(i).getTerm2().get_constant());
					column_name = prop.get(i).getTerm1().get_column().toLowerCase();
					if(alias_clumn_name_2_id.get(alias_name+"_"+column_name)!=null)
					{
						column_id = alias_clumn_name_2_id.get(alias_name+"_"+column_name);
						if(!columns_in_one_table.contains(column_id))
						{
							columns_in_one_table.add(column_id);
						}
					}
				}
				
			}
			//projection
			if(t.get_Optype() == Optypes.PROJECTION_OPERATOR)
			{
				List<Term> terms = t.get_TermList();
				String alias_name = terms.get(0).get_variable();
				String column_name = "";
				int column_id= 0;
				List<Integer> columns_in_one_table = alias_2_all_columns.get(alias_name);
				if(alias_2_table_type.get(alias_name)==1)//entity table, add the column id for the dummy column
				{
					columns_in_one_table.add(0);
				}
				System.out.print("[Projection]");
				for (int i = 0; i < terms.size(); i++){
					System.out.print(terms.get(i).get_variable()+"."+terms.get(i).get_column()+";");
					column_name = terms.get(i).get_column().toLowerCase();
					if(alias_clumn_name_2_id.get(alias_name+"_"+column_name)!=null)
					{
						column_id = alias_clumn_name_2_id.get(alias_name+"_"+column_name);
						if(!columns_in_one_table.contains(column_id))
						{
							columns_in_one_table.add(column_id);
						}
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
					
					if(has_join ==  true)
					{
						//threading operator
						Operator thread_operator = new ThreadingOperator(alias_2_AliasClass.get(prop.get(i).getTerm1().get_variable()), false); 
						operators.add(1, thread_operator);
						current_operator_number = 2;
					}
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
					Alias driving_alias = alias_2_AliasClass.get(previous_alias_name); //(dt1)
					int driving_alias_column= 0;
					if(alias_2_table_type.get(previous_alias_name) == 1 && is_join_operator_changed == false) //entity table check
					{
						driving_alias_column= 0;//the column id of the dummy column
						is_join_operator_changed = true;
					}
					else
					{
						driving_alias_column = alias_clumn_name_2_id.get(previous_alias_name+"_"+prop.get(i).getTerm1().get_column().toLowerCase()); //(term column id in dt1)
					}
					
					
					String current_alias_name = prop.get(i).getTerm2().get_variable();
					//for join_operator
					boolean entityFlag = false; // ENTITY = 1; RELATIONSHIP = 0
					Alias join_alias = alias_2_AliasClass.get(current_alias_name); // (dt2)
					List<Integer> join_columnIDs = alias_2_all_columns.get(current_alias_name);
					
					Operator join_operator = new JoinOperator(entityFlag, join_columnIDs, join_alias, driving_alias, driving_alias_column) ;
					operators.add(current_operator_number+number_of_joins-current_join_operator_number-1, join_operator);
					current_join_operator_number++;
					
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
					operators.add(current_operator_number+number_of_joins-current_join_operator_number-1, semi_join_operator);
					current_join_operator_number++;
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

				int AGGREGATION_INT = 1, AGGREGATION_DOUBLE = 2;
				int FUNCTION_COUNT = 1, FUNCTION_SUM = 2;
				
				int dataType = AGGREGATION_INT; // Always INT for now	
				int aggregationFunction =FUNCTION_COUNT; // COUNT or SUM for now
				String alias_name = "", column_name = "";
				String sum_alias = "";
				String sum_column = "";
				
				for (int i = 0; i < aggr_term.size(); i++){
					System.out.print("[aggregation]"+aggr_term.get(i).get_variable()+"."+aggr_term.get(i).get_column()+";");
					alias_name = aggr_term.get(i).get_variable();
					column_name= aggr_term.get(i).get_column().toLowerCase();
				}
				for (int i = 0; i < aggr.size(); i++){
					System.out.println(aggr.get(i)+",");
					if(aggr.get(i).toLowerCase().indexOf("sum")!=-1)//sum
					{
						aggregationFunction = FUNCTION_SUM;
						//since now we only support one aggregation column inside function
						String b = aggr.get(i).substring(aggr.get(i).indexOf("(")+1, aggr.get(i).indexOf(")"));
						if(b.indexOf("*")!=-1)
						{
							b = b.substring(0, b.indexOf("*"));
						}
						if(b.indexOf("+")!=-1)
						{
							b = b.substring(0, b.indexOf("+"));
						}
						if(b.indexOf("-")!=-1)
						{
							b = b.substring(0, b.indexOf("-"));
						}
						
						sum_alias = b.substring(0,b.indexOf("."));
						sum_column = b.substring(b.indexOf(".")+1).toLowerCase();
//						System.out.println("sum_alias:"+sum_alias+" , sum_column:"+sum_column);
					}
					else if(aggr.get(i).toLowerCase().indexOf("count")!=-1)
					{
						aggregationFunction = FUNCTION_COUNT;
					}
				}
				
				Alias drivingAlias = alias_2_AliasClass.get(alias_name); // This should be the alias driving the 'for' loop of the aggregation 
				int drivingAliasColumn = alias_clumn_name_2_id.get(alias_name+"_"+column_name); // Alias column
				System.out.println("aggregation_size:"+(alias_name+"_"+column_name));
				aggregation_domain_size = alias_clumn_name_2_domain_size.get(alias_name+"_"+column_name);
				
				Alias aggregationAlias = null;
				int aggregationAliasColumn = 0;
				if(aggregationFunction ==  FUNCTION_COUNT)
				{
					aggregationAlias = null;
					aggregationAliasColumn = 0;
				}
				else if(aggregationFunction ==  FUNCTION_SUM)
				{
					aggregationAlias=alias_2_AliasClass.get(sum_alias); // For SUM only: from what alias are we summing up?
					aggregationAliasColumn = alias_clumn_name_2_id.get(sum_alias+"_"+sum_column); //  For SUM only: from which column?
				}
				
				
				 
				Operator aggregation_operator = new AggregationOperator( dataType,  aggregationFunction,  drivingAlias, 
						 drivingAliasColumn,  aggregationAlias,  aggregationAliasColumn);
				operators.add(current_operator_number+number_of_joins,aggregation_operator);
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
		int table_type = 0; //0: relationship table, 1: entity table
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
			  System.out.println("========> number of columns:" +numColumns);
			  for(int i=0;i<column_names.length;i++)
			  {
				  System.out.println("========>[domain_size_setting]"+alias+"_"+column_names[i].toLowerCase()+"===>"+ Long.parseLong(column_domains[i]));
				  alias_clumn_name_2_domain_size.put(alias+"_"+column_names[i].toLowerCase(), Long.parseLong(column_domains[i]));
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
			  
			  maxFragmentSize = Integer.parseInt( br.readLine());//line 9
			  
			  table_type = Integer.parseInt( br.readLine()); //line 10
			  alias_2_table_type.put(alias, table_type);
			  if(table_type == 1)
			  {
				  has_entity_table = true;
			  }
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
