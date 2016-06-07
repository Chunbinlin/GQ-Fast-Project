package gqfast.codeGenerator.testcases;

import gqfast.codeGenerator.AggregationOperator;
import gqfast.codeGenerator.CodeGenerator;
import gqfast.codeGenerator.IntersectionOperator;
import gqfast.codeGenerator.JoinOperator;
import gqfast.codeGenerator.Operator;
import gqfast.codeGenerator.ThreadingOperator;
import gqfast.global.Alias;
import gqfast.global.MetaData;
import gqfast.global.MetaIndex;
import gqfast.global.MetaQuery;

import java.util.ArrayList;
import java.util.List;

public class PubmedQueryFAD {
	private static void initQ4Queries(MetaData metadata, String queryName, int numThreads) {

		List<Alias> aliases = new ArrayList<Alias>();
		
		Alias alias0 = new Alias(0, "doc1", metadata.getIndexList().get(0));
		Alias alias1 = new Alias(1, "term1", metadata.getIndexList().get(1));
		
		aliases.add(alias0);
		aliases.add(alias1);
		
		// public MetaQuery(int queryID, String queryName, int numThreads,
		// int numBuffers, int bufferPoolSize, List<String> aliases)
		MetaQuery q4Optimal = new MetaQuery(0, queryName, numThreads, 2, aliases);
				
		metadata.getQueryList().add(q4Optimal);
		metadata.setCurrentQueryID(metadata.getQueryList().size()-1);
		
	}
	
	private static void initQ4Indexes(MetaData metadata, int encodingType) {
		
		// DT2
		int indexID = 3;
		int numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(encodingType);
		columnEncodingsList3.add(encodingType);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(0, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().add(DT2);
		
		// DT1
		indexID = 2;
		numColumns = 2;
		List<Integer> columnEncodingsList2 = new ArrayList<Integer>();
		columnEncodingsList2.add(encodingType);
		columnEncodingsList2.add(encodingType);
		
		List<Integer >columnEncodedByteSizesList2 = new ArrayList<Integer>();
		columnEncodedByteSizesList2.add(MetaData.BYTES_4);
		columnEncodedByteSizesList2.add(MetaData.BYTES_1);
		
		MetaIndex DT1 = new MetaIndex(1, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList2, columnEncodedByteSizesList2);
		metadata.getIndexList().add(DT1);
		

	}
	
	
	private static void initQ4Indexes(MetaData metadata) {

		// DT2
		int indexID = 3;
		int numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(MetaData.ENCODING_BB);
		columnEncodingsList3.add(MetaData.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(0, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().add(DT2);
		
		
		// DT1
		indexID = 2;
		numColumns = 2;
		List<Integer> columnEncodingsList2 = new ArrayList<Integer>();
		columnEncodingsList2.add(MetaData.ENCODING_BB);
		columnEncodingsList2.add(MetaData.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList2 = new ArrayList<Integer>();
		columnEncodedByteSizesList2.add(MetaData.BYTES_4);
		columnEncodedByteSizesList2.add(MetaData.BYTES_1);
		
		MetaIndex DT1 = new MetaIndex(1, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList2, columnEncodedByteSizesList2);
		metadata.getIndexList().add(DT1);

	}
	private static void initQ4Operators(List<Operator> operators, MetaQuery query, List<Integer> selections) {
	
		List<Alias> aliases = query.getAliases();
	
		
		List<Alias> intersectionAliases = new ArrayList<Alias>();
		intersectionAliases.add(aliases.get(0));
		intersectionAliases.add(aliases.get(0));
		
		List<Integer> columnIDs = new ArrayList<Integer>();
		columnIDs.add(0);
		columnIDs.add(0);
		
		//public IntersectionOperator(boolean bitwiseFlag, List<Alias> aliases, 
			//	List<Integer> columnIDs, List<Integer> selections) {
		Operator intersectionOp = new IntersectionOperator(false, intersectionAliases, columnIDs, selections);
		operators.add(intersectionOp);	
		
		List<Integer> column1IDs = new ArrayList<Integer>();
		column1IDs.add(0);
		column1IDs.add(1);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join1 = new JoinOperator(false, column1IDs, aliases.get(1), aliases.get(0), 0);
		
		operators.add(join1);
			
		int aggregationindexID = 2;
		
		String aggString = "op0";
		
		List<Alias> aggAliasList = new ArrayList<Alias>();
		aggAliasList.add(query.getAliases().get(1));
				
		List<Integer> aggOpColList = new ArrayList<Integer>();
		aggOpColList.add(1);
		
		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_INT, aggString, aggAliasList, aggOpColList, aliases.get(1), 0);
	
		operators.add(agg);
	}
	
	private static void initQ4OperatorsThreaded(List<Operator> operators, MetaQuery query, List<Integer> selections) {
		List<Alias> aliases = query.getAliases();
	
		
		List<Alias> intersectionAliases = new ArrayList<Alias>();
		intersectionAliases.add(aliases.get(0));
		intersectionAliases.add(aliases.get(0));
		
		List<Integer> columnIDs = new ArrayList<Integer>();
		columnIDs.add(0);
		columnIDs.add(0);
		
		//public IntersectionOperator(boolean bitwiseFlag, List<Alias> aliases, 
			//	List<Integer> columnIDs, List<Integer> selections) {
		Operator intersectionOp = new IntersectionOperator(false, intersectionAliases, columnIDs, selections);
		operators.add(intersectionOp);	
		
		Operator threadingOp = new ThreadingOperator(aliases.get(0), true);
		operators.add(threadingOp);
		
		List<Integer> column1IDs = new ArrayList<Integer>();
		column1IDs.add(0);
		column1IDs.add(1);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join1 = new JoinOperator(false, column1IDs, aliases.get(1), aliases.get(0), 0);
		
		operators.add(join1);
			
		int aggregationindexID = 2;
		
		String aggString = "op0";
		
		List<Alias> aggAliasList = new ArrayList<Alias>();
		aggAliasList.add(query.getAliases().get(1));
				
		List<Integer> aggOpColList = new ArrayList<Integer>();
		aggOpColList.add(1);
		
		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_INT, aggString, aggAliasList, aggOpColList, aliases.get(1), 0);
	
		operators.add(agg);
	
	}
	
	private static void runQ4(String queryName, int numThreads, List<Integer> selections, int encoding) {
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		
		initQ4Indexes(metadata, encoding);
		initQ4Queries(metadata, queryName, numThreads);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());
		
		
		if (numThreads > 1) {
			initQ4OperatorsThreaded(operators, query, selections);
		}
		else {
			initQ4Operators(operators, query, selections);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	

	private static void runQ4(String queryName, int numThreads, List<Integer> selections, boolean b) {
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		initQ4Indexes(metadata);
		initQ4Queries(metadata, queryName, numThreads);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());
		if (numThreads > 1) {
			initQ4OperatorsThreaded(operators, query, selections);
		}
		else {
			initQ4Operators(operators, query, selections);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	
	public static void main(String[] args) {

		List<Integer> selections1 = new ArrayList<Integer>();
		selections1.add(90725);
		selections1.add(1570);
		
		List<Integer> selections2 = new ArrayList<Integer>();
		selections2.add(90725);
		selections2.add(2584);
		
		List<Integer> selections3 = new ArrayList<Integer>();
		selections3.add(1570);
		selections3.add(2584);
		
		List<Integer> selections4 = new ArrayList<Integer>();
		selections4.add(4446);
		selections4.add(535);
		
		List<Integer> selections5 = new ArrayList<Integer>();
		selections5.add(4446);
		selections5.add(1478);
		
		List<Integer> selections6 = new ArrayList<Integer>();
		selections6.add(535);
		selections6.add(1478);
		
		List<Integer> selections7 = new ArrayList<Integer>();
		selections7.add(7608);
		selections7.add(3646);
		//Q3 Mesh Optimal
		runQ4("q4_mesh_opt_0threads_90725_1570", 1, selections1, true);
		runQ4("q4_mesh_opt_2threads_90725_1570", 2, selections1, true);
		runQ4("q4_mesh_opt_4threads_90725_1570", 4, selections1, true);
		runQ4("q4_mesh_opt_1threads_90725_1570", 10, selections1, true);
		
		runQ4("q4_mesh_opt_0threads_90725_2584", 1, selections2, true);
		runQ4("q4_mesh_opt_2threads_90725_2584", 2, selections2, true);
		runQ4("q4_mesh_opt_4threads_90725_2584", 4, selections2, true);
		runQ4("q4_mesh_opt_1threads_90725_2584", 10, selections2, true);
		
		runQ4("q4_mesh_opt_0threads_1570_2584", 1, selections3, true);
		runQ4("q4_mesh_opt_2threads_1570_2584", 2, selections3, true);
		runQ4("q4_mesh_opt_4threads_1570_2584", 4, selections3, true);
		runQ4("q4_mesh_opt_1threads_1570_2584", 10, selections3, true);
		
		runQ4("q4_mesh_opt_0threads_4446_535", 1, selections4, true);
		runQ4("q4_mesh_opt_2threads_4446_535", 2, selections4, true);
		runQ4("q4_mesh_opt_4threads_4446_535", 4, selections4, true);
		runQ4("q4_mesh_opt_1threads_4446_535", 10, selections4, true);
		
		runQ4("q4_mesh_opt_0threads_4446_1478", 1, selections5, true);
		runQ4("q4_mesh_opt_2threads_4446_1478", 2, selections5, true);
		runQ4("q4_mesh_opt_4threads_4446_1478", 4, selections5, true);
		runQ4("q4_mesh_opt_1threads_4446_1478", 10, selections5, true);
		
		runQ4("q4_mesh_opt_0threads_535_1478", 1, selections6, true);
		runQ4("q4_mesh_opt_2threads_535_1478", 2, selections6, true);
		runQ4("q4_mesh_opt_4threads_535_1478", 4, selections6, true);
		runQ4("q4_mesh_opt_1threads_535_1478", 10, selections6, true);
		
		runQ4("q4_mesh_opt_0threads_7608_3646", 1, selections7, true);
		runQ4("q4_mesh_opt_2threads_7608_3646", 2, selections7, true);
		runQ4("q4_mesh_opt_4threads_7608_3646", 4, selections7, true);
		runQ4("q4_mesh_opt_1threads_7608_3646", 10, selections7, true);
		
		// Q3 Mesh Array
		runQ4("q4_mesh_array_0threads_90725_1570", 1, selections1, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_2threads_90725_1570", 2, selections1, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_4threads_90725_1570", 4, selections1, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_1threads_90725_1570", 10, selections1, MetaData.ENCODING_UA);
		
		runQ4("q4_mesh_array_0threads_90725_2584", 1, selections2, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_2threads_90725_2584", 2, selections2, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_4threads_90725_2584", 4, selections2, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_1threads_90725_2584", 10, selections2, MetaData.ENCODING_UA);
		
		runQ4("q4_mesh_array_0threads_1570_2584", 1, selections3, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_2threads_1570_2584", 2, selections3, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_4threads_1570_2584", 4, selections3, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_1threads_1570_2584", 10, selections3, MetaData.ENCODING_UA);
		
		runQ4("q4_mesh_array_0threads_4446_535", 1, selections4, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_2threads_4446_535", 2, selections4, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_4threads_4446_535", 4, selections4, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_1threads_4446_535", 10, selections4, MetaData.ENCODING_UA);
		
		runQ4("q4_mesh_array_0threads_4446_1478", 1, selections5, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_2threads_4446_1478", 2, selections5, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_4threads_4446_1478", 4, selections5, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_1threads_4446_1478", 10, selections5, MetaData.ENCODING_UA);
		
		runQ4("q4_mesh_array_0threads_535_1478", 1, selections6, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_2threads_535_1478", 2, selections6, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_4threads_535_1478", 4, selections6, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_1threads_535_1478", 10, selections6, MetaData.ENCODING_UA);
		
		runQ4("q4_mesh_array_0threads_7608_3646", 1, selections7, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_2threads_7608_3646", 2, selections7, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_4threads_7608_3646", 4, selections7, MetaData.ENCODING_UA);
		runQ4("q4_mesh_array_1threads_7608_3646", 10, selections7, MetaData.ENCODING_UA);
		
		// Mesh+Supp.
		
		selections1 = new ArrayList<Integer>();
		selections1.add(879);
		selections1.add(254);
		
		selections2 = new ArrayList<Integer>();
		selections2.add(879);
		selections2.add(7041);
		
		selections3 = new ArrayList<Integer>();
		selections3.add(254);
		selections3.add(7041);
		
		selections4 = new ArrayList<Integer>();
		selections4.add(1412);
		selections4.add(10350);
		
		selections5 = new ArrayList<Integer>();
		selections5.add(10350);
		selections5.add(17630);
		
		selections6 = new ArrayList<Integer>();
		selections6.add(231);
		selections6.add(4366);
		
		selections7 = new ArrayList<Integer>();
		selections7.add(1130);
		selections7.add(2994);
		
		
		//Q3 Mesh+Supp Optimal
		runQ4("q4_tag_opt_0threads_879_254", 1, selections1, true);
		runQ4("q4_tag_opt_2threads_879_254", 2, selections1, true);
		runQ4("q4_tag_opt_4threads_879_254", 4, selections1, true);
		runQ4("q4_tag_opt_1threads_879_254", 10, selections1, true);
		
		runQ4("q4_tag_opt_0threads_879_7041", 1, selections2, true);
		runQ4("q4_tag_opt_2threads_879_7041", 2, selections2, true);
		runQ4("q4_tag_opt_4threads_879_7041", 4, selections2, true);
		runQ4("q4_tag_opt_1threads_879_7041", 10, selections2, true);
		
		runQ4("q4_tag_opt_0threads_254_7041", 1, selections3, true);
		runQ4("q4_tag_opt_2threads_254_7041", 2, selections3, true);
		runQ4("q4_tag_opt_4threads_254_7041", 4, selections3, true);
		runQ4("q4_tag_opt_1threads_254_7041", 10, selections3, true);
		
		runQ4("q4_tag_opt_0threads_1412_10350", 1, selections4, true);
		runQ4("q4_tag_opt_2threads_1412_10350", 2, selections4, true);
		runQ4("q4_tag_opt_4threads_1412_10350", 4, selections4, true);
		runQ4("q4_tag_opt_1threads_1412_10350", 10, selections4, true);
		
		runQ4("q4_tag_opt_0threads_10350_17630", 1, selections5, true);
		runQ4("q4_tag_opt_2threads_10350_17630", 2, selections5, true);
		runQ4("q4_tag_opt_4threads_10350_17630", 4, selections5, true);
		runQ4("q4_tag_opt_1threads_10350_17630", 10, selections5, true);
		
		runQ4("q4_tag_opt_0threads_231_4366", 1, selections6, true);
		runQ4("q4_tag_opt_2threads_231_4366", 2, selections6, true);
		runQ4("q4_tag_opt_4threads_231_4366", 4, selections6, true);
		runQ4("q4_tag_opt_1threads_231_4366", 10, selections6, true);
		
		runQ4("q4_tag_opt_0threads_1130_2994", 1, selections7, true);
		runQ4("q4_tag_opt_2threads_1330_2994", 2, selections7, true);
		runQ4("q4_tag_opt_4threads_1330_2994", 4, selections7, true);
		runQ4("q4_tag_opt_1threads_1130_2994", 10, selections7, true);
		
		// Q3 Mesh+Supp Array
		runQ4("q4_tag_array_0threads_879_254", 1, selections1, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_2threads_879_254", 2, selections1, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_4threads_879_254", 4, selections1, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_1threads_879_254", 10, selections1, MetaData.ENCODING_UA);
		
		runQ4("q4_tag_array_0threads_879_7041", 1, selections2, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_2threads_879_7041", 2, selections2, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_4threads_879_7041", 4, selections2, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_1threads_879_7041", 10, selections2, MetaData.ENCODING_UA);
		
		runQ4("q4_tag_array_0threads_254_7041", 1, selections3, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_2threads_254_7041", 2, selections3, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_4threads_254_7041", 4, selections3, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_1threads_254_7041", 10, selections3, MetaData.ENCODING_UA);
		
		runQ4("q4_tag_array_0threads_1412_10350", 1, selections4, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_2threads_1412_10350", 2, selections4, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_4threads_1412_10350", 4, selections4, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_1threads_1412_10350", 10, selections4, MetaData.ENCODING_UA);
		
		runQ4("q4_tag_array_0threads_10350_17630", 1, selections5, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_2threads_10350_17630", 2, selections5, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_4threads_10350_17630", 4, selections5, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_1threads_10350_17630", 10, selections5, MetaData.ENCODING_UA);
		
		runQ4("q4_tag_array_0threads_231_4366", 1, selections6, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_2threads_231_4366", 2, selections6, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_4threads_231_4366", 4, selections6, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_1threads_231_4366", 10, selections6, MetaData.ENCODING_UA);
		
		runQ4("q4_tag_array_0threads_1130_2994", 1, selections7, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_2threads_1330_2994", 2, selections7, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_4threads_1330_2994", 4, selections7, MetaData.ENCODING_UA);
		runQ4("q4_tag_array_1threads_1130_2994", 10, selections7, MetaData.ENCODING_UA);
		
		
		
	}
	
	
}
