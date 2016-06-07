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
		List<Integer> selections = new ArrayList<Integer>();
		selections.add(1);
		selections.add(2);
		//Q2 Optimal
		runQ4("test_pubmed_q4_opt", 1, selections, true);
		runQ4("test_pubmed_q4_opt_threaded", 4, selections, true);
		
		// Q2 UA
		runQ4("test_pubmed_q4_array", 1, selections, MetaData.ENCODING_UA);
		runQ4("test_pubmed_q4_array_threaded", 4, selections, MetaData.ENCODING_UA);
		
		
		// Q2 BCA
		runQ4("test_pubmed_q4_bca", 1, selections, MetaData.ENCODING_BCA);
		runQ4("test_pubmed_q4_bca_threaded", 4, selections, MetaData.ENCODING_BCA);
				
		// Q2 Huffman
		runQ4("test_pubmed_q4_huffman", 1, selections, MetaData.ENCODING_HUFFMAN);
		runQ4("test_pubmed_q4_huffman_threaded", 4, selections, MetaData.ENCODING_HUFFMAN);
		
		
		
	}
	
	
}
