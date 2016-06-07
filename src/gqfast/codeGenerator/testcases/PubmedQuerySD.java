package gqfast.codeGenerator.testcases;

import gqfast.codeGenerator.AggregationOperator;
import gqfast.codeGenerator.CodeGenerator;
import gqfast.codeGenerator.JoinOperator;
import gqfast.codeGenerator.Operator;
import gqfast.codeGenerator.SelectionOperator;
import gqfast.codeGenerator.ThreadingOperator;
import gqfast.global.Alias;
import gqfast.global.MetaData;
import gqfast.global.MetaIndex;
import gqfast.global.MetaQuery;

import java.util.ArrayList;
import java.util.List;

public class PubmedQuerySD {


	private static void initQ1Queries(MetaData metadata, String queryName, int numThreads) {

		List<Alias> aliases = new ArrayList<Alias>();
		
		Alias alias0 = new Alias(0, "doc1");
		Alias alias1 = new Alias(1, "term1", metadata.getIndexList().get(0));
		Alias alias2 = new Alias(2, "doc2", metadata.getIndexList().get(1));
		
		aliases.add(alias0);
		aliases.add(alias1);
		aliases.add(alias2);
		
		// public MetaQuery(int queryID, String queryName, int numThreads,
		// int numBuffers, int bufferPoolSize, List<String> aliases)
		MetaQuery q2Optimal = new MetaQuery(0, queryName, numThreads, 1, aliases);
				
		metadata.getQueryList().add(q2Optimal);
		metadata.setCurrentQueryID(metadata.getQueryList().size()-1);
		
	}
	
	private static void initQ1Indexes(MetaData metadata, int encodingType) {

		// DT1
		int indexID = 2;
		int numColumns = 2;
		List<Integer> columnEncodingsList2 = new ArrayList<Integer>();
		columnEncodingsList2.add(encodingType);
		columnEncodingsList2.add(encodingType);
		
		List<Integer >columnEncodedByteSizesList2 = new ArrayList<Integer>();
		columnEncodedByteSizesList2.add(MetaData.BYTES_4);
		columnEncodedByteSizesList2.add(MetaData.BYTES_1);
		
		MetaIndex DT1 = new MetaIndex(0, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList2, columnEncodedByteSizesList2);
		metadata.getIndexList().add(DT1);
		
		// DT2
		indexID = 3;
		numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(encodingType);
		columnEncodingsList3.add(encodingType);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(1, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().add(DT2);
		

	}
	
	
	private static void initQ1Indexes(MetaData metadata) {

		// DT1
		int indexID = 2;
		int numColumns = 2;
		List<Integer> columnEncodingsList2 = new ArrayList<Integer>();
		columnEncodingsList2.add(MetaData.ENCODING_BB);
		columnEncodingsList2.add(MetaData.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList2 = new ArrayList<Integer>();
		columnEncodedByteSizesList2.add(MetaData.BYTES_4);
		columnEncodedByteSizesList2.add(MetaData.BYTES_1);
		
		MetaIndex DT1 = new MetaIndex(0, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList2, columnEncodedByteSizesList2);
		metadata.getIndexList().add(DT1);
		
		// DT2
		indexID = 3;
		numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(MetaData.ENCODING_BB);
		columnEncodingsList3.add(MetaData.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(1, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().add(DT2);

	}
	private static void initQ1Operators(List<Operator> operators, MetaQuery query, List<Integer> selections) {
	
		List<Alias> aliases = query.getAliases();
		
		
		Operator selection1 = new SelectionOperator(selections, aliases.get(0));
		operators.add(selection1);
		
		List<Integer> column2IDs = new ArrayList<Integer>();
		column2IDs.add(0);
		Operator join2 = new JoinOperator(false, column2IDs, aliases.get(1), aliases.get(0), 0);
		
		operators.add(join2);

		List<Integer> column3IDs = new ArrayList<Integer>();
		column3IDs.add(0);
		Operator join3 = new JoinOperator(false, column3IDs, aliases.get(2), aliases.get(1), 0);
		
		operators.add(join3);
				
		int aggregationindexID = 3;
		
		String aggString = "1";
		
		List<Alias> aggAliasList = new ArrayList<Alias>();
		
		List<Integer> aggOpColList = new ArrayList<Integer>();

		/*public AggregationOperator(int indexID, 
				int dataType, String aggregationString, 
				List<Integer> aggregationVariablesOperators, List<Integer> aggregationVariablesColumns, int drivingAlias, 
				int drivingAliasColumn, int drivingOperator, int drivingAliasIndexID) {*/
		
		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_INT, aggString, aggAliasList, aggOpColList, aliases.get(2), 0);
	
		operators.add(agg);
	}
	
	private static void initQ1OperatorsThreaded(List<Operator> operators, MetaQuery query, List<Integer> selections) {
		
		
		
		List<Alias> aliases = query.getAliases();
		
		
		Operator selection1 = new SelectionOperator(selections, aliases.get(0));
		operators.add(selection1);
		
		List<Integer> column2IDs = new ArrayList<Integer>();
		column2IDs.add(0);
		Operator join2 = new JoinOperator(false, column2IDs, aliases.get(1), aliases.get(0), 0);
		
		operators.add(join2);

		Operator threadOp = new ThreadingOperator(aliases.get(1), false);
		operators.add(threadOp);
		
		List<Integer> column3IDs = new ArrayList<Integer>();
		column3IDs.add(0);
		Operator join3 = new JoinOperator(false, column3IDs, aliases.get(2), aliases.get(1), 0);
		
		operators.add(join3);
				
		int aggregationindexID = 3;
		
		String aggString = "1";
		
		List<Alias> aggAliasList = new ArrayList<Alias>();
		
		List<Integer> aggOpColList = new ArrayList<Integer>();

		
		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_INT, aggString, aggAliasList, aggOpColList, aliases.get(2), 0);
	
		operators.add(agg);
		
		
		
	}
	
	private static void runQ2(String queryName, int numThreads, List<Integer> selections, int encoding) {
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		
		initQ1Indexes(metadata, encoding);
		initQ1Queries(metadata, queryName, numThreads);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());
		
		
		if (numThreads > 1) {
			initQ1OperatorsThreaded(operators, query, selections);
		}
		else {
			initQ1Operators(operators, query, selections);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	

	private static void runQ2(String queryName, int numThreads, List<Integer> selections, boolean b) {
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		initQ1Indexes(metadata);
		initQ1Queries(metadata, queryName, numThreads);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());
		if (numThreads > 1) {
			initQ1OperatorsThreaded(operators, query, selections);
		}
		else {
			initQ1Operators(operators, query, selections);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	
	public static void main(String[] args) {
		List<Integer> selections = new ArrayList<Integer>();
		selections.add(1000);
		//Q1 Optimal
		runQ2("test_pubmed_q1_opt", 1, selections, true);
		runQ2("test_pubmed_q1_opt_threaded", 4, selections, true);
		
		// Q1 UA
		runQ2("test_pubmed_q1_array", 1, selections, MetaData.ENCODING_UA);
		runQ2("test_pubmed_q1_array_threaded", 4, selections, MetaData.ENCODING_UA);
		
		
		// Q1 BCA
		runQ2("test_pubmed_q1_bca", 1, selections, MetaData.ENCODING_BCA);
		runQ2("test_pubmed_q1_bca_threaded", 4, selections, MetaData.ENCODING_BCA);
				
		// Q1 Huffman
		runQ2("test_pubmed_q1_huffman", 1, selections, MetaData.ENCODING_HUFFMAN);
		runQ2("test_pubmed_q1_huffman_threaded", 4, selections, MetaData.ENCODING_HUFFMAN);
		
		
		
	}
	
	
	
}
