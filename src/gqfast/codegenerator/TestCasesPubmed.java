package codegenerator;

import java.util.ArrayList;
import java.util.List;


public class TestCasesPubmed {


	private static void initPubmedQueries(Metadata metadata, String queryName, int numThreads) {

		List<String> aliases = new ArrayList<String>();
		aliases.add("author1");
		aliases.add("doc1");
		aliases.add("term");
		aliases.add("doc2");
		aliases.add("author2");
		aliases.add("year");
		
		// public MetaQuery(int queryID, String queryName, int numThreads,
		// int numBuffers, int bufferPoolSize, List<String> aliases)
		MetaQuery q5Optimal = new MetaQuery(0, queryName, numThreads, 5, 1, aliases);
		
		
		metadata.getQueryList().add(q5Optimal);
		metadata.setCurrentQueryID(metadata.getQueryList().size()-1);
	}

	private static void initPubmedIndexes(Metadata metadata, int encodingType) {
		
		// DA1
		int indexID = 0;
		int numColumns = 1;
		List<Integer> columnEncodingsList0 = new ArrayList<Integer>();
		columnEncodingsList0.add(encodingType);
		List<Integer> columnEncodedByteSizesList0 = new ArrayList<Integer>();
		columnEncodedByteSizesList0.add(Metadata.BYTES_4);
		
		MetaIndex DA1 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList0, columnEncodedByteSizesList0);
		metadata.getIndexList().add(DA1);

		// DY
		indexID = 1;
		numColumns = 1;
		List<Integer> columnEncodingsList1 = new ArrayList<Integer>();
		columnEncodingsList1.add(encodingType);
		List<Integer> columnEncodedByteSizesList1 = new ArrayList<Integer>();
		columnEncodedByteSizesList1.add(Metadata.BYTES_4);
		
		MetaIndex DY = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList1, columnEncodedByteSizesList1);
		metadata.getIndexList().add(DY);

		// DT1
		indexID = 2;
		numColumns = 2;
		List<Integer> columnEncodingsList2 = new ArrayList<Integer>();
		columnEncodingsList2.add(encodingType);
		columnEncodingsList2.add(encodingType);
		
		List<Integer >columnEncodedByteSizesList2 = new ArrayList<Integer>();
		columnEncodedByteSizesList2.add(Metadata.BYTES_4);
		columnEncodedByteSizesList2.add(Metadata.BYTES_1);
		
		MetaIndex DT1 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList2, columnEncodedByteSizesList2);
		metadata.getIndexList().add(DT1);
		
		// DT2
		indexID = 3;
		numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(encodingType);
		columnEncodingsList3.add(encodingType);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(Metadata.BYTES_4);
		columnEncodedByteSizesList3.add(Metadata.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().add(DT2);
		
		// DA2
		indexID = 4;
		numColumns = 1;
		List<Integer> columnEncodingsList4 = new ArrayList<Integer>();
		columnEncodingsList4.add(encodingType);
		List<Integer> columnEncodedByteSizesList4 = new ArrayList<Integer>();
		columnEncodedByteSizesList4.add(Metadata.BYTES_4);
		
		MetaIndex DA2 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList4, columnEncodedByteSizesList4);
		metadata.getIndexList().add(DA2);

	}
	private static void initPubmedIndexes(Metadata metadata) {
				
		// DA1
		int indexID = 0;
		int numColumns = 1;
		List<Integer> columnEncodingsList0 = new ArrayList<Integer>();
		columnEncodingsList0.add(Metadata.ENCODING_BB);
		List<Integer> columnEncodedByteSizesList0 = new ArrayList<Integer>();
		columnEncodedByteSizesList0.add(Metadata.BYTES_4);
		
		MetaIndex DA1 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList0, columnEncodedByteSizesList0);
		metadata.getIndexList().add(DA1);

		// DY
		indexID = 1;
		numColumns = 1;
		List<Integer> columnEncodingsList1 = new ArrayList<Integer>();
		columnEncodingsList1.add(Metadata.ENCODING_BCA);
		List<Integer> columnEncodedByteSizesList1 = new ArrayList<Integer>();
		columnEncodedByteSizesList1.add(Metadata.BYTES_4);
		
		MetaIndex DY = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList1, columnEncodedByteSizesList1);
		metadata.getIndexList().add(DY);

		// DT1
		indexID = 2;
		numColumns = 2;
		List<Integer> columnEncodingsList2 = new ArrayList<Integer>();
		columnEncodingsList2.add(Metadata.ENCODING_BB);
		columnEncodingsList2.add(Metadata.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList2 = new ArrayList<Integer>();
		columnEncodedByteSizesList2.add(Metadata.BYTES_4);
		columnEncodedByteSizesList2.add(Metadata.BYTES_1);
		
		MetaIndex DT1 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList2, columnEncodedByteSizesList2);
		metadata.getIndexList().add(DT1);
		
		// DT2
		indexID = 3;
		numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(Metadata.ENCODING_BB);
		columnEncodingsList3.add(Metadata.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(Metadata.BYTES_4);
		columnEncodedByteSizesList3.add(Metadata.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().add(DT2);
		
		// DA2
		indexID = 4;
		numColumns = 1;
		List<Integer> columnEncodingsList4 = new ArrayList<Integer>();
		columnEncodingsList4.add(Metadata.ENCODING_BCA);
		List<Integer> columnEncodedByteSizesList4 = new ArrayList<Integer>();
		columnEncodedByteSizesList4.add(Metadata.BYTES_4);
		
		MetaIndex DA2 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList4, columnEncodedByteSizesList4);
		metadata.getIndexList().add(DA2);

	}
	
	private static void initQ5OperatorsThreaded(List<Operator> operators) {
	
		
		
		List<Integer> selections = new ArrayList<Integer>();
		selections.add(4945389);
		Operator selection1 = new SelectionOperator(selections, 0);
		operators.add(selection1);
		
		
		
		int join1indexID = 0;
		List<Integer> column1IDs = new ArrayList<Integer>();
		column1IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join1 = new JoinOperator(join1indexID, false, column1IDs, 1, 0, 0, 0);
		
		operators.add(join1);
		
		int join2indexID = 2;
		List<Integer> column2IDs = new ArrayList<Integer>();
		column2IDs.add(0);
		column2IDs.add(1);
		Operator join2 = new JoinOperator(join2indexID, false, column2IDs, 2, 0, 1, 0);
		
		operators.add(join2);
		
		Operator threadOp = new ThreadingOperator(2);
		operators.add(threadOp);
		
		int join3indexID = 3;
		List<Integer> column3IDs = new ArrayList<Integer>();
		column3IDs.add(0);
		column3IDs.add(1);
		Operator join3 = new JoinOperator(join3indexID, false, column3IDs, 3, 0, 2, 0);
		
		operators.add(join3);
		
		int join4indexID = 1;
		List<Integer> column4IDs = new ArrayList<Integer>();
		column4IDs.add(0);
		Operator join4 = new JoinOperator(join4indexID, true, column4IDs, 5, 0, 3, 0);
		
		operators.add(join4);
		
		int join5indexID = 4;
		List<Integer> column5IDs = new ArrayList<Integer>();
		column5IDs.add(0);
		Operator join5 = new JoinOperator(join5indexID, false, column5IDs, 4, 0, 3, 0);
		
		operators.add(join5);
		
		int aggregationindexID = 4;
		
		String aggString = "(double)( op0 * op1 )/(2017 - op2 )";
		
		List<Integer> aggAliasList = new ArrayList<Integer>();
		aggAliasList.add(2);
		aggAliasList.add(3);
		aggAliasList.add(5);
		
		List<Integer> aggOpColList = new ArrayList<Integer>();
		aggOpColList.add(1);
		aggOpColList.add(1);
		aggOpColList.add(0);
		
		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_DOUBLE, aggString, aggAliasList, aggOpColList, 4, 0, 6, 4);
	
		operators.add(agg);
	}

	private static void initQ5Operators(List<Operator> operators) {
	
		
		
		List<Integer> selections = new ArrayList<Integer>();
		selections.add(4945389);
		Operator selection1 = new SelectionOperator(selections, 0);
		operators.add(selection1);
		
		
		
		int join1indexID = 0;
		List<Integer> column1IDs = new ArrayList<Integer>();
		column1IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join1 = new JoinOperator(join1indexID, false, column1IDs, 1, 0, 0, 0);
		
		operators.add(join1);
		
		int join2indexID = 2;
		List<Integer> column2IDs = new ArrayList<Integer>();
		column2IDs.add(0);
		column2IDs.add(1);
		Operator join2 = new JoinOperator(join2indexID, false, column2IDs, 2, 0, 1, 0);
		
		operators.add(join2);
		
		int join3indexID = 3;
		List<Integer> column3IDs = new ArrayList<Integer>();
		column3IDs.add(0);
		column3IDs.add(1);
		Operator join3 = new JoinOperator(join3indexID, false, column3IDs, 3, 0, 2, 0);
		
		operators.add(join3);
		
		int join4indexID = 1;
		List<Integer> column4IDs = new ArrayList<Integer>();
		column4IDs.add(0);
		Operator join4 = new JoinOperator(join4indexID, true, column4IDs, 5, 0, 3, 0);
		
		operators.add(join4);
		
		int join5indexID = 4;
		List<Integer> column5IDs = new ArrayList<Integer>();
		column5IDs.add(0);
		Operator join5 = new JoinOperator(join5indexID, false, column5IDs, 4, 0, 3, 0);
		
		operators.add(join5);
		
		int aggregationindexID = 4;
		
		String aggString = "(double)( op0 * op1 )/(2017 - op2 )";
		
		List<Integer> aggAliasList = new ArrayList<Integer>();
		aggAliasList.add(2);
		aggAliasList.add(3);
		aggAliasList.add(5);
		
		List<Integer> aggOpColList = new ArrayList<Integer>();
		aggOpColList.add(1);
		aggOpColList.add(1);
		aggOpColList.add(0);
		
		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_DOUBLE, aggString, aggAliasList, aggOpColList, 4, 0, 5, 4);
	
		operators.add(agg);
	}


	private static void runQuery(String queryName, int numThreads, int encoding) {
		List<Operator> operators = new ArrayList<Operator>();
		Metadata metadata = new Metadata();
		
		initPubmedIndexes(metadata, encoding);
		initPubmedQueries(metadata, queryName, numThreads);
		if (numThreads > 1) {
			initQ5OperatorsThreaded(operators);
		}
		else {
			initQ5Operators(operators);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	

	private static void runQuery(String queryName, int numThreads, boolean b) {
		List<Operator> operators = new ArrayList<Operator>();
		Metadata metadata = new Metadata();
		initPubmedIndexes(metadata);
		initPubmedQueries(metadata, queryName, numThreads);
		if (numThreads > 1) {
			initQ5OperatorsThreaded(operators);
		}
		else {
			initQ5Operators(operators);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	
	public static void main(String[] args) {
		
		
		
		
		
		// Pubmed 
		//Q5 Optimal 
		runQuery("test_pubmed_q5_opt", 1, true);
		runQuery("test_pubmed_q5_opt_threaded", 4, true);
		
		//Q5 Huffman
		runQuery("test_pubmed_q5_huffman", 1, Metadata.ENCODING_HUFFMAN);
		runQuery("test_pubmed_q5_huffman_threaded", 4, Metadata.ENCODING_HUFFMAN);
		
		//Q5 BCA
		runQuery("test_pubmed_q5_bca", 1, Metadata.ENCODING_BCA);
		runQuery("test_pubmed_q5_bca_threaded", 4, Metadata.ENCODING_BCA);
		
		//Q5 UA
		runQuery("test_pubmed_q5_array", 1, Metadata.ENCODING_UA);
		runQuery("test_pubmed_q5_array_threaded", 4, Metadata.ENCODING_UA);
				
	}




}
