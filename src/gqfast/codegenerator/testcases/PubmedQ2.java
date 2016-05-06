package codegenerator.testcases;

import java.util.ArrayList;
import java.util.List;

import codegenerator.AggregationOperator;
import codegenerator.Alias;
import codegenerator.CodeGenerator;
import codegenerator.JoinOperator;
import codegenerator.MetaData;
import codegenerator.MetaIndex;
import codegenerator.MetaQuery;
import codegenerator.Operator;
import codegenerator.SelectionOperator;
import codegenerator.ThreadingOperator;

public class PubmedQ2 {

	private static void initQ2Queries(MetaData metadata, String queryName, int numThreads) {

		List<Alias> aliases = new ArrayList<Alias>();
		
		Alias alias0 = new Alias(0, "doc1");
		Alias alias1 = new Alias(1, "year1", metadata.getIndexList().get(0));
		Alias alias2 = new Alias(2, "term1", metadata.getIndexList().get(1));
		Alias alias3 = new Alias(3, "doc2", metadata.getIndexList().get(2));
		Alias alias4 = new Alias(4, "year2", metadata.getIndexList().get(0));
		
		aliases.add(alias0);
		aliases.add(alias1);
		aliases.add(alias2);
		aliases.add(alias3);
		aliases.add(alias4);
		
		// public MetaQuery(int queryID, String queryName, int numThreads,
		// int numBuffers, int bufferPoolSize, List<String> aliases)
		MetaQuery q2Optimal = new MetaQuery(0, queryName, numThreads, 1, aliases);
				
		metadata.getQueryList().add(q2Optimal);
		metadata.setCurrentQueryID(metadata.getQueryList().size()-1);
		
	}
	
	private static void initQ2Indexes(MetaData metadata, int encodingType) {
		
		// DY
		int indexID = 1;
		int numColumns = 1;
		List<Integer> columnEncodingsList1 = new ArrayList<Integer>();
		columnEncodingsList1.add(encodingType);
		List<Integer> columnEncodedByteSizesList1 = new ArrayList<Integer>();
		columnEncodedByteSizesList1.add(MetaData.BYTES_4);
		
		MetaIndex DY = new MetaIndex(0, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList1, columnEncodedByteSizesList1);
		metadata.getIndexList().add(DY);

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
		
		// DT2
		indexID = 3;
		numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(encodingType);
		columnEncodingsList3.add(encodingType);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(2, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().add(DT2);
		

	}
	
	
	private static void initQ2Indexes(MetaData metadata) {
		
		// DY
		int indexID = 1;
		int numColumns = 1;
		List<Integer> columnEncodingsList1 = new ArrayList<Integer>();
		columnEncodingsList1.add(MetaData.ENCODING_BCA);
		List<Integer> columnEncodedByteSizesList1 = new ArrayList<Integer>();
		columnEncodedByteSizesList1.add(MetaData.BYTES_4);
		
		MetaIndex DY = new MetaIndex(0, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList1, columnEncodedByteSizesList1);
		metadata.getIndexList().add(DY);

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
		
		// DT2
		indexID = 3;
		numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(MetaData.ENCODING_BB);
		columnEncodingsList3.add(MetaData.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(2, indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().add(DT2);

	}
	private static void initQ2Operators(List<Operator> operators, MetaQuery query) {
	
		List<Alias> aliases = query.getAliases();
		
		List<Integer> selections = new ArrayList<Integer>();
		selections.add(16966392);
		Operator selection1 = new SelectionOperator(selections, aliases.get(0));
		operators.add(selection1);
		
		
		List<Integer> column1IDs = new ArrayList<Integer>();
		column1IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join1 = new JoinOperator(true, column1IDs, aliases.get(1), aliases.get(0), 0);
		
		operators.add(join1);

		List<Integer> column2IDs = new ArrayList<Integer>();
		column2IDs.add(0);
		column2IDs.add(1);
		Operator join2 = new JoinOperator(false, column2IDs, aliases.get(2), aliases.get(0), 0);
		
		operators.add(join2);

		List<Integer> column3IDs = new ArrayList<Integer>();
		column3IDs.add(0);
		column3IDs.add(1);
		Operator join3 = new JoinOperator(false, column3IDs, aliases.get(3), aliases.get(2), 0);
		
		operators.add(join3);

		List<Integer> column4IDs = new ArrayList<Integer>();
		column4IDs.add(0);
		Operator join4 = new JoinOperator(true, column4IDs, aliases.get(4), aliases.get(3), 0);
		
		operators.add(join4);
				
		int aggregationindexID = 3;
		
		String aggString = "(double)( op0 * op1 )/(ABS( op2 - op3 )+1)";
		
		List<Alias> aggAliasList = new ArrayList<Alias>();
		aggAliasList.add(query.getAliases().get(2));
		aggAliasList.add(query.getAliases().get(3));
		aggAliasList.add(query.getAliases().get(1));
		aggAliasList.add(query.getAliases().get(4));
		
		List<Integer> aggOpColList = new ArrayList<Integer>();
		aggOpColList.add(1);
		aggOpColList.add(1);
		aggOpColList.add(0);
		aggOpColList.add(0);
		
		/*public AggregationOperator(int indexID, 
				int dataType, String aggregationString, 
				List<Integer> aggregationVariablesOperators, List<Integer> aggregationVariablesColumns, int drivingAlias, 
				int drivingAliasColumn, int drivingOperator, int drivingAliasIndexID) {*/
		
		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_DOUBLE, aggString, aggAliasList, aggOpColList, aliases.get(3), 0);
	
		operators.add(agg);
	}
	
	private static void initQ2OperatorsThreaded(List<Operator> operators, MetaQuery query) {
		
		List<Alias> aliases = query.getAliases();
		
		List<Integer> selections = new ArrayList<Integer>();
		selections.add(16966392);
		Operator selection1 = new SelectionOperator(selections, aliases.get(0));
		operators.add(selection1);
		
		
		List<Integer> column1IDs = new ArrayList<Integer>();
		column1IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join1 = new JoinOperator(true, column1IDs, aliases.get(1), aliases.get(0), 0);
		
		operators.add(join1);
		
		List<Integer> column2IDs = new ArrayList<Integer>();
		column2IDs.add(0);
		column2IDs.add(1);
		Operator join2 = new JoinOperator(false, column2IDs, aliases.get(2), aliases.get(0), 0);
		
		operators.add(join2);
		
		Operator threadOp = new ThreadingOperator(aliases.get(2));
		operators.add(threadOp);
		
		List<Integer> column3IDs = new ArrayList<Integer>();
		column3IDs.add(0);
		column3IDs.add(1);
		Operator join3 = new JoinOperator(false, column3IDs, aliases.get(3), aliases.get(2), 0);
		
		operators.add(join3);

		List<Integer> column4IDs = new ArrayList<Integer>();
		column4IDs.add(0);
		Operator join4 = new JoinOperator(true, column4IDs, aliases.get(4), aliases.get(3), 0);
		
		operators.add(join4);
				
		int aggregationindexID = 3;
		
		String aggString = "(double)( op0 * op1 )/(ABS( op2 - op3 )+1)";
		
		List<Alias> aggAliasList = new ArrayList<Alias>();
		aggAliasList.add(query.getAliases().get(2));
		aggAliasList.add(query.getAliases().get(3));
		aggAliasList.add(query.getAliases().get(1));
		aggAliasList.add(query.getAliases().get(4));
		
		List<Integer> aggOpColList = new ArrayList<Integer>();
		aggOpColList.add(1);
		aggOpColList.add(1);
		aggOpColList.add(0);
		aggOpColList.add(0);
		
		/*public AggregationOperator(int indexID, 
				int dataType, String aggregationString, 
				List<Integer> aggregationVariablesOperators, List<Integer> aggregationVariablesColumns, int drivingAlias, 
				int drivingAliasColumn, int drivingOperator, int drivingAliasIndexID) {*/
		
		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_DOUBLE, aggString, aggAliasList, aggOpColList, aliases.get(3), 0);
	
		operators.add(agg);
	}
	
	private static void runQ2(String queryName, int numThreads, int encoding) {
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		
		initQ2Indexes(metadata, encoding);
		initQ2Queries(metadata, queryName, numThreads);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());
		if (numThreads > 1) {
			initQ2OperatorsThreaded(operators, query);
		}
		else {
			initQ2Operators(operators, query);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	

	private static void runQ2(String queryName, int numThreads, boolean b) {
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		initQ2Indexes(metadata);
		initQ2Queries(metadata, queryName, numThreads);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());
		if (numThreads > 1) {
			initQ2OperatorsThreaded(operators, query);
		}
		else {
			initQ2Operators(operators, query);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	
	public static void main(String[] args) {
		
		//Q2 Optimal
		runQ2("test_pubmed_q2_opt", 1, true);
		runQ2("test_pubmed_q2_opt_threaded", 4, true);
		
		// Q2 UA
		runQ2("test_pubmed_q2_array", 1, MetaData.ENCODING_UA);
		runQ2("test_pubmed_q2_array_threaded", 4, MetaData.ENCODING_UA);
	}

}
