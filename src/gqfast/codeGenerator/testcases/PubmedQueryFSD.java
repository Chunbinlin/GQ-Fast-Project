package gqfast.codeGenerator.testcases;

import java.util.ArrayList;
import java.util.List;

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

public class PubmedQueryFSD {

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
	private static void initQ2Operators(List<Operator> operators, MetaQuery query, List<Integer> selections) {
	
		List<Alias> aliases = query.getAliases();
		
		
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
		
		String aggString = "(double)( op0 * op1 )/(ABS((int) op2 - (int) op3 )+1)";
		
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
	
	private static void initQ2OperatorsThreaded(List<Operator> operators, MetaQuery query, List<Integer> selections) {
		
		List<Alias> aliases = query.getAliases();
		
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
		
		Operator threadOp = new ThreadingOperator(aliases.get(2), false);
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
		
		String aggString = "(double)( op0 * op1 )/(ABS((int) op2 - (int) op3 )+1)";
		
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
	
	private static void runQ2(String queryName, int numThreads, List<Integer> selections, int encoding) {
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		
		initQ2Indexes(metadata, encoding);
		initQ2Queries(metadata, queryName, numThreads);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());
		
		
		if (numThreads > 1) {
			initQ2OperatorsThreaded(operators, query, selections);
		}
		else {
			initQ2Operators(operators, query, selections);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	

	private static void runQ2(String queryName, int numThreads, List<Integer> selections, boolean b) {
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		initQ2Indexes(metadata);
		initQ2Queries(metadata, queryName, numThreads);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());
		if (numThreads > 1) {
			initQ2OperatorsThreaded(operators, query, selections);
		}
		else {
			initQ2Operators(operators, query, selections);
		}
		CodeGenerator.generateCode(operators, metadata);
	}
	
	public static void main(String[] args) {
		List<Integer> selection1 = new ArrayList<Integer>();
		selection1.add(16966392);
		
		List<Integer> selection2 = new ArrayList<Integer>();
		selection2.add(17996791);
		
		List<Integer> selection3 = new ArrayList<Integer>();
		selection3.add(17044542);
		
		List<Integer> selection4 = new ArrayList<Integer>();
		selection4.add(18681952);
		
		List<Integer> selection5 = new ArrayList<Integer>();
		selection5.add(19265035);
		
		List<Integer> selection6 = new ArrayList<Integer>();
		selection6.add(10296795);
		
		List<Integer> selection7 = new ArrayList<Integer>();
		selection7.add(17495979);
		//Q1 Optimal
		runQ2("q2_opt_0threads_16966392", 1, selection1, true);
		runQ2("q2_opt_2threads_16966392", 2, selection1, true);
		runQ2("q2_opt_4threads_16966392", 4, selection1, true);
		runQ2("q2_opt_1threads_16966392", 10, selection1, true);
		
		runQ2("q2_opt_0threads_17996791", 1, selection2, true);
		runQ2("q2_opt_2threads_17996791", 2, selection2, true);
		runQ2("q2_opt_4threads_17996791", 4, selection2, true);
		runQ2("q2_opt_1threads_17966791", 10, selection2, true);
		
		runQ2("q2_opt_0threads_17044542", 1, selection3, true);
		runQ2("q2_opt_2threads_17044542", 2, selection3, true);
		runQ2("q2_opt_4threads_17044542", 4, selection3, true);
		runQ2("q2_opt_1threads_17044542", 10, selection3, true);
		
		runQ2("q2_opt_0threads_18681952", 1, selection4, true);
		runQ2("q2_opt_2threads_18681952", 2, selection4, true);
		runQ2("q2_opt_4threads_18681952", 4, selection4, true);
		runQ2("q2_opt_1threads_18681952", 10, selection4, true);
		
		runQ2("q2_opt_0threads_19265035", 1, selection5, true);
		runQ2("q2_opt_2threads_19265035", 2, selection5, true);
		runQ2("q2_opt_4threads_19265035", 4, selection5, true);
		runQ2("q2_opt_1threads_19265035", 10, selection5, true);
		
		runQ2("q2_opt_0threads_10296795", 1, selection6, true);
		runQ2("q2_opt_2threads_10296795", 2, selection6, true);
		runQ2("q2_opt_4threads_10296795", 4, selection6, true);
		runQ2("q2_opt_1threads_10296795", 10, selection6, true);
		
		runQ2("q2_opt_0threads_17495979", 1, selection7, true);
		runQ2("q2_opt_2threads_17495979", 2, selection7, true);
		runQ2("q2_opt_4threads_17495979", 4, selection7, true);
		runQ2("q2_opt_1threads_17495979", 10, selection7, true);
				
		//Q1 Array
		runQ2("q2_array_0threads_16966392", 1, selection1, MetaData.ENCODING_UA);
		runQ2("q2_array_2threads_16966392", 2, selection1, MetaData.ENCODING_UA);
		runQ2("q2_array_4threads_16966392", 4, selection1, MetaData.ENCODING_UA);
		runQ2("q2_array_1threads_16966392", 10, selection1, MetaData.ENCODING_UA);
		
		runQ2("q2_array_0threads_17996791", 1, selection2, MetaData.ENCODING_UA);
		runQ2("q2_array_2threads_17996791", 2, selection2, MetaData.ENCODING_UA);
		runQ2("q2_array_4threads_17996791", 4, selection2, MetaData.ENCODING_UA);
		runQ2("q2_array_1threads_17966791", 10, selection2, MetaData.ENCODING_UA);
		
		runQ2("q2_array_0threads_17044542", 1, selection3, MetaData.ENCODING_UA);
		runQ2("q2_array_2threads_17044542", 2, selection3, MetaData.ENCODING_UA);
		runQ2("q2_array_4threads_17044542", 4, selection3, MetaData.ENCODING_UA);
		runQ2("q2_array_1threads_17044542", 10, selection3, MetaData.ENCODING_UA);
		
		runQ2("q2_array_0threads_18681952", 1, selection4, MetaData.ENCODING_UA);
		runQ2("q2_array_2threads_18681952", 2, selection4, MetaData.ENCODING_UA);
		runQ2("q2_array_4threads_18681952", 4, selection4, MetaData.ENCODING_UA);
		runQ2("q2_array_1threads_18681952", 10, selection4, MetaData.ENCODING_UA);
		
		runQ2("q2_array_0threads_19265035", 1, selection5, MetaData.ENCODING_UA);
		runQ2("q2_array_2threads_19265035", 2, selection5, MetaData.ENCODING_UA);
		runQ2("q2_array_4threads_19265035", 4, selection5, MetaData.ENCODING_UA);
		runQ2("q2_array_1threads_19265035", 10, selection5, MetaData.ENCODING_UA);
		
		runQ2("q2_array_0threads_10296795", 1, selection6, MetaData.ENCODING_UA);
		runQ2("q2_array_2threads_10296795", 2, selection6, MetaData.ENCODING_UA);
		runQ2("q2_array_4threads_10296795", 4, selection6, MetaData.ENCODING_UA);
		runQ2("q2_array_1threads_10296795", 10, selection6, MetaData.ENCODING_UA);
		
		runQ2("q2_array_0threads_17495979", 1, selection7, MetaData.ENCODING_UA);
		runQ2("q2_array_2threads_17495979", 2, selection7, MetaData.ENCODING_UA);
		runQ2("q2_array_4threads_17495979", 4, selection7, MetaData.ENCODING_UA);
		runQ2("q2_array_1threads_17495979", 10, selection7, MetaData.ENCODING_UA);
		
	}

}
