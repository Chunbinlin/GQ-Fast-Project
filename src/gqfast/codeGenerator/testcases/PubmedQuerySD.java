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
		MetaQuery q2Optimal = new MetaQuery(0, queryName, numThreads, aliases);
				
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
		
		MetaIndex DT1 = new MetaIndex(indexID, numColumns, MetaData.BYTES_4, columnEncodingsList2, columnEncodedByteSizesList2);
		metadata.getIndexList().put(indexID, DT1);
		
		// DT2
		indexID = 3;
		numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(encodingType);
		columnEncodingsList3.add(encodingType);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().put(indexID, DT2);
		

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
		
		MetaIndex DT1 = new MetaIndex(indexID, numColumns, MetaData.BYTES_4, columnEncodingsList2, columnEncodedByteSizesList2);
		metadata.getIndexList().put(indexID, DT1);
		
		// DT2
		indexID = 3;
		numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(MetaData.ENCODING_BB);
		columnEncodingsList3.add(MetaData.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().put(indexID, DT2);

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
	
	private static void runQ1(String queryName, int numThreads, List<Integer> selections, int encoding) {
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
	

	private static void runQ1(String queryName, int numThreads, List<Integer> selections, boolean b) {
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
		runQ1("q1_opt_0threads_16966392", 1, selection1, true);
		runQ1("q1_opt_2threads_16966392", 2, selection1, true);
		runQ1("q1_opt_4threads_16966392", 4, selection1, true);
		runQ1("q1_opt_1threads_16966392", 10, selection1, true);
		
		runQ1("q1_opt_0threads_17996791", 1, selection2, true);
		runQ1("q1_opt_2threads_17996791", 2, selection2, true);
		runQ1("q1_opt_4threads_17996791", 4, selection2, true);
		runQ1("q1_opt_1threads_17966791", 10, selection2, true);
		
		runQ1("q1_opt_0threads_17044542", 1, selection3, true);
		runQ1("q1_opt_2threads_17044542", 2, selection3, true);
		runQ1("q1_opt_4threads_17044542", 4, selection3, true);
		runQ1("q1_opt_1threads_17044542", 10, selection3, true);
		
		runQ1("q1_opt_0threads_18681952", 1, selection4, true);
		runQ1("q1_opt_2threads_18681952", 2, selection4, true);
		runQ1("q1_opt_4threads_18681952", 4, selection4, true);
		runQ1("q1_opt_1threads_18681952", 10, selection4, true);
		
		runQ1("q1_opt_0threads_19265035", 1, selection5, true);
		runQ1("q1_opt_2threads_19265035", 2, selection5, true);
		runQ1("q1_opt_4threads_19265035", 4, selection5, true);
		runQ1("q1_opt_1threads_19265035", 10, selection5, true);
		
		runQ1("q1_opt_0threads_10296795", 1, selection6, true);
		runQ1("q1_opt_2threads_10296795", 2, selection6, true);
		runQ1("q1_opt_4threads_10296795", 4, selection6, true);
		runQ1("q1_opt_1threads_10296795", 10, selection6, true);
		
		runQ1("q1_opt_0threads_17495979", 1, selection7, true);
		runQ1("q1_opt_2threads_17495979", 2, selection7, true);
		runQ1("q1_opt_4threads_17495979", 4, selection7, true);
		runQ1("q1_opt_1threads_17495979", 10, selection7, true);
				
		//Q1 Array
		runQ1("q1_array_0threads_16966392", 1, selection1, MetaData.ENCODING_UA);
		runQ1("q1_array_2threads_16966392", 2, selection1, MetaData.ENCODING_UA);
		runQ1("q1_array_4threads_16966392", 4, selection1, MetaData.ENCODING_UA);
		runQ1("q1_array_1threads_16966392", 10, selection1, MetaData.ENCODING_UA);
		
		runQ1("q1_array_0threads_17996791", 1, selection2, MetaData.ENCODING_UA);
		runQ1("q1_array_2threads_17996791", 2, selection2, MetaData.ENCODING_UA);
		runQ1("q1_array_4threads_17996791", 4, selection2, MetaData.ENCODING_UA);
		runQ1("q1_array_1threads_17966791", 10, selection2, MetaData.ENCODING_UA);
		
		runQ1("q1_array_0threads_17044542", 1, selection3, MetaData.ENCODING_UA);
		runQ1("q1_array_2threads_17044542", 2, selection3, MetaData.ENCODING_UA);
		runQ1("q1_array_4threads_17044542", 4, selection3, MetaData.ENCODING_UA);
		runQ1("q1_array_1threads_17044542", 10, selection3, MetaData.ENCODING_UA);
		
		runQ1("q1_array_0threads_18681952", 1, selection4, MetaData.ENCODING_UA);
		runQ1("q1_array_2threads_18681952", 2, selection4, MetaData.ENCODING_UA);
		runQ1("q1_array_4threads_18681952", 4, selection4, MetaData.ENCODING_UA);
		runQ1("q1_array_1threads_18681952", 10, selection4, MetaData.ENCODING_UA);
		
		runQ1("q1_array_0threads_19265035", 1, selection5, MetaData.ENCODING_UA);
		runQ1("q1_array_2threads_19265035", 2, selection5, MetaData.ENCODING_UA);
		runQ1("q1_array_4threads_19265035", 4, selection5, MetaData.ENCODING_UA);
		runQ1("q1_array_1threads_19265035", 10, selection5, MetaData.ENCODING_UA);
		
		runQ1("q1_array_0threads_10296795", 1, selection6, MetaData.ENCODING_UA);
		runQ1("q1_array_2threads_10296795", 2, selection6, MetaData.ENCODING_UA);
		runQ1("q1_array_4threads_10296795", 4, selection6, MetaData.ENCODING_UA);
		runQ1("q1_array_1threads_10296795", 10, selection6, MetaData.ENCODING_UA);
		
		runQ1("q1_array_0threads_17495979", 1, selection7, MetaData.ENCODING_UA);
		runQ1("q1_array_2threads_17495979", 2, selection7, MetaData.ENCODING_UA);
		runQ1("q1_array_4threads_17495979", 4, selection7, MetaData.ENCODING_UA);
		runQ1("q1_array_1threads_17495979", 10, selection7, MetaData.ENCODING_UA);
	}
	
	
	
}
