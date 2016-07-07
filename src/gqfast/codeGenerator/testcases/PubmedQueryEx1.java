package gqfast.codeGenerator.testcases;

import java.util.ArrayList;
import java.util.List;

import gqfast.codeGenerator.AggregationOperator;
import gqfast.codeGenerator.CodeGenerator;
import gqfast.codeGenerator.JoinOperator;
import gqfast.codeGenerator.Operator;
import gqfast.codeGenerator.SelectionOperator;
import gqfast.global.Alias;
import gqfast.global.MetaData;
import gqfast.global.MetaIndex;
import gqfast.global.MetaQuery;

public class PubmedQueryEx1 {

	
	private static void initE1Operators(List<Operator> operators,
		MetaQuery query, List<Integer> selections) {
		
		List<Alias> aliases = query.getAliases();
		
		Operator selection1 = new SelectionOperator(selections, aliases.get(0));
		operators.add(selection1);
		
		List<Integer> column1IDs = new ArrayList<Integer>();
		column1IDs.add(0);
		
		// JoinOperator(boolean entityFlag, List<Integer> columnIDs,  Alias alias, int loopColumn, Alias drivingAlias, int drivingAliasColumn) {
		Operator join1 = new JoinOperator(false, column1IDs, aliases.get(1), aliases.get(0), 0);	
		operators.add(join1);
		
		int aggregationindexID = 3;
		
		String aggString = null;
		
		List<Alias> aggAliasList = null;
		List<Integer> aggOpColList = null;
		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_INT, aggString, aggAliasList, aggOpColList, aliases.get(1), 0);
	
		operators.add(agg);
		
	}

	private static void initEx1Queries(MetaData metadata, String queryName,
			int numThreads) {
		
		List<Alias> aliases = new ArrayList<Alias>();
		
		Alias alias0 = new Alias(0, "term");
		Alias alias1 = new Alias(1, "doc", metadata.getIndexList().get(0));
		
		aliases.add(alias0);
		aliases.add(alias1);
		
		// public MetaQuery(int queryID, String queryName, int numThreads,
		// int numBuffers, int bufferPoolSize, List<String> aliases)
		MetaQuery e1Optimal = new MetaQuery(0, queryName, numThreads, 1, aliases);
				
		metadata.getQueryList().add(e1Optimal);
		metadata.setCurrentQueryID(metadata.getQueryList().size()-1);
		
	}


	private static void initEx1Indexes(MetaData metadata) {
		
		// DT2
		int indexID = 3;
		int numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(MetaData.ENCODING_BB);
		columnEncodingsList3.add(MetaData.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().add(DT2);
	}
	
	private static void initEx1Indexes(MetaData metadata, int encoding) {
		// DT2
		int indexID = 3;
		int numColumns = 2;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(encoding);
		columnEncodingsList3.add(encoding);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(MetaData.BYTES_4);
		columnEncodedByteSizesList3.add(MetaData.BYTES_1);
		
		MetaIndex DT2 = new MetaIndex(indexID, numColumns, MetaData.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.getIndexList().put(indexID,DT2);
	}
	
	
	
	private static void runEx1(String queryName, List<Integer> selections,
			int encoding) {
		
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		initEx1Indexes(metadata, encoding);
		initEx1Queries(metadata, queryName, 1);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());

		initE1Operators(operators, query, selections);
		
		CodeGenerator.generateCode(operators, metadata);
		
	}

	private static void runEx1(String queryName, List<Integer> selections,
			boolean b) {
		
		List<Operator> operators = new ArrayList<Operator>();
		MetaData metadata = new MetaData();
		initEx1Indexes(metadata);
		initEx1Queries(metadata, queryName, 1);
		MetaQuery query = metadata.getQueryList().get(metadata.getCurrentQueryID());

		initE1Operators(operators, query, selections);
		
		CodeGenerator.generateCode(operators, metadata);
		
	}


	public static void main(String[] args) {
		
		List<Integer> selections = new ArrayList<Integer>();
		
		selections.add(105);
		
		// Pubmed 
		// Example 1 
		runEx1("test_pubmed_e1_bb", selections, true);
		
		//Example 1 Huffman
		runEx1("test_pubmed_e1_huffman", selections, MetaData.ENCODING_HUFFMAN);
		
		//Example 1 BCA
		runEx1("test_pubmed_e1_bca", selections, MetaData.ENCODING_BCA);
		
		//Example 1 UA
		runEx1("test_pubmed_e1_array", selections, MetaData.ENCODING_UA);
	
		System.out.println("Done");
	}




}
