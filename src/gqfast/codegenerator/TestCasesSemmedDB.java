package codegenerator;

import java.util.ArrayList;
import java.util.List;


public class TestCasesSemmedDB {

	private static void initSemmedDBOperators(List<Operator> operators) {
		List<Integer> selections = new ArrayList<Integer>();
		selections.add(2019);
		Operator selection1 = new SelectionOperator(selections, 0);
		operators.add(selection1);
		
		int join1indexID = 0;
		List<Integer> column1IDs = new ArrayList<Integer>();
		column1IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join1 = new JoinOperator(join1indexID, false, column1IDs, 1, 0, 0, 0);
		
		operators.add(join1);
		
		int join2indexID = 1;
		List<Integer> column2IDs = new ArrayList<Integer>();
		column2IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join2 = new JoinOperator(join2indexID, false, column2IDs, 2, 0, 1, 0);
		
		operators.add(join2);
		
		int join3indexID = 2;
		List<Integer> column3IDs = new ArrayList<Integer>();
		column3IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join3 = new JoinOperator(join3indexID, false, column3IDs, 3, 0, 2, 0);
		
		operators.add(join3);
		
		int semijoin4indexID = 3;
		List<Integer> column4IDs = new ArrayList<Integer>();
		column4IDs.add(0);
		// SemiJoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs, int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn, int drivingAliasIndexID) {
		Operator semijoin4 = new SemiJoinOperator(semijoin4indexID, false, column4IDs, 4, 0, 3, 0, 2);
		
		operators.add(semijoin4);
		

		
		int join5indexID = 4;
		List<Integer> column5IDs = new ArrayList<Integer>();
		column5IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join5 = new JoinOperator(join5indexID, false, column5IDs, 5, 0, 4, 0);
		
		operators.add(join5);
		
		int join6indexID = 5;
		List<Integer> column6IDs = new ArrayList<Integer>();
		column6IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join6 = new JoinOperator(join6indexID, false, column6IDs, 6, 0, 5, 0);
		
		operators.add(join6);
		
		int aggregationindexID = 5;
		
		String aggString = "1";
		
		List<Integer> aggAliasList = new ArrayList<Integer>();
		
		List<Integer> aggOpColList = new ArrayList<Integer>();

		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_INT, aggString, aggAliasList, aggOpColList, 6, 0, 6, 5);
	
		operators.add(agg);
		
	}

	private static void initSemmedDBOperatorsThreaded(List<Operator> operators) {
		List<Integer> selections = new ArrayList<Integer>();
		selections.add(2019);
		Operator selection1 = new SelectionOperator(selections, 0);
		operators.add(selection1);
		
		int join1indexID = 0;
		List<Integer> column1IDs = new ArrayList<Integer>();
		column1IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join1 = new JoinOperator(join1indexID, false, column1IDs, 1, 0, 0, 0);
		
		operators.add(join1);
		
		Operator threadOp = new ThreadingOperator(1);
		operators.add(threadOp);
		
		int join2indexID = 1;
		List<Integer> column2IDs = new ArrayList<Integer>();
		column2IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join2 = new JoinOperator(join2indexID, false, column2IDs, 2, 0, 1, 0);
		
		operators.add(join2);
		
		int join3indexID = 2;
		List<Integer> column3IDs = new ArrayList<Integer>();
		column3IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join3 = new JoinOperator(join3indexID, false, column3IDs, 3, 0, 2, 0);
		
		operators.add(join3);
		
		int semijoin4indexID = 3;
		List<Integer> column4IDs = new ArrayList<Integer>();
		column4IDs.add(0);
		// SemiJoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs, int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn, int drivingAliasIndexID) {
		Operator semijoin4 = new SemiJoinOperator(semijoin4indexID, false, column4IDs, 4, 0, 3, 0, 2);
		
		operators.add(semijoin4);
		

		
		int join5indexID = 4;
		List<Integer> column5IDs = new ArrayList<Integer>();
		column5IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join5 = new JoinOperator(join5indexID, false, column5IDs, 5, 0, 4, 0);
		
		operators.add(join5);
		
		int join6indexID = 5;
		List<Integer> column6IDs = new ArrayList<Integer>();
		column6IDs.add(0);
		// JoinOperator(int indexID, boolean entityFlag, List<Integer> columnIDs,  int alias, int loopColumn, int drivingAliasID, int drivingAliasColumn)
		Operator join6 = new JoinOperator(join6indexID, false, column6IDs, 6, 0, 5, 0);
		
		operators.add(join6);
		
		int aggregationindexID = 5;
		
		String aggString = "1";
		
		List<Integer> aggAliasList = new ArrayList<Integer>();
		
		List<Integer> aggOpColList = new ArrayList<Integer>();

		Operator agg = new AggregationOperator(aggregationindexID, 
				AggregationOperator.AGGREGATION_INT, aggString, aggAliasList, aggOpColList, 6, 0, 7, 5);
	
		operators.add(agg);
		
	}

	private static void initSemmedDBQueries(Metadata metadata, String queryName, int numThreads) {

		List<String> aliases = new ArrayList<String>();
		aliases.add("concept1");
		aliases.add("concept_semtype1");
		aliases.add("predication1");
		aliases.add("sentence1");
		aliases.add("predication2");
		aliases.add("concept_semtype2");
		aliases.add("concept2");
		
		MetaQuery smdbOptimal = new MetaQuery(1, queryName, numThreads,
				6, 1, aliases);
		
		metadata.queryList.add(smdbOptimal);	
		metadata.currentQueryID = metadata.queryList.size()-1;
	}


	private static void initSemmedDBIndexes(Metadata metadata) {
		// CS1
		int indexID = 0;
		int numColumns = 1;
		List<Integer> columnEncodingsList0 = new ArrayList<Integer>();
		columnEncodingsList0.add(Metadata.ENCODING_BB);
		List<Integer> columnEncodedByteSizesList0 = new ArrayList<Integer>();
		columnEncodedByteSizesList0.add(Metadata.BYTES_4);
		
		MetaIndex CS1 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList0, columnEncodedByteSizesList0);
		metadata.indexList.add(CS1);

		// PA1
		indexID = 1;
		numColumns = 1;
		List<Integer> columnEncodingsList1 = new ArrayList<Integer>();
		columnEncodingsList1.add(Metadata.ENCODING_BB);
		List<Integer> columnEncodedByteSizesList1 = new ArrayList<Integer>();
		columnEncodedByteSizesList1.add(Metadata.BYTES_4);
		
		MetaIndex PA1 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList1, columnEncodedByteSizesList1);
		metadata.indexList.add(PA1);

		// SP1
		indexID = 2;
		numColumns = 1;
		List<Integer> columnEncodingsList2 = new ArrayList<Integer>();
		columnEncodingsList2.add(Metadata.ENCODING_BB);	
		List<Integer >columnEncodedByteSizesList2 = new ArrayList<Integer>();
		columnEncodedByteSizesList2.add(Metadata.BYTES_4);
		MetaIndex SP1 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList2, columnEncodedByteSizesList2);
		metadata.indexList.add(SP1);
		
		// SP2
		indexID = 3;
		numColumns = 1;
		List<Integer> columnEncodingsList3 = new ArrayList<Integer>();
		columnEncodingsList3.add(Metadata.ENCODING_HUFFMAN);
		
		List<Integer >columnEncodedByteSizesList3 = new ArrayList<Integer>();
		columnEncodedByteSizesList3.add(Metadata.BYTES_4);
		
		MetaIndex SP2 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList3, columnEncodedByteSizesList3);
		metadata.indexList.add(SP2);
		
		// PA2
		indexID = 4;
		numColumns = 1;
		List<Integer> columnEncodingsList4 = new ArrayList<Integer>();
		columnEncodingsList4.add(Metadata.ENCODING_HUFFMAN);
		List<Integer> columnEncodedByteSizesList4 = new ArrayList<Integer>();
		columnEncodedByteSizesList4.add(Metadata.BYTES_4);
		
		MetaIndex PA2 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList4, columnEncodedByteSizesList4);
		metadata.indexList.add(PA2);

		// CS2
		indexID = 5;
		numColumns = 1;
		List<Integer> columnEncodingsList5 = new ArrayList<Integer>();
		columnEncodingsList5.add(Metadata.ENCODING_BB);
		List<Integer> columnEncodedByteSizesList5 = new ArrayList<Integer>();
		columnEncodedByteSizesList5.add(Metadata.BYTES_4);
		
		MetaIndex CS2 = new MetaIndex(indexID, numColumns, Metadata.BYTES_4, columnEncodingsList5, columnEncodedByteSizesList5);
		metadata.indexList.add(CS2);
		
	}

	public static void main(String[] args) {
		
		
		List<Operator> operators = new ArrayList<Operator>();
		Metadata metadata = new Metadata();
		
		// SemmedDB Atropine Optimal
		initSemmedDBIndexes(metadata);
		initSemmedDBQueries(metadata, "test_smdb_opt", 1);
		initSemmedDBOperators(operators);
		
		CodeGenerator.generateCode(operators, metadata);
		
		operators.clear();
		initSemmedDBQueries(metadata, "test_smdb_opt_threaded", 4);
		initSemmedDBOperatorsThreaded(operators);
		CodeGenerator.generateCode(operators, metadata);
		
	}

	
}
