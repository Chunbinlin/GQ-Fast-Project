package gqfast.codeGenerator;

import gqfast.RQNA2physical.R2P_Output;
import gqfast.RQNA2physical.RQNA2Physical;
import gqfast.global.Alias;
import gqfast.global.Global.Optypes;
import gqfast.global.MetaData;
import gqfast.global.MetaIndex;
import gqfast.global.MetaQuery;
import gqfast.global.TreeNode;
import gqfast.logical2RQNA.RelationalAlgebra2RQNA;
import gqfast.unitTest.TestTree_logical2RQNA;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class CodeGenerator {
	

	static boolean hasThreading;
	static int threadOpIndex;
	static boolean hasIntersection;
	//static List<Integer> intersectionAliasAppearanceIDs;
	//static HashMap<Alias,Integer> joinAliasAppearanceIDs; 
	static List<String> joinBufferNames; 
	static List<String> intersectionBufferNames;
	
	private static void checkThreading(MetaQuery query, List<Operator> operators) {
		if (query.getNumThreads() > 1) {
			hasThreading = true;
			for (int i=0; i<operators.size(); i++) {
				Operator currOp = operators.get(i);
				if (currOp.getType() == Optypes.THREADING_OPERATOR) {
					threadOpIndex = i;
					break;
				}
			}
		}	
		else {
			hasThreading = false;
		}
	}
	
	/*
	 * Function initialImportsAndConstants
	 * ------------------------------------
	 * Input:	
	 * 			query:	The current query meta-data that code is being generated with
	 * 			
	 * Output:	A String of code that is the very beginning of the generated code file: #include, #define, and such
	 * 
	 */
	private static String initialImportsAndConstants(MetaQuery query) {
		
	
		String initCppCode = "";
		// Initial Code
		initCppCode += "#ifndef " + query.getQueryName() + "_\n"; 
		initCppCode += "#define " + query.getQueryName() + "_\n";
		//initCppCode += "\n#include \"../fastr_index.hpp\"\n";
		initCppCode += "#include \"../gqfast_executor.hpp\"\n\n";
		initCppCode += "#include <atomic>\n";
		if (hasThreading) {
			initCppCode += "#define NUM_THREADS " + query.getNumThreads() + "\n";
		}
		//initCppCode += "#define BUFFER_POOL_SIZE " + query.getBufferPoolSize() + "\n";
		initCppCode += "\nusing namespace std;\n";
				
		return initCppCode;
	}

	/*
	 * Function openingLine
	 * ---------------------
	 * Input:
	 * 			query:	The current query meta-data that code is being generated with
	 * 			aggregation:	The aggregation operator that was assumed to be the last operator in the operator list 
	 * 	
	 * Output:	A string that is the function declaration in the generated code for the core processing function
	 * 
	 */
	private static String openingLine(MetaQuery query, int resultDataType) {

		String openingCppCode = "\nextern \"C\" ";
		if (resultDataType == AggregationOperator.AGGREGATION_INT) {
			openingCppCode += "uint32_t* ";
		}
		else if (resultDataType == AggregationOperator.AGGREGATION_DOUBLE) {
			openingCppCode += "double* ";
		}
		openingCppCode += query.getQueryName() + "(int** null_checks) {\n";
		
		return openingCppCode;
	}

	/*
	 * Function bufferInitCode
	 * ------------------------
	 * Input:
	 * 			query:	The current query meta-data that code is being generated with
	 * 
	 * Output:	The part of the generated code that initializes the buffers used in the query processing
	 * 
	 */
	private static String bufferInitCode(List<Operator> operators , List<String> globalsCppCode) {
		
		//Set<Integer> indexSet = query.getIndexIDs(); 	
		String globalCodeString = "\n";
		String bufferInitString = "\n";
		for (int i=0; i<operators.size(); i++) {
			
			Operator currOp = operators.get(i);
			if (currOp.getType() == Optypes.INTERSECTION_OPERATOR) {
				IntersectionOperator currInterOp = (IntersectionOperator) currOp;
				List<Alias> aliases = currInterOp.getAliases();
				List<Integer> colIDs = currInterOp.getColumnIDs();
				for (int j=0; j<aliases.size(); j++) {
					Alias currAlias = aliases.get(j);
					String currAliasString = currAlias.getAlias();
					//int gqFastIndexID = currAlias.getAssociatedIndex().getGQFastIndexID();
					int currColID = colIDs.get(j);
					//bufferInitString += "\n\tmax_frag = metadata.idx_max_fragment_sizes[" + gqFastIndexID + "];\n";
					//bufferInitString += "\n\tmax_frag = " + currAlias.getAssociatedIndex().getMaxFragmentSize() + ";\n";
					String bufferName = currAliasString + "_" + j + "_col" + currColID + "_intersection_buffer";
					intersectionBufferNames.add(bufferName);
					/*if (hasThreading) {
						globalCodeString += "static uint64_t** " + bufferName + ";\n";
						bufferInitString += "\t" + bufferName + " = new uint64_t*[NUM_THREADS];\n";
						bufferInitString += "\tfor (int i=0; i<NUM_THREADS; i++) {\n";
						bufferInitString += "\t\t" + bufferName + "[i] = new uint64_t[max_frag];\n";
						bufferInitString += "\t}\n";	
					}
					else {*/
					globalCodeString += "static uint64_t* " + bufferName + ";\n";
					bufferInitString += "\t" + bufferName + " = new uint64_t["+currAlias.getAssociatedIndex().getMaxFragmentSize()+"];\n";
						
					//}
				}
			}
			else if (currOp.getType() == Optypes.JOIN_OPERATOR || currOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
				Alias currAlias;
				List<Integer> colIDs;
				if (currOp.getType() == Optypes.JOIN_OPERATOR) {
					JoinOperator currJoinOp = (JoinOperator) currOp;
					currAlias = currJoinOp.getAlias();
					colIDs = currJoinOp.getColumnIDs();
				}
				else {
					SemiJoinOperator currSemiOp = (SemiJoinOperator) currOp;
					currAlias = currSemiOp.getAlias();
					colIDs = currSemiOp.getColumnIDs();
				}
				String currAliasString = currAlias.getAlias();
				//int gqFastIndexID = currAlias.getAssociatedIndex().getGQFastIndexID();
				//bufferInitString += "\n\tmax_frag = metadata.idx_max_fragment_sizes[" + gqFastIndexID + "];\n";
				//bufferInitString += "\n\tmax_frag = " + currAlias.getAssociatedIndex().getMaxFragmentSize() + ";\n";
				int maxFragmentSize = currAlias.getAssociatedIndex().getMaxFragmentSize();
				for (int currColID : colIDs) {
					String bufferName = currAliasString + "_col" + currColID + "_buffer";
					joinBufferNames.add(bufferName);
					if (hasThreading) {
						globalCodeString += "static uint64_t** " + bufferName + ";\n";
						bufferInitString += "\t" + bufferName + " = new uint64_t*[NUM_THREADS];\n";
						bufferInitString += "\tfor (int i=0; i<NUM_THREADS; i++) {\n";
						bufferInitString += "\t\t" + bufferName + "[i] = new uint64_t["+maxFragmentSize+"];\n";
						bufferInitString += "\t}\n";
					}
					else{
						globalCodeString += "static uint64_t* " + bufferName + ";\n";
						bufferInitString += "\t" + bufferName + " = new uint64_t["+maxFragmentSize+"];\n";
					}
				}
				
			}
				
		}
		globalsCppCode.add(globalCodeString);
		return bufferInitString;
		
	}
	
	/*
	 * Function bufferDeallocation
	 * ---------------------------
	 * Input:
	 * 			query:	The current query meta-data that code is being generated with
	 * 
	 * Output:	The part of the generated code that deallocates the buffers used in the query processing
	 * 
	 */
	private static String bufferDeallocations(MetaQuery query) {
		
		
		
		
		
		//Set<Integer> indexSet = query.getIndexIDs();
		//HashMap<Integer, Integer> numColumnsMap = query.getNumColumns(indexSet);
		String bufferDeallocString = "\n";
		String tabString = "\t";
		
		for (String bufferName : joinBufferNames) {
			if (hasThreading) {
				bufferDeallocString += tabString + "for (int i=0; i<NUM_THREADS; i++) {\n";
				tabString += "\t";  
				bufferDeallocString += tabString +  "delete[] " + bufferName + "[i];\n";
				tabString = tabString.substring(0, tabString.length()-1);
				bufferDeallocString += tabString + "}\n";
			}
			bufferDeallocString += tabString + "delete[] " + bufferName + ";\n";
			
			
		}
		
		/*int i=0;
		for (Integer curr : indexSet) {
			int numColumns = numColumnsMap.get(curr);
			
			for (int j=0; j<numColumns; j++) {
				bufferDeallocString += tabString + "for (int j=0; j<NUM_THREADS; j++) {\n";
				tabString += "\t";            
				bufferDeallocString += tabString + "for (int k=0; k<BUFFER_POOL_SIZE; k++) {\n";
				tabString += "\t";
				bufferDeallocString += tabString +  "delete[] index"+curr+"_col"+j+"_buffer[j][k];\n";
				tabString = tabString.substring(0, tabString.length()-1);
				bufferDeallocString += tabString + "}\n";
				bufferDeallocString += tabString + "delete[] index"+curr+"_col"+j+"_buffer[j];\n";
				tabString = tabString.substring(0, tabString.length()-1);
				bufferDeallocString += tabString + "}\n";
			}
			i++;
		}*/
		
		if (hasIntersection) {
			for (String bufferName : intersectionBufferNames) {
				bufferDeallocString += tabString + "delete[] " + bufferName + ";\n";
			}
			bufferDeallocString += tabString + "delete[] " + query.getQueryName() + "_intersection_buffer;\n";
		}
		
		return bufferDeallocString;
	}
	
	/*
	 * Function semiJoinBufferDeallocation
	 * ------------------------------------
	 * Input:
	 * 			operators:	The list of operators to the code generator for the current query
	 * 			metadata:	Contains information on the indexes and queries available to the code
	 * 						generator
	 * Output:	The part of the generated code that deallocates the semijoin buffers used in the query processing
	 * 
	 */
	
	private static String semiJoinBufferDeallocation(List<Operator> operators,
			MetaData metadata) {
		
		String semiDeallocString = "\n";
		int i = 0;
		for (Operator currentOp : operators) {
			if (currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
				SemiJoinOperator semiOp = (SemiJoinOperator)currentOp;
				String alias = semiOp.getDrivingAlias().getAlias();
				semiDeallocString += "\tdelete[] " + alias + "_bool_array;\n";
				if (hasThreading && i > threadOpIndex) {
				//	semiDeallocString += "\tdelete[] " + alias + "_spin_lock;\n";
				}
				semiDeallocString += "\n";	
			}
			i++;
		}
		return semiDeallocString;
		
	}


	/* 
	 * Function initResultArray
	 *---------------------------
	 * Input:		
	 * 			aggregation:	The aggregation operator that was assumed to be the last operator in the operator list 
	 * 
	 * Output:	The part of the code that allocates the result and null-checking arrays for the query
	 * 			
	 *
	 */
	private static String initResultArray(int resultDataType, MetaData metadata) {
		
		//int metaindexId = metadata.getAggregation_domain_index_id(); 
		//MetaIndex currIndex = metadata.getIndexMap().get(metaindexId);
		long domain = metadata.getAggregationDomain();
		//int gqFastIndexID = currIndex.getGQFastIndexID();
		/*Optypes opType = lastOp.getType();
		switch (opType) {
		case JOIN_OPERATOR:
			JoinOperator joinOp = (JoinOperator) lastOp;
			//gqFastIndexID = joinOp.getAlias().getAssociatedIndex().getGQFastIndexID();
			index_domain = joinOp.getAlias().getAssociatedIndex().getColumnDomains().get(0);
			break;
		case SEMIJOIN_OPERATOR:
			SemiJoinOperator semiJoinOp = (SemiJoinOperator) lastOp;
			//gqFastIndexID = semiJoinOp.getAlias().getAssociatedIndex().getGQFastIndexID();
			break;
		case AGGREGATION_OPERATOR:
			AggregationOperator aggregationOp = (AggregationOperator) lastOp;
			//gqFastIndexID = aggregationOp.getGQFastIndexID();
			break;
		}*/
		//String resultString = "\n\tRC = new int[metadata.idx_domains[" + gqFastIndexID + "][0]]();\n";
		
		String resultString = "\n\tRC = new int[" + domain + "]();\n";
		
		if (resultDataType == AggregationOperator.AGGREGATION_INT) {
		//	resultString += "\tR = new int[metadata.idx_domains[" + gqFastIndexID + "][0]]();\n";
			resultString += "\tR = new uint32_t[" + domain + "]();\n";
		}
		else if (resultDataType == AggregationOperator.AGGREGATION_DOUBLE) {
		//	resultString += "\tR = new double[metadata.idx_domains[" + gqFastIndexID + "][0]]();\n";
			resultString += "\tR = new double[" + domain + "]();\n";
		}
		
		if (hasThreading) {
			//resultString += "\n\tr_spin_locks = spin_locks[" + gqFastIndexID + "];\n";
			
		}
		
		return resultString;
	}
	
	/*
	 * Function initSemiJoinArray
	 * ---------------------------
	 * Input:	
	 * 			operators:	The list of operators to the code generator for the current query
	 * 			metadata:	Contains information on the indexes and queries available to the code
	 * 						generator
	 * 
	 * Output:	A String of generated code for the allocation and initialization of the boolean 
	 * 			array for semijoin operators
	 * 
	 */
	private static String initSemiJoinArray(List<Operator> operators,
			MetaData metadata, List<String> globalsCppCode) {
		
		String resultString = "\n";
		
		int i = 0;
		for (Operator currentOp: operators) {
			if (currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
				SemiJoinOperator currentSemiJoinOp = (SemiJoinOperator) currentOp;
				String alias = currentSemiJoinOp.getDrivingAlias().getAlias();
				MetaIndex currentIndex = currentSemiJoinOp.getDrivingAlias().getAssociatedIndex();
				int col = currentSemiJoinOp.getDrivingAliasColumn();
				long domain = currentIndex.getColumnDomains().get(col);
				
				String globalDeclarationString = "\nstatic atomic<bool>* " + alias + "_bool_array;\n";
				if (hasThreading && i > threadOpIndex) {
				//	globalDeclarationString += "static pthread_spinlock_t* " + alias + "_spin_lock;\n";
				}
				globalsCppCode.add(globalDeclarationString);
				
				//resultString += "\tuint64_t " + alias + "_domain = metadata.idx_domains[" + gqFastIndexID + "][0];\n";
				//resultString += "\tuint64_t " + alias + "_domain = " + domain + ";\n";
				resultString += "\t" + alias + "_bool_array = new atomic<bool>["+domain+"]();\n";
				/*if (hasThreading && i > threadOpIndex) {
					resultString += "\t" + alias + "_spin_lock = new pthread_spinlock_t["+alias+"_domain];\n";
					resultString += "\tfor (uint64_t i=0; i<"+alias+"_domain; i++) {\n";
					resultString += "\t\tpthread_spin_init(&" + alias + "_spin_lock[i], PTHREAD_PROCESS_PRIVATE);\n";
					resultString += "\t}\n";
				}*/
			}
			i++;
		}
		
		return resultString;
	}

	
	/*
	 * Function initDecodeVars
	 * -----------------------
	 * Input:	
	 * 			operators:		The list of operators to the code generator for the current query
	 * 			mainCppCode:	A list of Strings that forms the main body of cpp code
	 * 			globalsCppCode:	A list of Strings that forms the global variable declarations 
	 * 							towards the beginning of the file
	 *			metadata:		Contains information on the indexes and queries available to the code
	 * 							generator
	 * 			query:			The current query meta-data that code is being generated with
	 * 
	 * Output:	
	 * 			No output, but mainCppCode and globalsCppCode are updated with the code for the
	 * 			variable declarations and assignments for Huffman and BCA related decoding information
	 */
	private static void initDecodeVars(List<Operator> operators, List<String> mainCppCode, 
			List<String> globalsCppCode, MetaData metadata, MetaQuery query) {
		
		
		// Skips last operator since it is the aggregation
		for (int i=0; i<operators.size()-1; i++) {
			Operator currentOperator = operators.get(i);
			
			HashMap<Alias, HashMap<Integer,Boolean> > hmap = new HashMap<Alias, HashMap<Integer,Boolean>>();
			
			if (currentOperator.getType() == Optypes.JOIN_OPERATOR || currentOperator.getType() == Optypes.SEMIJOIN_OPERATOR) {
				
				int gqFastIndexID;
				MetaIndex tempIndex;
				String alias;
				List<Integer> columnIDs;
				Alias tempAlias;
				if (currentOperator.getType() == Optypes.JOIN_OPERATOR) {
					JoinOperator tempJoinOp = (JoinOperator) currentOperator;
					tempAlias = tempJoinOp.getAlias();
					tempIndex = tempAlias.getAssociatedIndex();
					gqFastIndexID = tempIndex.getGQFastIndexID();
					alias = tempAlias.getAlias();
					columnIDs = tempJoinOp.getColumnIDs();
				}
				else {
					SemiJoinOperator tempSemiJoinOp = (SemiJoinOperator) currentOperator;
					tempAlias = tempSemiJoinOp.getAlias();
					tempIndex = tempAlias.getAssociatedIndex();
					gqFastIndexID = tempIndex.getGQFastIndexID();
					alias = tempAlias.getAlias();
					columnIDs = tempSemiJoinOp.getColumnIDs();
				}
				
				if (!hmap.containsKey(tempAlias)) {
					HashMap<Integer,Boolean> cmap = new HashMap<Integer,Boolean>();
					hmap.put(tempAlias, cmap);
				}
				
				for (int j=0; j<columnIDs.size(); j++) {
						
					int columnID = columnIDs.get(j);
					
					HashMap<Integer,Boolean> tempMap = hmap.get(tempAlias);
					if (!tempMap.containsKey(columnID)) {
						tempMap.put(columnID, true);
						
						int columnEncoding = tempIndex.getColumnEncodingsList().get(columnID);
					
						if (columnEncoding == MetaData.ENCODING_BCA) {
							String nextGlobal = "\nstatic uint32_t* " + alias + "_col" + j + "_bits_info;\n";
							nextGlobal += "static uint64_t " + alias + "_col" + j + "_offset;\n";
							globalsCppCode.add(nextGlobal);
							String nextMain = "\n\t"+alias+ "_col" + j + "_bits_info = idx[" + gqFastIndexID + "]->dict[" + columnID + "]->bits_info;\n";
							nextMain += "\t"+alias+"_col" + j + "_offset = idx[" + gqFastIndexID + "]->dict[" + columnID + "]->offset;\n";
							mainCppCode.add(nextMain);
						}
						else if (columnEncoding == MetaData.ENCODING_HUFFMAN) {
							String nextGlobal = "\nstatic uint32_t* "+ alias + "_col" + j + "_huffman_tree_array;\n";
							nextGlobal += "static bool* " + alias + "_col" + j + "_huffman_terminator_array;\n";
							globalsCppCode.add(nextGlobal);
							String nextMain = "\n\t" + alias + "_col" + j + "_huffman_tree_array = idx[" + gqFastIndexID + "]->huffman_tree_array[" + columnID + "];\n";
							nextMain += "\t"+ alias + "_col" + j + "_huffman_terminator_array = idx[" + gqFastIndexID + "]->huffman_terminator_array[" + columnID + "];\n";
							mainCppCode.add(nextMain);
		 				}
					}
				}
				
			
				
			}
			else if (currentOperator.getType() == Optypes.INTERSECTION_OPERATOR) {
				hasIntersection = true;
				IntersectionOperator interOp = (IntersectionOperator) currentOperator;
				String nextGlobal = "\nstatic uint64_t* " + query.getQueryName() + "_intersection_buffer;\n";  
				globalsCppCode.add(nextGlobal);
				Alias firstAlias = interOp.getAliases().get(0);
				// Min fragment size of first detected index (any min will do, so we just use the first)
				//int gqFastIndexIDTemp = firstAlias.getAssociatedIndex().getGQFastIndexID();
				int maxFragmentSize = firstAlias.getAssociatedIndex().getMaxFragmentSize();
				String nextMain = "\n\t" + query.getQueryName() + "_intersection_buffer = new uint64_t["+maxFragmentSize+"];\n"; 
				mainCppCode.add(nextMain);
				
				
				for (int j=0; j<interOp.getAliases().size(); j++) {
					
					Alias tempAlias = interOp.getAliases().get(j);
					
					if (!hmap.containsKey(tempAlias)) {
						HashMap<Integer,Boolean> cmap = new HashMap<Integer,Boolean>();
						hmap.put(tempAlias, cmap);
					}
						
					int columnID = interOp.getColumnIDs().get(j);
						
					HashMap<Integer,Boolean> tempMap = hmap.get(tempAlias);
					if (!tempMap.containsKey(columnID)) {
						tempMap.put(columnID, true);
						
						MetaIndex tempIndex = tempAlias.getAssociatedIndex();
						int gqFastIndexID = tempIndex.getGQFastIndexID();
						String alias = tempAlias.getAlias();
						
						int columnEncoding = tempIndex.getColumnEncodingsList().get(columnID);
						
						if (columnEncoding == MetaData.ENCODING_BCA) {
							String nextGlobal2 = "\nstatic uint32_t* " + alias + "_col" + j + "_bits_info;\n";
							nextGlobal2 += "static uint64_t " + alias + "_col" + j + "_offset;\n";
							globalsCppCode.add(nextGlobal2);
							String nextMain2 = "\n\t"+alias+ "_col" + j + "_bits_info = idx[" + gqFastIndexID + "]->dict[" + columnID + "]->bits_info;\n";
							nextMain2 += "\t"+alias+"_col" + j + "_offset = idx[" + gqFastIndexID + "]->dict[" + columnID + "]->offset;\n";
							mainCppCode.add(nextMain2);
						}
						else if (columnEncoding == MetaData.ENCODING_HUFFMAN) {
							String nextGlobal2 = "\nstatic uint32_t* "+ alias + "_col" + j + "_huffman_tree_array;\n";
							nextGlobal2 += "static bool* " + alias + "_col" + j + "_huffman_terminator_array;\n";
							globalsCppCode.add(nextGlobal2);
							String nextMain2 = "\n\t" + alias + "_col" + j + "_huffman_tree_array = idx[" + gqFastIndexID + "]->huffman_tree_array[" + columnID + "];\n";
							nextMain2 += "\t"+ alias + "_col" + j + "_huffman_terminator_array = idx[" + gqFastIndexID + "]->huffman_terminator_array[" + columnID + "];\n";
							mainCppCode.add(nextMain2);
		 				}
					}
					 					
				}
				
				
			}		
		}
		
	}
	

	/*
	 * Function initialDeclarations
	 * -----------------------------
	 * Input:	
	 * 			globalsCppCode:	A list of Strings that forms the global variable declarations 
	 * 							towards the beginning of the file
	 * 			aggregation:	The aggregation operator that was assumed to be the last operator in the operator list 
	 * 			query:			The current query meta-data that code is being generated with
	 * 			
	 * 	Output:
	 * 			None, but globalsCppCode is updated with the global declarations for the threading related,
	 * 			result array, and null-check array
	 */
	private static void initialDeclarations(List<String> globalsCppCode,
			int resultDataType, MetaQuery query, List<Operator> operators) {
		
		String resultsGlobals = "";
		
		if (hasThreading) {
			String arguments = "\nstatic args_threading arguments[NUM_THREADS];\n";
			globalsCppCode.add(arguments);
			resultsGlobals += "\n";
			ThreadingOperator threadOp = (ThreadingOperator) operators.get(threadOpIndex);
			Alias drivingAlias = threadOp.getDrivingAlias();
			for (int i=0; i<threadOpIndex; i++) {
				Operator currOp = operators.get(i);
				Optypes type = currOp.getType();
				if (type == Optypes.JOIN_OPERATOR || type == Optypes.SEMIJOIN_OPERATOR) {
				
					Alias currAlias;
					List<Integer> columnIDs;
					if (type == Optypes.JOIN_OPERATOR) {
						JoinOperator tempJoinOp = (JoinOperator) currOp;
						currAlias = tempJoinOp.getAlias();
						columnIDs = tempJoinOp.getColumnIDs();
					}
					else {
						SemiJoinOperator tempSemiOp = (SemiJoinOperator) currOp;
						currAlias = tempSemiOp.getAlias();
						columnIDs = tempSemiOp.getColumnIDs();
					}

					if (currAlias != drivingAlias) {
						MetaIndex currIndex = currAlias.getAssociatedIndex();
						String currAliasString = currAlias.getAlias();
						for (int currColumn : columnIDs) {
							int currBytesSize = currIndex.getColumnEncodedByteSizesList().get(currColumn); 
							String elementName = currAliasString + "_col" + currColumn + "_element";
							resultsGlobals += "static " + getElementPrimitive(currBytesSize) + " " + elementName + ";\n";
						}
					}
				}
			}
		}
		

		
		if (resultDataType == AggregationOperator.AGGREGATION_INT)  {		
			resultsGlobals += "\nstatic uint32_t* R;\n";
		}
		else if (resultDataType == AggregationOperator.AGGREGATION_DOUBLE) {
			resultsGlobals += "\nstatic double* R;\n";	
		}
		resultsGlobals += "static int* RC;\n";
		if (hasThreading) {
			
			//resultsGlobals += "\nstatic pthread_spinlock_t* r_spin_locks;\n";
		}
		
		globalsCppCode.add(resultsGlobals);
		
	}

	private static String generateDecodeFunctionBodyEntityTable(
			String pointerName, String elementName, String alias,
			int currentEncoding, int currentCol) {
		
		String function = "\n";
		String tabString = "\t";
		
		if (currentEncoding == MetaData.ENCODING_BB) {
			function += tabString + elementName + " = 0;\n";
			function += "\n" + tabString + "int shiftbits = 0;\n";
			function += tabString + "do { \n";
			tabString += "\t";
			function += tabString + "uint32_t next_seven_bits = *" + pointerName + " & 127;\n";
			function += tabString + "next_seven_bits = next_seven_bits << shiftbits;\n";
			function += tabString + elementName +" |= next_seven_bits;\n";
			function += tabString + "shiftbits += 7;\n";
			tabString = tabString.substring(0, tabString.length()-1);
			function += tabString + "} while (!(*" + pointerName + "++ & 128));\n";
		}
		else if (currentEncoding == MetaData.ENCODING_HUFFMAN) {
			function += tabString + "int mask = 0x100;\n";
			function += tabString + "bool* terminator_array = &("+alias+"_col"+currentCol+"_huffman_terminator_array[0]);\n";
			function += tabString + "uint32_t* tree_array = &("+alias+"_col"+currentCol+"_huffman_tree_array[0]);\n";
			function += "\n" + tabString + "while(!*terminator_array) {\n";
			tabString += "\t";
			function += "\n" + tabString + "char direction = *" + pointerName + " & (mask >>= 1);\n";
			function += "\n" + tabString + "if (mask == 1) {\n";
			tabString += "\t";
			function += tabString + "mask = 0x100;\n";
			function += tabString + pointerName + "++;\n";
			tabString = tabString.substring(0, tabString.length()-1);
			function += tabString + "}\n";
			function += "\n" + tabString + "terminator_array += *tree_array;\n";
			function += tabString + "tree_array += *tree_array;\n";
			function += "\n" + tabString + "if (direction) {\n";
			tabString += "\t";
			function += tabString + "terminator_array++;\n";
			function += tabString + "tree_array++;\n";
			tabString = tabString.substring(0, tabString.length()-1);
			function += tabString + "}\n";
			tabString = tabString.substring(0, tabString.length()-1);
			function += tabString + "}\n";
			function += "\n" + tabString + elementName + " = *tree_array;\n";
		}
		else if (currentEncoding == MetaData.ENCODING_BCA) {
			function += tabString + elementName + " = " + alias + "_col" + currentCol + "_bits_info[1];\n";
			function += tabString + elementName + " &= *" + pointerName + ";\n";
			function += tabString + elementName + " += " + alias + "_col" + currentCol + "_offset;\n";
		}
		else if (currentEncoding == MetaData.ENCODING_UA) {
			function += tabString + elementName + " = *" + pointerName + ";\n"; 
		}
		return function;
	}
	
	/*
	 * Function generateDecodeFunctionBody
	 * ------------------------------------
	 * Input:
	 * 			preThreading:				'true' if the function is being generated before threading begins 
	 * 										in the query processing, 'false' if it occurs after  
	 * 			indexID: 					the id of the current index
	 * 			alias:   					The alias name of the current operator in the query
	 * 			currentEncoding: 			the encoding type as represented by an int constant
	 * 			columnIteration:			the current column iteration
	 * 			currentCol:					The id of the current column (could be different than 'columnIteration')
	 * 			sizeName:					A string of the cpp that is the variable name for the current fragment cardinality
	 * 			currentFragmentBytesName:	A string of cpp that is the variable name for the current fragment bytes
	 * 			pointerName:				A string of cpp that is the variable name for the current fragment pointer
	 * 			currByteSize:				Specifies how many bytes an uncompressed element uses
	 * Output:
	 * 			A string that represents the body of the function in cpp that is the decoding of a fragment
	 * 
	 */
	private static String generateDecodeFunctionBody(boolean preThreading, int gqFastIndexID, String alias,
			int currentEncoding, int columnIteration, int currentCol, String sizeName,
			String currentFragmentBytesName, String pointerName,
			int currentByteSize, Alias currAlias, boolean intersectionFlag) {
		
		String function = "\n";
		String tabString = "\t";
		
				
		//int currAppearance = joinAliasAppearanceIDs.get(currAlias);
		String bufferArraysPart;
		if (!intersectionFlag) {
			bufferArraysPart = alias + "_col" + currentCol + "_buffer";
		}
		else {
			bufferArraysPart = alias + "_col" + currentCol + "_intersection_buffer";
		}
		if (hasThreading && !intersectionFlag) {
			if (preThreading) {
				bufferArraysPart += "[0]";
				//bufferArraysPart = "index" + gqFastIndexID + "_col" + currentCol + "_buffer[0][" + currPool + "]";
			}
			else {
				bufferArraysPart += "[thread_id]";
				//bufferArraysPart = "index" + gqFastIndexID + "_col" + currentCol + "_buffer[thread_id][" + currPool + "]";
			}
		}
		if (columnIteration == 0) {
			// Size is initially 0, is passed by reference, and will calculated in the function
			if (currentEncoding == MetaData.ENCODING_UA) {			
				function += tabString + sizeName + " = " + currentFragmentBytesName + "/" + currentByteSize + ";\n"; 
				function += "\n" + tabString + "for (uint32_t i=0; i<" + sizeName + "; i++) {\n";
				tabString += "\t";
				function += tabString + bufferArraysPart + "[i] = *" + pointerName + "++;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
					
			}
			else if (currentEncoding == MetaData.ENCODING_BB) {
							
				function += tabString + bufferArraysPart + "[0] = 0;\n";
				function += "\n" + tabString + "int shiftbits = 0;\n";
				function += tabString + "do { \n";
				tabString += "\t";
				
				function += tabString + currentFragmentBytesName + "--;\n";
				function += tabString + "uint32_t next_seven_bits = *" + pointerName + " & 127;\n";
				function += tabString + "next_seven_bits = next_seven_bits << shiftbits;\n";
				function += tabString + bufferArraysPart + "[0] |= next_seven_bits;\n";
				function += tabString + "shiftbits += 7;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "} while (!(*" + pointerName + "++ & 128));\n";
				function += tabString + sizeName + "++;\n";
				
				function += "\n" + tabString + "while (" + currentFragmentBytesName + " > 0) {\n";
				tabString += "\t";
				function += tabString + "shiftbits = 0;\n";
				function += tabString + "uint32_t result = 0;\n";
				function += "\n" + tabString + "do {\n";
				tabString += "\t";
				function += "\n" + tabString + currentFragmentBytesName + "--;\n";
				function += tabString + "uint32_t next_seven_bits = *" + pointerName + " & 127;\n";
				function += tabString + "next_seven_bits = next_seven_bits << shiftbits;\n";
				function += tabString + "result |= next_seven_bits;\n";
				function += tabString + "shiftbits += 7;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += "\n" + tabString + "} while (!(*" + pointerName + "++ & 128));\n";
				function += tabString + bufferArraysPart + "[" + sizeName + "] = " + bufferArraysPart + "[" + sizeName + "-1]+1+result;\n";
				function += tabString + sizeName + "++;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
			}
			else if (currentEncoding == MetaData.ENCODING_HUFFMAN) {
				
				function += tabString + "bool* terminate_start = &("+ alias + "_col" + currentCol + "_huffman_terminator_array[0]);\n" ;
				function += tabString + "uint32_t* tree_array_start = &("+ alias + "_col" + currentCol + "_huffman_tree_array[0]);\n";
				
				function += "\n" + tabString + "int mask = 0x100;\n";
				function += "\n" + tabString + "while ("+ currentFragmentBytesName + " > 1) {\n";
				tabString += "\t";
				function += "\n" + tabString + "bool* terminator_array = terminate_start;\n";
				function += tabString + "uint32_t* tree_array = tree_array_start;\n";
				function += "\n" + tabString + "while(!*terminator_array) { \n";
				tabString += "\t";
				function += "\n" + tabString + "char direction = *" + pointerName + " & (mask >>= 1);\n";
				function += "\n" + tabString + "if (mask == 1) { \n";
				tabString += "\t";
				function += tabString + "mask = 0x100;\n";
				function += tabString + pointerName + "++;\n";
				function += tabString + currentFragmentBytesName + "--;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				function += "\n" + tabString + "terminator_array += *tree_array;\n";
				function += tabString + "tree_array += *tree_array;\n";
				function += "\n" + tabString + "if (direction) {\n";
				tabString += "\t";
				function += tabString + "terminator_array++;\n";
				function += tabString + "tree_array++;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				function += "\n" + tabString + bufferArraysPart + "["+ sizeName + "++] = *tree_array;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				
				function += "\n" + tabString + "if (mask != 0x100) {\n";
				tabString += "\t";
				function += tabString + "int bit_pos = mask;\n";
				function += tabString + "unsigned char last_byte = *" + pointerName + ";\n";
				function += tabString + "while (bit_pos > 1) {\n";
				tabString += "\t";
				function += tabString + "unsigned char bit = last_byte & (bit_pos >>= 1);\n";
				function += tabString + "if (bit) {\n";
				tabString += "\t";
				function += tabString + "bool* terminator_array = terminate_start;\n";
				function += tabString + "uint32_t* tree_array = tree_array_start;\n";
				function += "\n" + tabString + "while (!*terminator_array) {\n";
				tabString += "\t";
				function += tabString + "char direction = *" + pointerName + " & (mask >>= 1);\n";
				function += "\n" + tabString + "terminator_array += *tree_array;\n";
				function += tabString + "tree_array += *tree_array;\n";
				function += "\n" + tabString + "if (direction) {\n";
				tabString += "\t";
				function += tabString + "terminator_array++;\n";
				function += tabString + "tree_array++;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				function += "\n" + tabString + bufferArraysPart + "["+ sizeName + "++] = *tree_array;\n";
				function += tabString + "bit_pos = mask;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
			}
			else if (currentEncoding == MetaData.ENCODING_BCA) {
				
				String bitsInfoPrefix = alias + "_col" + currentCol + "_bits_info";
				String offsetString = alias + "_col" + currentCol + "_offset";
				function += tabString + sizeName + " = " + currentFragmentBytesName + "* 8 / " + bitsInfoPrefix + "[0];\n";
				function += tabString + "int bit_pos = 0;\n";
				function += tabString + "for (uint32_t i=0; i<" + sizeName + "; i++) {\n";
				tabString += "\t";
				function += tabString + "uint32_t encoded_value = " + bitsInfoPrefix + "[1] << bit_pos;\n";
				function += tabString + "uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(" + pointerName + ");\n";
				function += tabString + "encoded_value &= *next_8_ptr;\n";
				function += tabString + "encoded_value >>= bit_pos;\n";
				function += "\n" + tabString + pointerName + " += (bit_pos + " + bitsInfoPrefix + "[0]) / 8;\n";
				function += tabString + "bit_pos = (bit_pos + " + bitsInfoPrefix + "[0]) % 8;\n";
				
				function += tabString + bufferArraysPart + "[i] = encoded_value + " + offsetString + ";\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				
			}
			
		}
		else {
			// Size is pre-calculated and will be used to control the iteration
			if (currentEncoding == MetaData.ENCODING_UA) {
				function += tabString + "for (uint32_t i=0; i<" + sizeName + "; i++) {\n";
				tabString += "\t";
				function += tabString + bufferArraysPart + "[i] = *" + pointerName + "++;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				
			}
			else if (currentEncoding == MetaData.ENCODING_BB) {
				
				
				function += tabString + bufferArraysPart + "[0] = 0;\n";
				function += "\n" + tabString + "int shiftbits = 0;\n";
				function += tabString + "do { \n";
				tabString += "\t";
				function += tabString + "uint32_t next_seven_bits = *" + pointerName + " & 127;\n";
				function += tabString + "next_seven_bits = next_seven_bits << shiftbits;\n";
				function += tabString + bufferArraysPart + "[0] |= next_seven_bits;\n";
				function += tabString + "shiftbits += 7;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "} while (!(*" + pointerName + "++ & 128));\n";
				function += tabString + "int size = 1;\n";
				function += tabString + sizeName + "--;\n";
				function += "\n" + tabString + "while (" + sizeName + " > 0) {\n";
				tabString += "\t";
				function += tabString + "shiftbits = 0;\n";
				function += tabString + "uint32_t result = 0;\n";
				function += "\n" + tabString + "do {\n";
				tabString += "\t";
				function += tabString + "uint32_t next_seven_bits = *" + pointerName + " & 127;\n";
				function += tabString + "next_seven_bits = next_seven_bits << shiftbits;\n";
				function += tabString + "result |= next_seven_bits;\n";
				function += tabString + "shiftbits += 7;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += "\n" + tabString + "} while (!(*" + pointerName + "++ & 128));\n";
				function += tabString + bufferArraysPart + "[size] = " + bufferArraysPart + "[size-1]+1+result;\n";
				function += tabString + "size++;\n";
				function += tabString + sizeName + "--;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
			}
			else if (currentEncoding == MetaData.ENCODING_HUFFMAN) {
				
				function += tabString + "bool* terminate_start = &("+alias+ "_col" + currentCol + "_huffman_terminator_array[0]);\n" ;
				function += tabString + "uint32_t* tree_array_start = &("+alias + "_col" + currentCol + "_huffman_tree_array[0]);\n";
				
				function += "\n" + tabString + "int mask = 0x100;\n";
				function += "\n" + tabString + "for (uint32_t i=0; i<"+sizeName+"; i++) {\n";
				tabString += "\t";
				function += "\n" + tabString + "bool* terminator_array = terminate_start;\n";
				function += tabString + "uint32_t* tree_array = tree_array_start;\n";
				function += "\n" + tabString + "while(!*terminator_array) { \n";
				tabString += "\t";
				function += "\n" + tabString + "char direction = *" + pointerName + " & (mask >>= 1);\n";
				function += "\n" + tabString + "if (mask == 1) { \n";
				tabString += "\t";
				function += tabString + "mask = 0x100;\n";
				function += tabString + pointerName + "++;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				function += "\n" + tabString + "terminator_array += *tree_array;\n";
				function += tabString + "tree_array += *tree_array;\n";
				function += "\n" + tabString + "if (direction) {\n";
				tabString += "\t";
				function += tabString + "terminator_array++;\n";
				function += tabString + "tree_array++;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				function += "\n" + tabString + bufferArraysPart + "[i] = *tree_array;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				
			}
			else if (currentEncoding == MetaData.ENCODING_BCA) {
				String bitsInfoPrefix = alias + "_col" + currentCol + "_bits_info";
				String offsetString = alias + "_col" + currentCol + "_offset";
				function += tabString + "int bit_pos = 0;\n";
				function += tabString + "for (uint32_t i=0; i<" + sizeName + "; i++) {\n";
				tabString += "\t";
				function += tabString + "uint32_t encoded_value = " + bitsInfoPrefix + "[1] << bit_pos;\n";
				function += tabString + "uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(" + pointerName + ");\n";
				function += tabString + "encoded_value &= *next_8_ptr;\n";
				function += tabString + "encoded_value >>= bit_pos;\n";
				function += "\n" + tabString + pointerName + " += (bit_pos + " + bitsInfoPrefix + "[0]) / 8;\n";
				function += tabString + "bit_pos = (bit_pos + " + bitsInfoPrefix + "[0]) % 8;\n";
				
				function += tabString + bufferArraysPart + "[i] = encoded_value + " + offsetString + ";\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				
			}
			
		}
		
		
		
		return function;
		
	}
	
	
	private static String generateIntersectionFunctionBody(
			IntersectionOperator interOp, MetaQuery query, int[] poolNums,
			List<String> sizeNames, String intersectionSizeName) {
		
		String functionBody = new String();
		String queryName = query.getQueryName();
		int numInputs = sizeNames.size();
		
		//int[] inputGQFastIndexIDs = new int[numInputs];
		String[] aliasStrings = new String[numInputs];
		int[] inputColIDs = new int[numInputs];
		//int[] inputAppearanceID = new int[numInputs];
		for (int i=0; i<numInputs; i++) {
			Alias currAlias = interOp.getAliases().get(i);
			aliasStrings[i] = currAlias.getAlias();
			//inputGQFastIndexIDs[i] = currAlias.getAssociatedIndex().getGQFastIndexID();
			inputColIDs[i] = interOp.getColumnIDs().get(i);
			//inputAppearanceID[i] = intersectionAliasAppearanceIDs.get(i);
		}
		
		String tabString = "\t";
		for (String currSizeName : sizeNames) {
			functionBody += "\n" + tabString + "if (" + currSizeName + " == 0) { return; }\n";
		}
		functionBody += "\n" + tabString + "uint32_t intersection_index = 0;\n";
		functionBody += tabString + "uint32_t* its = new uint32_t[" + numInputs + "]();\n";
		functionBody += tabString + "bool end = false;\n";
		
		functionBody += "\n" + tabString + "while(!end) {\n";
		tabString += "\t";
		functionBody += "\n" + tabString + "bool match = true;\n";
		functionBody += tabString + "while (1) {\n";
		tabString += "\t";
		for (int i=1; i<numInputs;i++) {
			functionBody += tabString + "if (" + aliasStrings[0]  + "_0_col" + inputColIDs[0] + "_intersection_buffer[its[0]] ";
			functionBody += " != " + aliasStrings[i] + "_" + i + "_col" + inputColIDs[i] + "_intersection_buffer[its[" + i + "]]) {\n";
			tabString += "\t";
			functionBody += tabString + "match = false;\n";
			functionBody += tabString + "break;\n";
			tabString = tabString.substring(0, tabString.length()-1);
			functionBody += tabString + "}\n";
		}
		functionBody += "\n" + tabString + "break;\n";
		tabString = tabString.substring(0, tabString.length()-1);
		functionBody += tabString + "}\n";
		
		functionBody += "\n" + tabString + "if (match) {\n";
		tabString += "\t";
		functionBody += tabString + queryName + "_intersection_buffer[intersection_index++] = " +
						aliasStrings[0] + "_0_col" + inputColIDs[0] + "_intersection_buffer[its[0]];\n";
		functionBody += tabString + "while(1) {\n";
		tabString += "\t";
		for (int i=0; i<numInputs; i++) {
			functionBody += tabString + "if (++its["+ i +"] == " + sizeNames.get(i) + ") {\n";
			tabString += "\t";
			functionBody += tabString + "end = true;\n";
			functionBody += tabString + "break;\n";
			tabString = tabString.substring(0, tabString.length()-1);
			functionBody += tabString + "}\n";
		}
		functionBody += "\n" + tabString + "break;\n";
		tabString = tabString.substring(0, tabString.length()-1);
		functionBody += tabString + "}\n";
		tabString = tabString.substring(0, tabString.length()-1);
		functionBody += tabString + "}\n";
		functionBody += tabString + "else {\n";
		tabString += "\t";
		functionBody += "\n" + tabString + "uint64_t smallest = "+aliasStrings[0] + 
				 "_0_col" + inputColIDs[0] + "_intersection_buffer[its[0]];\n";
		functionBody += tabString + "int index_of_smallest = 0;\n";
		functionBody += tabString + "uint32_t fragment_size_of_smallest = " + sizeNames.get(0) + ";\n";
		for (int i=1; i<numInputs; i++) {
			functionBody += "\n" + tabString + "if (smallest > " + aliasStrings[i] + "_" + i + "_col" + inputColIDs[i] + "_intersection_buffer[its["+i+"]]) {\n";
			tabString += "\t";
			functionBody += tabString + "smallest = " + aliasStrings[i] + "_"+ i + "_col" + inputColIDs[i] +
					"_intersection_buffer[its["+i+"]];\n";
			functionBody += tabString + "index_of_smallest = " + i + ";\n";
			functionBody += tabString + "fragment_size_of_smallest = " + sizeNames.get(i) + ";\n";
			tabString = tabString.substring(0, tabString.length()-1);
			functionBody += tabString + "}\n";
		}
		functionBody += "\n" + tabString + "if (++its[index_of_smallest] == fragment_size_of_smallest) {\n";
		tabString += "\t";
		functionBody += tabString + "end = true;\n";
		tabString = tabString.substring(0, tabString.length()-1);
		functionBody += tabString + "}\n";
		tabString = tabString.substring(0, tabString.length()-1);
		functionBody += tabString + "}\n";
		tabString = tabString.substring(0, tabString.length()-1);
		functionBody += tabString + "}\n";
		
 		functionBody += "\n" + tabString + "delete[] its;\n";
		functionBody += tabString + intersectionSizeName + " = intersection_index;\n";
		
		return functionBody;
		
	}
	
	/* 	Function joinGenerateDecodeFragmentFunction
	 * ----------------------------------------------
	 *	Input: 
	 *			preThreading:				'true' if the function is being generated before threading begins 
	 * 			k: 							The curent column iteration	
	 * 			tabString:					A string that keeps track of the tabbing of the generated code 
	 * 										(for readability)
	 * 			query: 						metadata for the current query
	 * 			functionHeadersCppCode:		A list of strings that make up the headers of the functions that are 
	 * 										generated (except the original function)
	 * 			functionCppCode: 			A list of strings, each of which is a function and function body
	 * 			currentCol:					The current column ID (note that this could potentially be different than 
	 * 										'columnIteration' if certain columns were omitted)
	 * 			currentFragmentRow:			 String of the cpp that is the variable name for the current row
	 * 			currentFragmentBytesName:	A String of cpp that is the variable name for the current fragment bytes 
	 * 			entityFlag:					'true' if current index is an entity table, 'false' if it is a relationship table
	 *			currentAlias:				The Alias object that represents the alias on which the function is being generated 
	 * 
	 *	Output:
	 *			A String that continues the cpp code for the original function and deals with the decoding portion of 
	 *			the join evaluation for a particular column.
	 */
	private static String joinGenerateDecodeFragmentFunction(boolean preThreading, int k, StringBuilder tabString, MetaQuery query, 
			List<String> functionHeadersCppCode, List<String> functionsCppCode, int currentCol,
			String currentFragmentBytesName, boolean entityFlag, Alias currentAlias, Operator currOp) {
		
		String mainString = "\n";
		String currFunctionHeader = new String();
		String currFunction = new String();
		String currentFragmentRow = "row_" + currentAlias.getAlias();
		String alias = currentAlias.getAlias();
		MetaIndex currMetaIndex = currentAlias.getAssociatedIndex();
		int gqFastIndexID = currMetaIndex.getGQFastIndexID();
		int currentBytesSize = currMetaIndex.getColumnEncodedByteSizesList().get(currentCol);
		int currentEncoding = currMetaIndex.getColumnEncodingsList().get(currentCol);
		
		String pointerName = alias + "_col" + currentCol + "_ptr";
		String pointerString = new String();
		String functionParameters = "(";
		if (!preThreading && !entityFlag) {
			functionParameters += "int thread_id, ";
		}
		// UA pointer points to type of size of element
		if (currentEncoding == MetaData.ENCODING_UA) {
			switch (currentBytesSize) {
			case MetaData.BYTES_1: 
				pointerString = "unsigned char* " + pointerName + 
				" = &(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]);\n";
				functionParameters += "unsigned char* " + pointerName;
				break;
			case MetaData.BYTES_2: 
				pointerString = "uint16_t* " + pointerName + 
				" = reinterpret_cast<uint16_t *>(&(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
				functionParameters += "" + "uint16_t* " + pointerName;
				break;
			case MetaData.BYTES_4:
				pointerString = "uint32_t* " + pointerName + 
				" = reinterpret_cast<uint32_t *>(&(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
				functionParameters += "uint32_t* " + pointerName;
				break;
			case MetaData.BYTES_8:
				pointerString = "uint64_t* " + pointerName +
				" = reinterpret_cast<uint64_t *>(&(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
				functionParameters += "uint64_t* " + pointerName;
				break;
			}
		}
		else if (entityFlag && currentEncoding == MetaData.ENCODING_BCA){
			pointerString = "uint64_t* " + pointerName +
					" = reinterpret_cast<uint64_t *>(&(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
			functionParameters += "uint64_t* " + pointerName;
		}
		else {
			pointerString = "unsigned char* " + pointerName + 
					" = &(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]);\n";
			functionParameters += "unsigned char* " + pointerName;
		}
		
		mainString += tabString + pointerString;
		
		
		String functionName = query.getQueryName() +"_"+ alias + "_col" + currentCol;

		switch (currentEncoding) {

		case MetaData.ENCODING_UA:	{
			functionName += "_decode_UA";
			break;
		}
		case MetaData.ENCODING_BCA: {
			functionName += "_decode_BCA";
			break;
		}
		case MetaData.ENCODING_BB: {
			functionName += "_decode_BB";
			break;
		}
		case MetaData.ENCODING_HUFFMAN: {
			functionName += "_decode_Huffman";
			break;
		}

		}

		if (!preThreading && !entityFlag) {
			functionName += "_threaded";
		}
		
		
		if (entityFlag) {
			String elementName = alias + "_col" + currentCol + "_element";
			
			if (hasThreading && !preThreading || !hasThreading) {
				mainString +=  tabString + getElementPrimitive(currentBytesSize) +" " + elementName + ";\n";
				functionParameters += ", " + getElementPrimitive(currentBytesSize) + " & " + elementName;
			}
			
			functionParameters += ")";
			
			currFunction += "\nvoid " + functionName + functionParameters + " {\n";
			currFunctionHeader += "\nextern inline void " + functionName + functionParameters + " __attribute__((always_inline));\n";
			
			if (hasThreading && !preThreading || !hasThreading) {
				mainString += tabString + functionName + "(" + pointerName + ", " + elementName + ");\n";
			}
			else {
				mainString += tabString + functionName + "(" + pointerName + ");\n";
			}

			currFunction += generateDecodeFunctionBodyEntityTable(pointerName, elementName, alias, currentEncoding, currentCol);
			currFunction += "}\n";
			
			
		}
		else {
			String sizeName = alias + "_fragment_size";
	
			// First column's decoding determines the fragment size for all subsequent column decodings


			if (k == 0) {
				mainString += tabString + "uint32_t " + sizeName + " = 0;\n";
				functionParameters += ", uint32_t " + currentFragmentBytesName;
				functionParameters += ", uint32_t & " + sizeName + ")";
			}
			else {
				functionParameters += ", uint32_t " + sizeName + ")";
			}

			currFunction += "\nvoid " + functionName + functionParameters + " {\n";
			currFunctionHeader += "\nextern inline void " + functionName + functionParameters + " __attribute__((always_inline));\n";
			
			if (preThreading) {
				mainString += tabString + functionName + "(" + pointerName;
			}
			else {
				mainString += tabString + functionName + "(thread_id, " + pointerName;
			}
			if (k == 0 ) {
				mainString += ", " + currentFragmentBytesName; 
			}
			mainString += ", " + sizeName + ");\n";

			//int indexID = currMetaIndex.getIndexID();
			//int aliasID = currentAlias.getAliasID();
			//int currPool = ++bufferPoolTrackingArray[indexID][currentCol];
			//query.setBufferPoolID(aliasID, currentCol, currPool);
			
			currFunction += generateDecodeFunctionBody(preThreading, gqFastIndexID, alias, currentEncoding, k, 
					currentCol, sizeName, currentFragmentBytesName, pointerName, currentBytesSize, currentAlias, false);
			currFunction += "}\n";

			

		}
		
		functionHeadersCppCode.add(currFunctionHeader);
		functionsCppCode.add(currFunction);
		return mainString;
	}
	

	/*
	 * Function getElementPrimitive
	 * ----------------------
	 * Input:
	 * 			colBytes:	 The size in bytes an associated value would use
	 * Output:
	 * 			A string that gives a primitive type for the corresponding size
	 * 			Returns null if no primitive is known.
	 * 
	 */
	private static String getElementPrimitive(int colBytes) {
		switch (colBytes) {
		case MetaData.BYTES_1:	return "unsigned char";
		case MetaData.BYTES_2:	return "uint16_t";
		case MetaData.BYTES_4:	return "uint32_t";
		case MetaData.BYTES_8:	return "uint64_t";
		}
		return null;
	}
	
	/*
	 * Function getIndexPrimitive
	 * ----------------------
	 * Input:
	 * 			colBytes:	 The size in bytes an associated value would use
	 * Output:
	 * 			A string that gives a primitive type for the corresponding size (unsigned). 
	 * 			Returns null if no primitive is known.
	 * 
	 */
	private static String getIndexPrimitive(int colBytes) {
		switch (colBytes) {
		case MetaData.BYTES_1:	return "unsigned char";
		case MetaData.BYTES_2:	return "uint16_t";
		case MetaData.BYTES_4:	return "uint32_t";
		case MetaData.BYTES_8:	return "uint64_t";
		}
		return null;
	}
	
	/*
	 * Function evaluatePreviousJoinEntityTable
	 * ----------------------------------------
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */
	private static String evaluatePreviousJoinEntityTable(Operator currentOp, MetaQuery query, boolean threadID, StringBuilder tabString, 
			int[] closingBraces, int drivingAliasCol, List<Integer> columnIDs, int indexByteSize, boolean preThreading, List<String> functionHeadersCppCode, 
			List<String> functionsCppCode, boolean entityFlag, Alias currentAlias, Alias drivingAlias) {
		String mainString = new String();
		
		String drivAliasString = drivingAlias.getAlias();
	
		// SemiJoin Check
		if (currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
			//int drivingAliasID = drivingAlias.getAliasID();
			//int drivingPool = query.getBufferPoolID(drivingAliasID, drivingAliasCol);
			//int drivingGQFastIndexID = drivingAlias.getAssociatedIndex().getGQFastIndexID();
			//int drivingAppearance = joinAliasAppearanceIDs.get(drivingAlias);
			if (hasThreading) {
				if (threadID) {
					mainString += "\n" + tabString + "if (!(" + drivAliasString 
							+ "_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer[thread_id]["+drivAliasString+ "_it]])) {\n";
						closingBraces[0]++;
						tabString.append("\t");
						mainString += tabString + drivAliasString + 
							"_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer[thread_id]["+drivAliasString+ "_it]] = true;\n";
					
				}
				else {
					mainString += "\n" + tabString + "if (!(" + drivAliasString 
						+ "_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer[0]["+drivAliasString+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivAliasString + 
						"_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer[0]["+drivAliasString+ "_it]] = true;\n";
				}
			}
			else {
				mainString += "\n" + tabString + "if (!(" + drivAliasString 
						+ "_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer["+drivAliasString+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivAliasString + 
						"_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer["+drivAliasString+ "_it]] = true;\n";
			}
		}
		
		String elementString = drivAliasString  + "_col" + drivingAliasCol + "_element";
		mainString += "\n" + tabString + getIndexPrimitive(indexByteSize) + "* ";

		String currentFragmentRow = "row_" + currentAlias.getAlias();
		int gqFastIndexID = currentAlias.getAssociatedIndex().getGQFastIndexID();
		String currAliasString = currentAlias.getAlias();
		mainString += currentFragmentRow + " = idx[" + gqFastIndexID + "]->" +
				"index_map["+ elementString +"];\n"; 
		
		String currentFragmentBytesName = new String();
		for (int k=0; k<columnIDs.size(); k++) {
			int currentCol = columnIDs.get(k);

			if (k == 0) {
				currentFragmentBytesName = currAliasString + "_col" + currentCol + "_bytes";
				mainString += tabString + "uint32_t " + currentFragmentBytesName + " = " +
						"idx[" + gqFastIndexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}
		
			mainString += joinGenerateDecodeFragmentFunction(preThreading, k, tabString, query, functionHeadersCppCode, functionsCppCode, 
					currentCol, currentFragmentBytesName, entityFlag, currentAlias, currentOp);
			
		}
		
		return mainString;
	}
	

	/*
	 * Function evaluePreviousJoinRelationshipTable
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */
	private static String evaluatePreviousJoinRelationshipTable(Operator currentOp, MetaQuery query, boolean threadID, StringBuilder tabString, 
			int[] closingBraces, int drivingAliasCol, List<Integer> columnIDs, int indexByteSize, boolean preThreading, List<String> functionHeadersCppCode, 
			List<String> functionsCppCode, boolean entityFlag, List<Integer> previousColumnIDs,
			boolean justStartedThreading, Alias currentAlias, Alias drivingAlias, Alias previousAlias) {
		String mainString = new String();
		
		String prevAliasString = previousAlias.getAlias();
		
		if (justStartedThreading) {
			mainString += "\n" + tabString + "for (; " +
					prevAliasString + "_it < " + prevAliasString + "_fragment_size; " + prevAliasString + "_it++) {\n";
		}
		else {
			mainString += "\n" + tabString + "for (uint32_t " + prevAliasString + "_it = 0; " +
				prevAliasString + "_it < " + prevAliasString + "_fragment_size; " + prevAliasString + "_it++) {\n";
		}
		closingBraces[0]++;
		tabString.append("\t");

		mainString += "\n";
		
		// SemiJoin Check
		if (currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
			//int drivingAliasID = drivingAlias.getAliasID();
			//int drivingAppearance = joinAliasAppearanceIDs.get(drivingAlias);
			//int drivingPool = query.getBufferPoolID(drivingAliasID, drivingAliasCol);
			//int drivingGQFastIndexID = drivingAlias.getAssociatedIndex().getGQFastIndexID();
			String drivAliasString = drivingAlias.getAlias();
			if (hasThreading) {
				if (threadID) {
					mainString += "\n" + tabString + "if (!(" + drivAliasString 
							+ "_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer[thread_id]["+drivAliasString+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivAliasString + 
							"_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer[thread_id]["+drivAliasString+ "_it]] = true;\n";
				}
				else {
					mainString += "\n" + tabString + "if (!(" + drivAliasString 
							+ "_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer[0]["+drivAliasString+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivAliasString + 
							"_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer[0]["+drivAliasString+ "_it]] = true;\n";
				}
			}
			else {
				mainString += "\n" + tabString + "if (!(" + drivAliasString 
						+ "_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer["+drivAliasString+ "_it]])) {\n";
				closingBraces[0]++;
				tabString.append("\t");
				mainString += tabString + drivAliasString + 
						"_bool_array[" + drivAliasString +"_col" + drivingAliasCol +"_buffer["+drivAliasString+ "_it]] = true;\n";
			}
		}
		
		//int previousAliasAppearance = joinAliasAppearanceIDs.get(previousAlias);
		for (int previousColID : previousColumnIDs) {

			MetaIndex previousIndex = previousAlias.getAssociatedIndex();
			//int previousIndexID = previousIndex.getIndexID();
			//int previousGQFastIndexID = previousIndex.getGQFastIndexID();
			int colBytes = previousIndex.getColumnEncodedByteSizesList().get(previousColID);
			mainString += tabString; 
			if (!preThreading && hasThreading || !hasThreading) {
				mainString += getElementPrimitive(colBytes) + " ";
			}
			if (hasThreading) {
				if (threadID) {
					mainString += prevAliasString + "_col" + previousColID + "_element = " +
							 prevAliasString + "_col" + previousColID + "_buffer[thread_id][" 
							+ prevAliasString + "_it];\n";
				}
				else {
					mainString += prevAliasString + "_col" + previousColID + "_element = " +
							prevAliasString + "_col" + previousColID + "_buffer[0][" 
							+ prevAliasString + "_it];\n";
				}
			}
			else {
				mainString += prevAliasString + "_col" + previousColID + "_element = " +
						prevAliasString + "_col" + previousColID + "_buffer[" 
						+ prevAliasString + "_it];\n";
			}
			
		}

		
		
		String elementString = drivingAlias.getAlias() + "_col" + drivingAliasCol + "_element";
		String currentFragmentRow = "row_" + currentAlias.getAlias();
		String currAliasString = currentAlias.getAlias();
		int gqFastIndexID = currentAlias.getAssociatedIndex().getGQFastIndexID();
		
		mainString += "\n" + tabString + getIndexPrimitive(indexByteSize) + "* ";
		mainString += currentFragmentRow + " = idx[" + gqFastIndexID + "]->" +
				"index_map["+ elementString +"];\n"; 
		String currentFragmentBytesName = "";
		for (int k=0; k<columnIDs.size(); k++) {
			int currentCol = columnIDs.get(k);

			if (k == 0 && !entityFlag) {
				currentFragmentBytesName = currAliasString + "_col" + currentCol + "_bytes";
				mainString += tabString + "uint32_t " + currentFragmentBytesName + " = " +
						"idx[" + gqFastIndexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}

			mainString += joinGenerateDecodeFragmentFunction(preThreading, k, tabString, query, functionHeadersCppCode, functionsCppCode,
					currentCol, currentFragmentBytesName, entityFlag, currentAlias, currentOp);

		}
		

		return mainString;
	}



	
	/*
	 * Function evaluatePreviousSelection
	 * ----------------------------------
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */
	private static String  evaluatePreviousSelection(Operator previousOp, MetaQuery query, StringBuilder tabString, 
			int[] closingBraces, int drivingAliasCol, int indexByteSize, 
			List<Integer> columnIDs, boolean entityFlag, List<String> functionHeadersCppCode, List<String> functionsCppCode, boolean preThreading,
			boolean justStartedThreading, Alias drivingAlias, Alias currentAlias, Operator currentOp) {
		
		String mainString = new String();
		
		String currentFragmentRow = "row_" + currentAlias.getAlias();
		String currAliasString = currentAlias.getAlias();
		int gqFastIndexID = currentAlias.getAssociatedIndex().getGQFastIndexID();
		SelectionOperator previousSelectionOp = (SelectionOperator) previousOp;

		String previousAliasString = previousSelectionOp.getAlias().getAlias();

		if (justStartedThreading) {
			mainString += "\n" + tabString + "for (; " + previousAliasString + "_it<" + 
					previousSelectionOp.getSelectionsList().size() + "; " + previousAliasString + "_it++) {\n";
		}
		else {
			mainString += "\n" + tabString + "for (int "+ previousAliasString + "_it = 0; " + previousAliasString + "_it<" + 
				previousSelectionOp.getSelectionsList().size() + "; " + previousAliasString + "_it++) {\n";
		}
		closingBraces[0]++;
		tabString.append("\t");
		mainString += "\n" + tabString + "uint64_t " + previousAliasString + "_col0_element = " + previousAliasString + "_list[" +previousAliasString+"_it];\n";


		String elementString = drivingAlias.getAlias() + "_col" + drivingAliasCol + "_element";


		mainString += "\n" + tabString + getIndexPrimitive(indexByteSize) + "* ";
		mainString += currentFragmentRow + " = idx[" + gqFastIndexID + "]->" +
				"index_map["+ elementString +"];\n"; 
		String currentFragmentBytesName = "";
		for (int k=0; k<columnIDs.size(); k++) {
			int currentCol = columnIDs.get(k);

			if (k == 0 && !entityFlag) {
				currentFragmentBytesName = currAliasString + "_col" + currentCol + "_bytes";
				mainString += tabString + "uint32_t " + currentFragmentBytesName + " = " +
						"idx[" + gqFastIndexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}

			mainString += joinGenerateDecodeFragmentFunction(preThreading, k, tabString, query, functionHeadersCppCode, functionsCppCode,currentCol,
					currentFragmentBytesName, entityFlag, currentAlias, currentOp);

		}
		
		
		return mainString;
	}

	

	private static String evaluatePreviousIntersection(boolean justStartedThreading, MetaQuery query, StringBuilder tabString, int[] closingBraces, 
			Alias currentAlias, Alias drivingAlias, int drivingAliasCol, Operator currentOp, List<Integer> columnIDs, boolean entityFlag, 
			List<String> functionHeadersCppCode, List<String> functionsCppCode) {
		String mainString = new String();

		String currentFragmentRow = "row_" + currentAlias.getAlias();
		String currAliasString = currentAlias.getAlias();
		int gqFastIndexID = currentAlias.getAssociatedIndex().getGQFastIndexID();
		
		String intersectionSizeString = query.getQueryName() + "_intersection_size";
		String intersectionIterator = query.getQueryName() + "_intersection_it";
		
		if (justStartedThreading) {
			mainString += "\n" + tabString + "for (; " + intersectionIterator + "<" +
					intersectionSizeString + "; " + intersectionIterator + "++) {\n";
		}
		else {
			mainString += "\n" + tabString + "for (uint32_t "+ intersectionIterator +"= 0; " + intersectionIterator +"<" + 
					intersectionSizeString + "; " + intersectionIterator +"++) {\n";
		}
		closingBraces[0]++;
		tabString.append("\t");
		
		// SemiJoin Check
		if (currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
			//int drivingAliasID = drivingAlias.getAliasID();
			//int drivingPool = query.getBufferPoolID(drivingAliasID, drivingAliasCol);
			//int drivingGQFastIndexID = drivingAlias.getAssociatedIndex().getGQFastIndexID();
			String drivAliasString = drivingAlias.getAlias();
			//int drivingAppearance = joinAliasAppearanceIDs.get(drivingAlias);
			if (hasThreading) {
				if (justStartedThreading) {
					mainString += "\n" + tabString + "if (!(" + drivAliasString 
							+ "_bool_array[" + drivAliasString + "_col" + drivingAliasCol +"_buffer[thread_id]["+drivAliasString+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivAliasString + 
							"_bool_array[" + drivAliasString + "_col" + drivingAliasCol +"_buffer[thread_id]["+drivAliasString+ "_it]] = true;\n";
				}
				else {
					mainString += "\n" + tabString + "if (!(" + drivAliasString 
							+ "_bool_array[" + drivAliasString + "_col" + drivingAliasCol +"_buffer[0]["+drivAliasString+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivAliasString + 
							"_bool_array[" + drivAliasString + "_col" + drivingAliasCol +"_buffer[0]["+drivAliasString+ "_it]] = true;\n";
				}
			}
			else {
				mainString += "\n" + tabString + "if (!(" + drivAliasString 
						+ "_bool_array[" + drivAliasString + "_col" + drivingAliasCol +"_buffer["+drivAliasString+ "_it]])) {\n";
				closingBraces[0]++;
				tabString.append("\t");
				mainString += tabString + drivAliasString + 
						"_bool_array[" + drivAliasString + "_col" + drivingAliasCol +"_buffer["+drivAliasString+ "_it]] = true;\n";

			}
		
			
		}
		
		int indexByteSize = currentAlias.getAssociatedIndex().getIndexMapByteSize();
		String elementString = query.getQueryName() + "_intersection_buffer[" + query.getQueryName() + "_intersection_it]";
		
		mainString += "\n" + tabString + getIndexPrimitive(indexByteSize) + "* ";
		mainString += currentFragmentRow + " = idx[" + gqFastIndexID + "]->" +
				"index_map["+ elementString +"];\n"; 
		
		String currentFragmentBytesName = "";
		for (int k=0; k<columnIDs.size(); k++) {
			int currentCol = columnIDs.get(k);

			if (k == 0 && !entityFlag) {
				currentFragmentBytesName = currAliasString + "_col" + currentCol + "_bytes";
				mainString += tabString + "uint32_t " + currentFragmentBytesName + " = " +
						"idx[" + gqFastIndexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}

			mainString += joinGenerateDecodeFragmentFunction(!justStartedThreading, k, tabString, query, functionHeadersCppCode, functionsCppCode,
					currentCol, currentFragmentBytesName, entityFlag, currentAlias, currentOp);

		}
		
		
		return mainString;
	}
	
	
	
	/*
	 * Function evaluateJoin
	 * 
	 * Input: 
	 *			preThreading:		'true' if the evaluated join occurs before threading begins 	
	 * 			i:					The current operator iteration
	 * 			operators:			The list of operators to the code generator
	 * 			metadata:			The meta data for the code generator
	 * 			tabString:			A string that keeps track of the tabbing of the generated code (for readability)
	 * 			closingBraces:		Keeps track of the number of closing braces to place at the end of the code
	 * 			query:				Meta data on the query
	 * 			functionHeadersCppCode:	A list of strings that make up the headers of the functions that are generated (except the original function)
	 * 			functionCppCode: A list of strings, each of which is a function and function body
	 * Output: 
	 * 			Emits a string that is the next piece of the code for the main function. Also, associated String Lists functionHeadersCppCode and functionCppCode are updated.
	 * 
	 */
		
	private static String evaluateJoin(boolean preThreading, int i, List<Operator> operators, MetaData metadata, 
			StringBuilder tabString, int[] closingBraces, MetaQuery query, List<String> functionHeadersCppCode, List<String> functionsCppCode) {

		Operator currentOp = operators.get(i);
		
		Alias drivingAlias;
		Alias currentAlias;
		
		int drivingAliasCol;
	
		boolean entityFlag;
		boolean threadID = true;
		List<Integer> columnIDs;
		
		if (currentOp.getType() == Optypes.JOIN_OPERATOR) {
			JoinOperator currentJoinOp = (JoinOperator) currentOp;
			drivingAlias = currentJoinOp.getDrivingAlias();
			currentAlias = currentJoinOp.getAlias();
			drivingAliasCol = currentJoinOp.getDrivingAliasColumn();
			columnIDs = currentJoinOp.getColumnIDs();
			entityFlag = currentJoinOp.isEntityFlag();
		}
		// Assumed to be Semi-Join
		else {
			SemiJoinOperator currentSemiJoinOp = (SemiJoinOperator) currentOp;
			drivingAlias = currentSemiJoinOp.getDrivingAlias();
			currentAlias = currentSemiJoinOp.getAlias();	
			drivingAliasCol = currentSemiJoinOp.getDrivingAliasColumn();
			columnIDs = currentSemiJoinOp.getColumnIDs();
			entityFlag = currentSemiJoinOp.isEntityFlag();
		}
		
		if (preThreading) {
			query.setPreThreading(currentAlias.getAliasID(), true);
		}
		if (query.getPreThreading(drivingAlias.getAliasID())) {
			threadID = false;
		}
		int indexByteSize = currentAlias.getAssociatedIndex().getIndexMapByteSize();
		String mainString = new String();
		
		
		if (i == 0) {
			//TODO: Implementation when First Operator is a Join, meaning there is no selection
		}
		else {
			Operator previousOp = operators.get(i-1);
			boolean loopAgain = true;
			boolean justStartedThreading = false;
			while(loopAgain) {
				if (previousOp.getType() == Optypes.THREADING_OPERATOR) {
					// It is assumed that a THREADING_OPERATOR will never be the first operator
					// It is also assumed that an Entity Table join will never immediately follow a ThreadingOperator
					// 
					previousOp = operators.get(i-2);
					justStartedThreading = true;
				}
				else if (previousOp.getType() == Optypes.JOIN_OPERATOR || previousOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
					loopAgain = false;
					boolean previousEntityFlag;
					Alias previousAlias;
					
					List<Integer> previousColumnIDs;
					//int previousLoopColumn;

					if (previousOp.getType() == Optypes.JOIN_OPERATOR) {
						JoinOperator previousJoinOp = (JoinOperator) previousOp;
						previousEntityFlag = previousJoinOp.isEntityFlag();
						previousAlias = previousJoinOp.getAlias();
						previousColumnIDs = previousJoinOp.getColumnIDs();
						//previousLoopColumn = previousJoinOp.loopColumn;

					}
					else {
						SemiJoinOperator previousSemiJoinOp = (SemiJoinOperator) previousOp;
						previousEntityFlag = previousSemiJoinOp.isEntityFlag();
						previousAlias = previousSemiJoinOp.getAlias();
						previousColumnIDs = previousSemiJoinOp.getColumnIDs();
						//previousLoopColumn = previousSemiJoinOp.loopColumn;
					}
					
					if (previousEntityFlag) {
						// Previous operator was an entity table
						// There is no need for an additional loop
						
						mainString += evaluatePreviousJoinEntityTable(currentOp, query, threadID, tabString, closingBraces,
								drivingAliasCol, columnIDs, indexByteSize, preThreading, functionHeadersCppCode, 
								functionsCppCode, entityFlag, currentAlias, drivingAlias);


					}
					else{
						// Previous operator was relationship table
						mainString += evaluatePreviousJoinRelationshipTable(currentOp, query, threadID, tabString, closingBraces,
								drivingAliasCol, columnIDs, indexByteSize, preThreading, functionHeadersCppCode, 
								functionsCppCode, entityFlag, previousColumnIDs,  
								justStartedThreading, currentAlias, drivingAlias, previousAlias);

					}
				}
				else if (previousOp.getType() == Optypes.SELECTION_OPERATOR) {
					loopAgain = false;
					mainString += evaluatePreviousSelection(previousOp, query, tabString, closingBraces, drivingAliasCol, 
							indexByteSize, columnIDs, entityFlag, functionHeadersCppCode, functionsCppCode, preThreading,
							justStartedThreading, drivingAlias, currentAlias, currentOp);
					

				}
				else if (previousOp.getType() == Optypes.INTERSECTION_OPERATOR) {
					loopAgain = false;
					mainString += evaluatePreviousIntersection(justStartedThreading, query, tabString, closingBraces, currentAlias, 
							drivingAlias, drivingAliasCol, currentOp, columnIDs, entityFlag, functionHeadersCppCode, functionsCppCode);
				}
			}
			
		}
		return mainString;
	}
		



	private static String evaluateSelection(int i, Operator currentOp, StringBuilder tabString, MetaQuery query) {
	
		
		String mainSelectionString = new String();
		
		SelectionOperator selectionOp = (SelectionOperator) currentOp;
		String selectionAlias = selectionOp.getAlias().getAlias();
		
		int numSelections = selectionOp.getSelectionsList().size();
		
		
		mainSelectionString += "\n" + tabString + "uint64_t " + selectionAlias + "_list[" + numSelections + "];\n";
		
		for (int j=0; j<numSelections; j++) {
			int currSelection = selectionOp.getSelectionsList().get(j);
			mainSelectionString += tabString + selectionAlias + "_list["+j+"] = " + currSelection + ";\n";
		}
		
		
		return mainSelectionString;
	}


	private static String evaluateIntersection(Operator currentOp,
			StringBuilder tabString, MetaQuery query, List<String> functionHeadersCppCode, List<String> functionsCppCode) {
		
		String mainString = new String();
		
		IntersectionOperator interOp = (IntersectionOperator) currentOp;
		List<String> sizeNames = new ArrayList<String>();
		int[] poolNums = new int[interOp.getAliases().size()];
		//
		// Decode the fragments for the inputs to the intersection
		//
		
		for (int i=0; i<interOp.getAliases().size(); i++) {
			
			Alias currAlias = interOp.getAliases().get(i);
			String currAliasString = currAlias.getAlias() + "_" + i;
			MetaIndex currIndex = currAlias.getAssociatedIndex();
			int currGQFastIndexID = currIndex.getGQFastIndexID();
			int currCol = interOp.getColumnIDs().get(i);
			int currColEncoding = currIndex.getColumnEncodingsList().get(currCol);
			int currColEncodedByteSize = currIndex.getColumnEncodedByteSizesList().get(currCol);
			int currSelection = interOp.getSelections().get(i);
			int indexByteSize = currIndex.getIndexMapByteSize();
			String currFragmentRow = "row_" + currAliasString + "_intersection" + i; 
			mainString += "\n" + tabString + getIndexPrimitive(indexByteSize) + "* ";
			mainString += currFragmentRow + " = idx[" + currGQFastIndexID + "]->" +
							"index_map["+ currSelection +"];\n"; 
			
			
			String currentFragmentBytesName = currAliasString + "_col" + currCol + "_bytes_intersection" + i;
			mainString += tabString + "uint32_t " + currentFragmentBytesName + " = " +
						"idx[" + currGQFastIndexID + "]->index_map["+ currSelection +"+1]" +
						"[" + currCol + "] - " + currFragmentRow + "[" + currCol + "];\n";
			
			String pointerName = currAliasString + "_col" + currCol + "_intersection_ptr_" + i;
			String pointerString = new String();
			String functionParameters = "(";
			
			// UA pointer points to type of size of element
			if (currColEncoding == MetaData.ENCODING_UA) {
				switch (currColEncodedByteSize) {
				case MetaData.BYTES_1: 
					pointerString = "unsigned char* " + pointerName + 
					" = &(idx[" + currGQFastIndexID + "]->fragment_data[" + currCol + "][" + currFragmentRow + "[" + currCol + "]]);\n";
					functionParameters += "unsigned char* " + pointerName;
					break;
				case MetaData.BYTES_2: 
					pointerString = "uint16_t* " + pointerName + 
					" = reinterpret_cast<uint16_t *>(&(idx[" + currGQFastIndexID + "]->fragment_data[" + currCol + "][" + currFragmentRow + "[" + currCol + "]]));\n";
					functionParameters += "" + "uint16_t* " + pointerName;
					break;
				case MetaData.BYTES_4:
					pointerString = "uint32_t* " + pointerName + 
					" = reinterpret_cast<uint32_t *>(&(idx[" + currGQFastIndexID + "]->fragment_data[" + currCol + "][" + currFragmentRow + "[" + currCol + "]]));\n";
					functionParameters += "uint32_t* " + pointerName;
					break;
				case MetaData.BYTES_8:
					pointerString = "uint64_t* " + pointerName +
					" = reinterpret_cast<uint64_t *>(&(idx[" + currGQFastIndexID + "]->fragment_data[" + currCol + "][" + currFragmentRow + "[" + currCol + "]]));\n";
					functionParameters += "uint64_t* " + pointerName;
					break;
				}
			}
			else {
				pointerString = "unsigned char* " + pointerName + 
						" = &(idx[" + currGQFastIndexID + "]->fragment_data[" + currCol + "][" + currFragmentRow + "[" + currCol + "]]);\n";
				functionParameters += "unsigned char* " + pointerName;
			}
			
			mainString += tabString + pointerString;
			
			
			String functionName = query.getQueryName() + "_" +currAliasString + "_col" + currCol + "_intersection" + i;

			switch (currColEncoding) {

			case MetaData.ENCODING_UA:	{
				functionName += "_decode_UA";
				break;
			}
			case MetaData.ENCODING_BCA: {
				functionName += "_decode_BCA";
				break;
			}
			case MetaData.ENCODING_BB: {
				functionName += "_decode_BB";
				break;
			}
			case MetaData.ENCODING_HUFFMAN: {
				functionName += "_decode_Huffman";
				break;
			}

			}
			
			String sizeName = currAliasString + "_intersection" + i + "_fragment_size";
			sizeNames.add(sizeName);
			// First column's decoding determines the fragment size for all subsequent column decodings
			mainString += tabString + "uint32_t " + sizeName + " = 0;\n";
			functionParameters += ", uint32_t " + currentFragmentBytesName;
					functionParameters += ", uint32_t & " + sizeName + ")";
				
			String currFunction = "\nvoid " + functionName + functionParameters + " {\n";
			String currFunctionHeader = "\nextern inline void " + functionName + functionParameters + " __attribute__((always_inline));\n";
				
			mainString += tabString + functionName + "(" + pointerName;
			mainString += ", " + currentFragmentBytesName; 
			mainString += ", " + sizeName + ");\n";

			//int indexID = currIndex.getIndexID();
			//int aliasID = currAlias.getAliasID();
			//int currPool = ++bufferPoolTrackingArray[indexID][currCol];
			//query.setBufferPoolID(aliasID, currCol, currPool);
			//poolNums[i] = currPool;
			currFunction += generateDecodeFunctionBody(true, currGQFastIndexID, currAliasString, currColEncoding, 0, 
						currCol, sizeName, currentFragmentBytesName, pointerName, currColEncodedByteSize, currAlias, true);
			currFunction += "}\n";

			
			functionHeadersCppCode.add(currFunctionHeader);
			functionsCppCode.add(currFunction);
			
			

			
		}
		
		//
		// Implement the intersection
		//
		
		//int numInputs = interOp.getAliases().size();
		String queryName = query.getQueryName();
		String intersectionSizeName = queryName + "_intersection_size";
		
		String functionHeader = new String();
		String function = new String();
		
		// Intersection function call 
		functionHeader += "\nextern inline void " + queryName + "_intersection(";
		function += "void " + queryName + "_intersection(";
		
		mainString += "\n" + tabString + "uint32_t " + intersectionSizeName + " = 0;\n";  
		mainString += tabString + queryName + "_intersection(";
		for (String currSizeName : sizeNames) {
			mainString += currSizeName + ", ";
			functionHeader += "uint32_t " + currSizeName + ", ";
			function += "uint32_t " + currSizeName + ", ";
		}
		mainString += intersectionSizeName + ");\n";
		functionHeader += "uint32_t & " + intersectionSizeName + ") __attribute__((always_inline));\n"; 
		function += "uint32_t & " + intersectionSizeName + ") { \n";
		
		function += generateIntersectionFunctionBody(interOp, query, poolNums, sizeNames, intersectionSizeName);
		function += "}\n";
		
		functionHeadersCppCode.add(functionHeader);
		functionsCppCode.add(function);
		return mainString;
		
		
	}
	


	private static String evaluateLastOperator(boolean preThreading, List<Operator> operators,
			MetaData metadata, StringBuilder tabString,
			int[] closingBraces, MetaQuery query) {
		
		String mainString = new String();
		
		Operator lastOp = operators.get(operators.size()-1);
		Operator previousOp = operators.get(operators.size()-2);
	
		if (previousOp.getType() == Optypes.JOIN_OPERATOR) {
			JoinOperator previousJoinOp = (JoinOperator) previousOp;
			if (!previousJoinOp.isEntityFlag()) {
				
				String previousAlias = previousJoinOp.getAlias().getAlias();
				int previousAliasID = previousJoinOp.getAlias().getAliasID();
				boolean previousThreadID = query.getPreThreading(previousAliasID);
				
				
				mainString += "\n" + tabString + "for (uint32_t " + previousAlias + "_it = 0; " + previousAlias + "_it " +
						"< " + previousAlias + "_fragment_size; " + previousAlias + "_it++) {\n";
				closingBraces[0]++;
				tabString.append("\t");

				String previousAliasString = previousJoinOp.getAlias().getAlias();
				//int previousAppearance = joinAliasAppearanceIDs.get(previousJoinOp.getAlias());
				int previousGQIndexID = previousJoinOp.getAlias().getAssociatedIndex().getGQFastIndexID();
				//int previousGQFastIndexID = previousJoinOp.getAlias().getAssociatedIndex().getGQFastIndexID();
				for (int previousColID : previousJoinOp.getColumnIDs()) {
					MetaIndex previousIndex = metadata.getIndexMap().get(previousGQIndexID);
					int colBytes = previousIndex.getColumnEncodedByteSizesList().get(previousColID);
					mainString += tabString + getElementPrimitive(colBytes) + " ";
					if (hasThreading) {
						if (previousThreadID) {
							mainString += previousAlias + "_col" + previousColID + "_element = " +
									 previousAliasString + "_col" + previousColID + "_buffer[0][" + previousAlias + "_it];\n";
						}
						else {
							mainString += previousAlias + "_col" + previousColID + "_element = " +
									previousAliasString + "_col" + previousColID + "_buffer[thread_id][" + previousAlias + "_it];\n";
						}
					}
					else {
						mainString += previousAlias + "_col" + previousColID + "_element = " +
								previousAliasString + "_col" + previousColID + "_buffer[" + previousAlias + "_it];\n";
					}
				}
			}
		}
		else if (previousOp.getType() == Optypes.SEMIJOIN_OPERATOR){
			
			
			SemiJoinOperator previousSemiJoinOp = (SemiJoinOperator) previousOp;
			if (!previousSemiJoinOp.isEntityFlag()) {
				
				String previousAlias = previousSemiJoinOp.getAlias().getAlias();
				int previousAliasID = previousSemiJoinOp.getAlias().getAliasID();
				boolean previousThreadID = query.getPreThreading(previousAliasID);
				
				
				mainString += "\n" + tabString + "for (uint32_t " + previousAlias + "_it = 0; " + previousAlias + "_it " +
						"< " + previousAlias + "_fragment_size; " + previousAlias + "_it++) {\n";
				closingBraces[0]++;
				tabString.append("\t");

				int previousGQIndexID = previousSemiJoinOp.getAlias().getAssociatedIndex().getGQFastIndexID();
				//int previousGQFastIndexID = previousSemiJoinOp.getAlias().getAssociatedIndex().getGQFastIndexID();
				
				for (int previousColID : previousSemiJoinOp.getColumnIDs()) {
					MetaIndex previousIndex = metadata.getIndexMap().get(previousGQIndexID);
					int colBytes = previousIndex.getColumnEncodedByteSizesList().get(previousColID);
					mainString += tabString + getElementPrimitive(colBytes) + " ";
					if (hasThreading) {
						if (previousThreadID) {
							mainString += previousAlias + "_col" + previousColID + "_element = " +
									previousAlias + "_col" + previousColID + "_buffer[0][" + previousAlias + "_it];\n";
						}
						else {
							mainString += previousAlias + "_col" + previousColID + "_element = " +
									previousAlias + "_col" + previousColID + "_buffer[thread_id][" + previousAlias + "_it];\n";
						}
					}
					else {
						mainString += previousAlias + "_col" + previousColID + "_element = " +
								previousAlias + "_col" + previousColID + "_buffer[" + previousAlias + "_it];\n";
					}
				}
			}
			
		}
		
		Optypes opType = lastOp.getType();
		if (opType == Optypes.AGGREGATION_OPERATOR) {
			AggregationOperator aggregationOp = (AggregationOperator) lastOp;
			
			String drivingAlias = aggregationOp.getDrivingAlias().getAlias(); 
			int drivingAliasCol = aggregationOp.getDrivingAliasColumn();
			//int drivingAliasIndex = aggregationOp.getDrivingAlias().getAssociatedIndex().getGQFastIndexID();
			String elementString = drivingAlias + "_col" + drivingAliasCol + "_element";
			

			mainString += "\n" + tabString + "RC[" + elementString + "] = 1;\n";
			
			if (!preThreading) {
				mainString += "\n" + tabString + "pthread_spin_lock(&r_spin_locks["+ elementString +"]);\n";
			}

			if (aggregationOp.getAggregationFunction() == AggregationOperator.FUNCTION_COUNT)
			{
				mainString += tabString + "R[" + elementString +"] += 1;";
			}
			else if (aggregationOp.getAggregationFunction() == AggregationOperator.FUNCTION_SUM)
			{
				String alias = aggregationOp.getAggregationAlias().getAlias();
				int aliasCol = aggregationOp.getAggregationAliasColumn();
				String fullElementName = alias + "_col" + aliasCol + "_element";
				mainString += tabString + "R[" + elementString +"] += " + fullElementName + ";";
			}
			
				/*
			if (aggregationOp.getAggregationString() != null) {
				String delims = "[ ]+";
				String[] tokens = aggregationOp.getAggregationString().split(delims);
				String reconstructedString = new String();
				for (int j=0; j< tokens.length;j++) {
					String upTo2Characters = tokens[j].substring(0, Math.min(tokens[j].length(), 2));
					if (upTo2Characters.equals("op")) {
						// Reads the number immediately following "op"
						String num_letter = Character.toString(tokens[j].charAt(2));
						int aggregationAliasNum = Integer.parseInt(num_letter);
						String alias = aggregationOp.getAggregationVariablesAliases().get(aggregationAliasNum).getAlias();
						int aliasCol = aggregationOp.getAggregationVariablesColumns().get(aggregationAliasNum);
						String fullElementName = alias + "_col" + aliasCol + "_element";
						reconstructedString += fullElementName;
					}
					else {
						reconstructedString += tokens[j];
					}
				}

				mainString += tabString + "R[" + elementString +"] += " + reconstructedString + ";";

			}
			*/
			
			if (!preThreading) {
				mainString += "\n" + tabString + "pthread_spin_unlock(&r_spin_locks["+ elementString +"]);\n";
			}

			mainString += "\n";
		}
		else {
			String drivingAlias = "";
			int drivingAliasCol = -1;
			//int drivingAliasIndex = -1;
			
			if (opType == Optypes.JOIN_OPERATOR) {
				JoinOperator joinOp = (JoinOperator) lastOp;
				drivingAlias = joinOp.getDrivingAlias().getAlias();
				drivingAliasCol = joinOp.getDrivingAliasColumn();
				//drivingAliasIndex = joinOp.getDrivingAlias().getAssociatedIndex().getGQFastIndexID();
			}
			else if (opType == Optypes.SEMIJOIN_OPERATOR) {
				SemiJoinOperator semiJoinOp = (SemiJoinOperator) lastOp;
				drivingAlias = semiJoinOp.getDrivingAlias().getAlias();
				drivingAliasCol = semiJoinOp.getDrivingAliasColumn();
				//drivingAliasIndex = semiJoinOp.getDrivingAlias().getAssociatedIndex().getGQFastIndexID();
			}
			String elementString = drivingAlias + "_col" + drivingAliasCol + "_element";
			
			
			mainString += "\n" + tabString + "RC[" + elementString + "] = 1;\n";
			
			if (!preThreading) {
			//	mainString += "\n" + tabString + "pthread_spin_lock(&r_spin_locks["+ elementString +"]);\n";
			}
			
						
			mainString += tabString + "R[" + elementString +"] = 1;";
			
			if (!preThreading) {
			//	mainString += "\n" + tabString + "pthread_spin_unlock(&r_spin_locks["+ elementString +"]);\n";
			}
		}

			

		return mainString;
	}
	
	/*
	 * initThreading
	 * 
	 * Description: This function writes the implementation for 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */
	private static String initThreading(Operator currentOp, MetaData metadata,
			StringBuilder tabString, MetaQuery query,
			List<String> functionHeadersCppCode, List<String> functionsCppCode) {
		
		String mainString = new String();
		
		ThreadingOperator threadingOp = (ThreadingOperator) currentOp;
		
		String alias = threadingOp.getDrivingAlias().getAlias();
		
		if (threadingOp.isThreadingAfterIntersection()) {
			mainString += "\n" + tabString + "uint32_t thread_size = " + query.getQueryName() + "_intersection_size/NUM_THREADS;\n";
		}
		else {
			mainString += "\n" + tabString + "uint32_t thread_size = " + alias + "_fragment_size/NUM_THREADS;\n";
		}
		mainString += tabString + "uint32_t position = 0;\n";
		mainString += "\n" + tabString + "for (int i=0; i<NUM_THREADS; i++) {\n";
		tabString.append("\t");
		mainString += tabString + "arguments[i].start = position;\n";
		mainString += tabString + "position += thread_size;\n";
		mainString += tabString + "arguments[i].end = position;\n";
		mainString += tabString + "arguments[i].thread_id = i;\n";
		tabString.setLength(tabString.length() - 1);
		mainString += tabString + "}\n";
		if (threadingOp.isThreadingAfterIntersection()) {
			mainString += tabString + "arguments[NUM_THREADS-1].end = " + query.getQueryName() + "_intersection_size;\n";
		}
		else {
			mainString += tabString + "arguments[NUM_THREADS-1].end = " + alias + "_fragment_size;\n";
		}
		mainString += "\n" + tabString + "for (int i=0; i<NUM_THREADS; i++) {\n";
		tabString.append("\t");
		mainString += tabString + "pthread_create(&threads[i], NULL, &pthread_" + query.getQueryName() + 
				"_worker, (void *) &arguments[i]);\n";
		tabString.setLength(tabString.length() - 1);
		mainString += tabString + "}\n";
		mainString += "\n" + tabString + "for (int i=0; i<NUM_THREADS; i++) {\n";
		tabString.append("\t");
		mainString += tabString + "pthread_join(threads[i], NULL);\n";
		tabString.setLength(tabString.length() - 1);
		mainString += tabString + "}\n";
	
		
		String threadingFunction = new String();
		String threadFunctionHeader = "\nvoid* pthread_" + query.getQueryName() + "_worker(void* arguments);\n";
		threadingFunction += "\nvoid* pthread_" + query.getQueryName() + "_worker(void* arguments) {\n";
		threadingFunction += "\n\targs_threading* args = (args_threading *) arguments;\n";
		if (threadingOp.isThreadingAfterIntersection()) {
			threadingFunction += "\n\tuint32_t " + query.getQueryName() + "_intersection_it = args->start;\n";
			threadingFunction += "\tuint32_t " + query.getQueryName() + "_intersection_size = args->end;\n";
		}
		else {
			threadingFunction += "\n\tuint32_t " + alias + "_it = args->start;\n";
			threadingFunction += "\tuint32_t " + alias + "_fragment_size = args->end;\n";
		}
		threadingFunction += "\tint thread_id = args->thread_id;\n";
		
		functionsCppCode.add(threadingFunction);
		functionHeadersCppCode.add(threadFunctionHeader);
		return mainString;
	}
	

	/* Function:	evaluateOpearators
	*  Input:		
	* 				operators: 		a List of operators
	*				query: 			Of type MetaQuery, which contains information about 
	*								the current query being evaluated	
	*				metadata:		Metadata that is used to access index information
	*								in the form of MetaIndex's
	*				mainCppCode:	A list of Strings that forms the main body of cpp code
	*				functionHeadersCppCode:	A list of Strings that are header declarations of the functions
	*										of the cpp code
	*				functionsCppCode:		A list of Strings that contains the function bodies of the 
	*										cpp code, not including the main function
	*  Output:		None, but the mainCppCode, functionHeadersCppCode, functionsCppCode are updated with new 
	* 				Strings that represent the bulk of the code for the cpp file	
	* 
	*/
	private static void evaluateOperators(List<Operator> operators,
			MetaQuery query, MetaData metadata, List<String> mainCppCode, List<String> functionHeadersCppCode,
			List<String> functionsCppCode) {
		
		StringBuilder tabString = new StringBuilder("\t");
		int[] closingBraces = {1};
	
		// Buffer Pool Tracking:
		// Since buffers are indexed by gq-fast indices, we may run into a situation where the same index is used
		// multiple times in the same query. In this case, we provide a pool of buffers per index. Currently, since
		// the pool number is considered to be small, we use MAX(number of occurrences of an index) for every index buffer.
		// The Code Generator needs to keep track of which pool is being used for what alias. This is achieved by
		// storing the pool ID for each alias in the query meta-data.
		
		//int[][] bufferPoolTrackingArray = new int[metadata.getMaxIndexID()+1][];
		
		/*for (int i=0; i<metadata.getMaxIndexID()+1; i++) {	
			bufferPoolTrackingArray[i] = new int[metadata.getMaxColID()+1];
			Arrays.fill(bufferPoolTrackingArray[i], -1);
		}*/
		
		
		
		boolean preThreadingOp = true;
		int threadingFunctionID = -1;
		StringBuilder threadingTabString = new StringBuilder("\t");
		int[] threadingClosingBraces = {1};
		
		for (int i = 0; i<operators.size()-1; i++) {
			Operator currentOp = operators.get(i);
			Optypes opType = currentOp.getType();
			if (opType == Optypes.SELECTION_OPERATOR) {
				if (preThreadingOp) {
					mainCppCode.add(evaluateSelection(i, currentOp, tabString, query));
				}
				else {
					String temp = functionsCppCode.get(threadingFunctionID);
					temp += evaluateSelection(i, currentOp, threadingTabString, query);
					functionsCppCode.set(threadingFunctionID, temp);
				}
			}
			else if (opType == Optypes.JOIN_OPERATOR || opType == Optypes.SEMIJOIN_OPERATOR) {
				if (preThreadingOp) {
					mainCppCode.add(evaluateJoin(preThreadingOp, i, operators, 
							metadata, tabString, closingBraces, query, 
							functionHeadersCppCode, functionsCppCode));
				}
				else {
					String temp = functionsCppCode.get(threadingFunctionID);
					temp += evaluateJoin(preThreadingOp, i, operators,
							metadata, threadingTabString, threadingClosingBraces,query,
							functionHeadersCppCode, functionsCppCode);
					functionsCppCode.set(threadingFunctionID, temp);
				}
			}
			else if (opType == Optypes.INTERSECTION_OPERATOR) {
				mainCppCode.add(evaluateIntersection(currentOp, tabString, query, functionHeadersCppCode, functionsCppCode));
			}

			else if (opType == Optypes.THREADING_OPERATOR) {
				mainCppCode.add(initThreading(currentOp, metadata, tabString, query, 
						functionHeadersCppCode, functionsCppCode));
				preThreadingOp = false;
				threadingFunctionID = functionsCppCode.size()-1;
			}
			else {
				System.err.println("Error: Unknown Operator");
				System.exit(0);
			}	
		}
		
		//Operator lastOp = operators.get(operators.size()-1);
		if (preThreadingOp) {
				mainCppCode.add(evaluateLastOperator(preThreadingOp, operators, metadata, tabString, 
						closingBraces, query));
		}
		else {
			String temp = functionsCppCode.get(threadingFunctionID);
			temp += evaluateLastOperator(preThreadingOp, operators, metadata, threadingTabString, 
					threadingClosingBraces, query);
			functionsCppCode.set(threadingFunctionID, temp);
		}
		
		
		
		// Emplace closing braces
		String closingBracesString = new String();
		
		for (int i=0; i<closingBraces[0]-1; i++) {
			tabString.setLength(tabString.length() - 1);
			closingBracesString += tabString + "}\n";
		}
		closingBracesString += "\n";
		mainCppCode.add(closingBracesString);
		
		if (!preThreadingOp) {
		
			String closingThreadingString = new String();
			for (int i=0; i<threadingClosingBraces[0]-1; i++) {
				threadingTabString.setLength(threadingTabString.length() - 1);
				closingThreadingString += threadingTabString + "}\n";
			}
			closingThreadingString += "\treturn nullptr;\n";
			closingThreadingString += "}\n";
			String temp = functionsCppCode.get(threadingFunctionID);
			temp += closingThreadingString;
			functionsCppCode.set(threadingFunctionID, temp);
		}
		
	}



/*	private static void initQueryBufferPool(MetaQuery query,
			List<Operator> operators, MetaData metadata) {
		
		for (Operator currentOp : operators) {

			
			
			if (currentOp.getType() == Optypes.JOIN_OPERATOR || currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
				
				int aliasID;
				int indexID;
				
				if (currentOp.getType() == Optypes.JOIN_OPERATOR) {
					JoinOperator tempOp = (JoinOperator) currentOp;
					aliasID = tempOp.getAlias().getAliasID();
					indexID = tempOp.getAlias().getAssociatedIndex().getIndexID();
				}
				else {
					SemiJoinOperator tempOp = (SemiJoinOperator) currentOp;
					aliasID = tempOp.getAlias().getAliasID();
					indexID = tempOp.getAlias().getAssociatedIndex().getIndexID();
				}

				//boolean found = false;
				MetaIndex currIndex = metadata.getIndexList().get(indexID);
					
				int numColumns = currIndex.getNumColumns();
				

			}
		}
	}
	*/
	
	/*
	private static void initAppearanceMapping(List<Operator> operators) {
		
		HashMap<Alias, Integer> aliasMap = new HashMap<Alias, Integer>();
		
		for (int i=0; i<operators.size(); i++) {
			Operator currOp = operators.get(i);
			if (currOp.getType() == Optypes.INTERSECTION_OPERATOR) {
				IntersectionOperator currInterOp = (IntersectionOperator) currOp;
				List<Alias> aliases = currInterOp.getAliases();
				for (int j=0; j<aliases.size(); j++) {
					Alias currAlias = aliases.get(j);	
					if (aliasMap.get(currAlias) == null) {
						aliasMap.put(currAlias, 1);
					}
					else {
						int temp = aliasMap.get(currAlias) + 1;
						aliasMap.put(currAlias, temp);
					}
					intersectionAliasAppearanceIDs.add(aliasMap.get(currAlias)); 
				}
			}
			else if (currOp.getType() == Optypes.JOIN_OPERATOR || currOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
				Alias currAlias;
				if (currOp.getType() == Optypes.JOIN_OPERATOR) {
					JoinOperator currJoinOp = (JoinOperator) currOp;
					currAlias = currJoinOp.getAlias();
				}
				else {
					SemiJoinOperator currSemiJoinOp = (SemiJoinOperator) currOp;
					currAlias = currSemiJoinOp.getAlias();
				}
				if (aliasMap.get(currAlias) == null) {
					aliasMap.put(currAlias, 1);
				}
				else {
					int temp = aliasMap.get(currAlias) + 1;
					aliasMap.put(currAlias, temp);
				}
				
				
			}
		}
		
	}
*/
	private static void writeToFile(String fullCppCode, String queryName) {

		try(  PrintWriter out = new PrintWriter(new File("/home/ben/git/GQ-Fast-Final/Query/" + queryName + ".cpp"))){
		    out.println(fullCppCode);
		} catch (FileNotFoundException e) {
			System.err.println("Error in writeToFile: FileNotFoundException");
			e.printStackTrace();
		}
	
	}

	public static void generateCode(){
		String query_name = "SD2";
        TestTree_logical2RQNA test = new TestTree_logical2RQNA();
        test.TreeSD();
        test.print(test.getroot());
        System.out.println("\n---------------------------------------------------------------------------------------");
        RelationalAlgebra2RQNA ra = new RelationalAlgebra2RQNA(test.getroot()); 
        TreeNode RQNA = ra.RA2RQNA(true);
        RQNA2Physical rqna2physical = new RQNA2Physical();
        R2P_Output input_of_code_generator = rqna2physical.RQNA2Physical(null,RQNA,query_name);
        List<Operator> operators = input_of_code_generator.getOperators();
        MetaData metadata = input_of_code_generator.getMetaData();
        
		//intersectionAliasAppearanceIDs = new ArrayList<Integer>();
		joinBufferNames = new ArrayList<String>();
		intersectionBufferNames = new ArrayList<String>();
		
		hasIntersection = false;
		int resultDataType = 0;
		Operator lastOp = operators.get(operators.size() - 1);
		// Query has an aggregation
		if (lastOp.getType() == Optypes.AGGREGATION_OPERATOR) {
			AggregationOperator aggregation = (AggregationOperator) lastOp;
			resultDataType = aggregation.getDataType();
		}
		else {
			resultDataType = AggregationOperator.AGGREGATION_INT;
		}
		//int queryID = metadata.getCurrentQueryID();
		MetaQuery query = metadata.getQuery();
	
		checkThreading(query, operators);

		//initQueryBufferPool(query, operators, metadata);
		
		// Initialize Code Segments Strings
		String initCppCode = initialImportsAndConstants(query);
		List<String> globalsCppCode = new ArrayList<String>();
		List<String> functionHeadersCppCode = new ArrayList<String>();
		List<String> functionsCppCode = new ArrayList<String>();
		List<String> mainCppCode = new ArrayList<String>();

		/*** Implementation ***/

		initialDeclarations(globalsCppCode, resultDataType, query, operators);
		// Opening Line
		mainCppCode.add(openingLine(query, resultDataType));
	
		String benchmarkingString = "\n\tbenchmark_t1 = chrono::steady_clock::now();\n";
		
		mainCppCode.add(benchmarkingString);
		
		// Array initializations
		mainCppCode.add(bufferInitCode(operators, globalsCppCode));
	
		mainCppCode.add(initResultArray(resultDataType, metadata));
		mainCppCode.add(initSemiJoinArray(operators, metadata, globalsCppCode));
		
		// Initializations for BCA and Huffman Decodes
		initDecodeVars(operators, mainCppCode, globalsCppCode, metadata, query);
		
		// Operator evaluation
	
		evaluateOperators(operators, query, metadata, mainCppCode, functionHeadersCppCode, functionsCppCode);
		mainCppCode.add(bufferDeallocations(query));
		
		
		mainCppCode.add(semiJoinBufferDeallocation(operators, metadata));
		
		String lastString = "\n\t*null_checks = RC;\n";
		lastString += "\treturn R;\n";
		lastString += "\n}\n";
		lastString += "\n#endif\n";
		
		mainCppCode.add(lastString);
		
		
		
		/*** Final Concatenation ***/
	
		
		String fullCppCode = "";
		fullCppCode += initCppCode;
		for (String currentGlobalCpp : globalsCppCode) {
			fullCppCode += currentGlobalCpp;
		}
		for (String currentFunctionHeaderCpp : functionHeadersCppCode) {
			fullCppCode += currentFunctionHeaderCpp;
		}
		for (String currentFunctionCpp : functionsCppCode) {
			fullCppCode += currentFunctionCpp;
		}
		for (String currentMainCpp : mainCppCode) {
			fullCppCode += currentMainCpp;
		}
		
		
		writeToFile(fullCppCode, query.getQueryName());
	}

	
	public static void main(String[] args)
	{
		generateCode();
	}

	










	
}
