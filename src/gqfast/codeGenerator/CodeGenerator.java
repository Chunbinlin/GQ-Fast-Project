package gqfast.codeGenerator;

import gqfast.global.Alias;
import gqfast.global.MetaData;
import gqfast.global.MetaIndex;
import gqfast.global.MetaQuery;
import gqfast.global.Global.Optypes;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;


public class CodeGenerator {
	
	static boolean hasThreading;
	static int threadOpIndex;
	
	

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
		initCppCode += "\n#include \"../fastr_index.hpp\"\n";
		initCppCode += "#include \"../global_vars.hpp\"\n\n";
	
		initCppCode += "#define NUM_THREADS " + query.getNumThreads() + "\n";
		initCppCode += "#define BUFFER_POOL_SIZE " + query.getBufferPoolSize() + "\n";
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
	private static String openingLine(MetaQuery query, AggregationOperator aggregation) {

		String openingCppCode = "\nextern \"C\" ";
		if (aggregation.getDataType() == AggregationOperator.AGGREGATION_INT) {
			openingCppCode += "int* ";
		}
		else if (aggregation.getDataType() == AggregationOperator.AGGREGATION_DOUBLE) {
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
	private static String bufferInitCode(MetaQuery query) {
		
		Set<Integer> indexSet = query.getIndexIDs(); 	
		String bufferInitString = "\n\tint max_frag;\n";
		for (Integer curr : indexSet) {
			bufferInitString += "\n\tmax_frag = metadata.idx_max_fragment_sizes[" + curr + "];\n";
			bufferInitString += "\tfor(int i=0; i<metadata.idx_num_encodings[" + curr + "]; i++) {\n";
			bufferInitString += "\t\tfor (int j=0; j<NUM_THREADS; j++) {\n";
			bufferInitString += "\t\t\tbuffer_arrays[" + curr + "][i][j] = new int*[BUFFER_POOL_SIZE];\n";
			bufferInitString += "\t\t\tfor (int k=0; k<BUFFER_POOL_SIZE; k++) {\n";
			bufferInitString += "\t\t\t\tbuffer_arrays[" + curr + "][i][j][k] = new int[max_frag];\n";
			bufferInitString += "\t\t\t}\n";
			bufferInitString += "\t\t}\n";
			bufferInitString += "\t}\n";
		}
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
	private static String bufferDeallocation(MetaQuery query) {
		
		Set<Integer> indexSet = query.getIndexIDs();
		
		String bufferDeallocString = "\n";
		String tabString = "\t";
		
		for (Integer curr : indexSet) {
			bufferDeallocString += tabString + "for (int j=0; j<metadata.idx_num_encodings["+ curr +"]; j++) {\n";
			tabString += "\t";
			bufferDeallocString += tabString + "for (int k=0; k<NUM_THREADS; k++) {\n";
			tabString += "\t";            
			bufferDeallocString += tabString + "for (int l=0; l<BUFFER_POOL_SIZE; l++) {\n";
			tabString += "\t";
			bufferDeallocString += tabString +  "delete[] buffer_arrays["+curr+"][j][k][l];\n";
			tabString = tabString.substring(0, tabString.length()-1);
			bufferDeallocString += tabString + "}\n";
			bufferDeallocString += tabString + "delete[] buffer_arrays["+curr+"][j][k];\n";
			tabString = tabString.substring(0, tabString.length()-1);
			bufferDeallocString += tabString + "}\n";
			tabString = tabString.substring(0, tabString.length()-1);
			bufferDeallocString += tabString + "}\n";

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
		for (Operator currentOp : operators) {
			if (currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
				SemiJoinOperator semiOp = (SemiJoinOperator)currentOp;
				String alias = semiOp.getDrivingAlias().getAlias();
				semiDeallocString += "\tdelete[] " + alias + "_bool_array;\n";
				semiDeallocString += "\n";
			}
		
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
	private static String initResultArray(AggregationOperator aggregation) {
		
		String resultString = "\n\tRC = new int[metadata.idx_domains[" + aggregation.getGQFastIndexID() + "][0]]();\n";
		
		if (aggregation.getDataType() == AggregationOperator.AGGREGATION_INT) {
			resultString += "\tR = new int[metadata.idx_domains[" + aggregation.getGQFastIndexID() + "][0]]();\n";
		}
		else if (aggregation.getDataType() == AggregationOperator.AGGREGATION_DOUBLE) {
			resultString += "\tR = new double[metadata.idx_domains[" + aggregation.getGQFastIndexID() + "][0]]();\n";
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
		
		for (Operator currentOp: operators) {
			if (currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
				SemiJoinOperator currentSemiJoinOp = (SemiJoinOperator) currentOp;
				String alias = currentSemiJoinOp.getDrivingAlias().getAlias();
				int gqFastIndexID = currentSemiJoinOp.getDrivingAlias().getAssociatedIndex().getGQFastIndexID();
				
				String globalDeclarationString = "\nstatic bool* " + alias + "_bool_array;\n";
				globalsCppCode.add(globalDeclarationString);
				
				resultString += "\t" + alias + "_bool_array = new bool[metadata.idx_domains[" + gqFastIndexID + "][0]]();\n";
				
			}
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
			
			if (currentOperator.getType() == Optypes.JOIN_OPERATOR || currentOperator.getType() == Optypes.SEMIJOIN_OPERATOR) {
				
				int gqFastIndexID;
				MetaIndex tempIndex;
				String alias;
				List<Integer> columnIDs;
				if (currentOperator.getType() == Optypes.JOIN_OPERATOR) {
					JoinOperator tempJoinOp = (JoinOperator) currentOperator;
					Alias tempAlias = tempJoinOp.getAlias();
					tempIndex = tempAlias.getAssociatedIndex();
					gqFastIndexID = tempIndex.getGQFastIndexID();
					alias = tempAlias.getAlias();
					columnIDs = tempJoinOp.getColumnIDs();
				}
				else {
					SemiJoinOperator tempSemiJoinOp = (SemiJoinOperator) currentOperator;
					Alias tempAlias = tempSemiJoinOp.getAlias();
					tempIndex = tempAlias.getAssociatedIndex();
					gqFastIndexID = tempIndex.getGQFastIndexID();
					alias = tempAlias.getAlias();
					columnIDs = tempSemiJoinOp.getColumnIDs();
				}
				
				
				for (int j=0; j<columnIDs.size(); j++) {
					
					int columnID = columnIDs.get(j);
					
					int columnEncoding = tempIndex.getColumnEncodingsList().get(columnID);
				
					if (columnEncoding == MetaData.ENCODING_BCA) {
						String nextGlobal = "\nstatic uint32_t* " + alias + "_col" + j + "_bits_info;\n";
						globalsCppCode.add(nextGlobal);
						String nextMain = "\n\t"+alias+ "_col" + j + "_bits_info = idx[" + gqFastIndexID + "]->dict[" + columnID + "]->bits_info;\n";
						mainCppCode.add(nextMain);
					}
					else if (columnEncoding == MetaData.ENCODING_HUFFMAN) {
						String nextGlobal = "\nstatic int* "+ alias + "_col" + j + "_huffman_tree_array;\n";
						nextGlobal += "static bool* " + alias + "_col" + j + "_huffman_terminator_array;\n";
						globalsCppCode.add(nextGlobal);
						String nextMain = "\n\t" + alias + "_col" + j + "_huffman_tree_array = idx[" + gqFastIndexID + "]->huffman_tree_array[" + columnID + "];\n";
						nextMain += "\t"+ alias + "_col" + j + "_huffman_terminator_array = idx[" + gqFastIndexID + "]->huffman_terminator_array[" + columnID + "];\n";
						mainCppCode.add(nextMain);
	 				}
					
				}
				
			}
			else if (currentOperator.getType() == Optypes.INTERSECTION_OPERATOR) {
					// TODO: Intersection implementation
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
			AggregationOperator aggregation, MetaQuery query, List<Operator> operators) {
		
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
		
		
		if (aggregation.getDataType() == AggregationOperator.AGGREGATION_INT)  {		
			resultsGlobals += "\nstatic int* R;\n";
		}
		else if (aggregation.getDataType() == AggregationOperator.AGGREGATION_DOUBLE) {
			resultsGlobals += "\nstatic double* R;\n";	
		}
		resultsGlobals += "static int* RC;\n";
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
			function += tabString + "int32_t next_seven_bits = *" + pointerName + " & 127;\n";
			function += tabString + "next_seven_bits = next_seven_bits << shiftbits;\n";
			function += tabString + elementName +" |= next_seven_bits;\n";
			function += tabString + "shiftbits += 7;\n";
			tabString = tabString.substring(0, tabString.length()-1);
			function += tabString + "} while (!(*" + pointerName + "++ & 128));\n";
		}
		else if (currentEncoding == MetaData.ENCODING_HUFFMAN) {
			function += tabString + "int mask = 0x100;\n";
			function += tabString + "bool* terminator_array = &("+alias+"_col"+currentCol+"_huffman_terminator_array[0]);\n";
			function += tabString + "int* tree_array = &("+alias+"_col"+currentCol+"_huffman_tree_array[0]);\n";
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
	 * 			bufferPoolTrackingArray:	Keeps track of the buffer pool array to use
	 * 			currPool:					The current pool that is used to specify which buffer is used
	 * 			currByteSize:				Specifies how many bytes an uncompressed element uses
	 * Output:
	 * 			A string that represents the body of the function in cpp that is the decoding of a fragment
	 * 
	 */
	private static String generateDecodeFunctionBody(boolean preThreading, int gqFastIndexID, String alias,
			int currentEncoding, int columnIteration, int currentCol, String sizeName,
			String currentFragmentBytesName, String pointerName,
			int[][] bufferPoolTrackingArray, int currPool, int currentByteSize) {
		
		String function = "\n";
		String tabString = "\t";
		
		String bufferArraysPart = new String();
		if (preThreading) {
			bufferArraysPart = "buffer_arrays[" + gqFastIndexID + "][" + currentCol + "][0][" + currPool + "]";
		}
		else {
			bufferArraysPart = "buffer_arrays[" + gqFastIndexID + "][" + currentCol + "][thread_id][" + currPool + "]";
		}
		if (columnIteration == 0) {
			// Size is initially 0, is passed by reference, and will calculated in the function
			if (currentEncoding == MetaData.ENCODING_UA) {			
				function += tabString + sizeName + " = " + currentFragmentBytesName + "/" + currentByteSize + ";\n"; 
				function += "\n" + tabString + "for (int32_t i=0; i<" + sizeName + "; i++) {\n";
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
				function += tabString + "int32_t next_seven_bits = *" + pointerName + " & 127;\n";
				function += tabString + "next_seven_bits = next_seven_bits << shiftbits;\n";
				function += tabString + bufferArraysPart + "[0] |= next_seven_bits;\n";
				function += tabString + "shiftbits += 7;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "} while (!(*" + pointerName + "++ & 128));\n";
				function += tabString + sizeName + "++;\n";
				
				function += "\n" + tabString + "while (" + currentFragmentBytesName + " > 0) {\n";
				tabString += "\t";
				function += tabString + "shiftbits = 0;\n";
				function += tabString + "int32_t result = 0;\n";
				function += "\n" + tabString + "do {\n";
				tabString += "\t";
				function += "\n" + tabString + currentFragmentBytesName + "--;\n";
				function += tabString + "int32_t next_seven_bits = *" + pointerName + " & 127;\n";
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
				function += tabString + "int* tree_array_start = &("+ alias + "_col" + currentCol + "_huffman_tree_array[0]);\n";
				
				function += "\n" + tabString + "int mask = 0x100;\n";
				function += "\n" + tabString + "while ("+ currentFragmentBytesName + " > 1) {\n";
				tabString += "\t";
				function += "\n" + tabString + "bool* terminator_array = terminate_start;\n";
				function += tabString + "int* tree_array = tree_array_start;\n";
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
				function += tabString + "int* tree_array = tree_array_start;\n";
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
				
				function += tabString + sizeName + " = " + currentFragmentBytesName + "* 8 / " + bitsInfoPrefix + "[0];\n";
				function += tabString + "int bit_pos = 0;\n";
				function += tabString + "for (int32_t i=0; i<" + sizeName + "; i++) {\n";
				tabString += "\t";
				function += tabString + "uint32_t encoded_value = " + bitsInfoPrefix + "[1] << bit_pos;\n";
				function += tabString + "int64_t * next_8_ptr = reinterpret_cast<int64_t *>(" + pointerName + ");\n";
				function += tabString + "encoded_value &= *next_8_ptr;\n";
				function += tabString + "encoded_value >>= bit_pos;\n";
				function += "\n" + tabString + pointerName + " += (bit_pos + " + bitsInfoPrefix + "[0]) / 8;\n";
				function += tabString + "bit_pos = (bit_pos + " + bitsInfoPrefix + "[0]) % 8;\n";
				
				function += tabString + bufferArraysPart + "[i] = encoded_value;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				
			}
			
		}
		else {
			// Size is pre-calculated and will be used to control the iteration
			if (currentEncoding == MetaData.ENCODING_UA) {
				function += tabString + "for (int32_t i=0; i<" + sizeName + "; i++) {\n";
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
				function += tabString + "int32_t next_seven_bits = *" + pointerName + " & 127;\n";
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
				function += tabString + "int32_t result = 0;\n";
				function += "\n" + tabString + "do {\n";
				tabString += "\t";
				function += tabString + "int32_t next_seven_bits = *" + pointerName + " & 127;\n";
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
				function += tabString + "int* tree_array_start = &("+alias + "_col" + currentCol + "_huffman_tree_array[0]);\n";
				
				function += "\n" + tabString + "int mask = 0x100;\n";
				function += "\n" + tabString + "for (int32_t i=0; i<"+sizeName+"; i++) {\n";
				tabString += "\t";
				function += "\n" + tabString + "bool* terminator_array = terminate_start;\n";
				function += tabString + "int* tree_array = tree_array_start;\n";
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
				
				function += tabString + "int bit_pos = 0;\n";
				function += tabString + "for (int32_t i=0; i<" + sizeName + "; i++) {\n";
				tabString += "\t";
				function += tabString + "uint32_t encoded_value = " + bitsInfoPrefix + "[1] << bit_pos;\n";
				function += tabString + "int64_t * next_8_ptr = reinterpret_cast<int64_t *>(" + pointerName + ");\n";
				function += tabString + "encoded_value &= *next_8_ptr;\n";
				function += tabString + "encoded_value >>= bit_pos;\n";
				function += "\n" + tabString + pointerName + " += (bit_pos + " + bitsInfoPrefix + "[0]) / 8;\n";
				function += tabString + "bit_pos = (bit_pos + " + bitsInfoPrefix + "[0]) % 8;\n";
				
				function += tabString + bufferArraysPart + "[i] = encoded_value;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				
			}
			
		}
		
		
		
		return function;
		
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
	 * 			bufferPoolTrackingArray:	To keep track of which array in the pool to use
	 * 			entityFlag:					'true' if current index is an entity table, 'false' if it is a relationship table
	 *			currentAlias:				The Alias object that represents the alias on which the function is being generated 
	 * 
	 *	Output:
	 *			A String that continues the cpp code for the original function and deals with the decoding portion of 
	 *			the join evaluation for a particular column.
	 */
	private static String joinGenerateDecodeFragmentFunction(boolean preThreading, int k, StringBuilder tabString, MetaQuery query, 
			List<String> functionHeadersCppCode, List<String> functionsCppCode, int currentCol,
			String currentFragmentBytesName, int[][] bufferPoolTrackingArray, boolean entityFlag, Alias currentAlias) {
		
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
				pointerString = "int16_t* " + pointerName + 
				" = reinterpret_cast<int16_t *>(&(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
				functionParameters += "" + "int16_t* " + pointerName;
				break;
			case MetaData.BYTES_4:
				pointerString = "int32_t* " + pointerName + 
				" = reinterpret_cast<int32_t *>(&(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
				functionParameters += "int32_t* " + pointerName;
				break;
			case MetaData.BYTES_8:
				pointerString = "int64_t* " + pointerName +
				" = reinterpret_cast<int64_t *>(&(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
				functionParameters += "int64_t* " + pointerName;
				break;
			}
		}
		else if (entityFlag && currentEncoding == MetaData.ENCODING_BCA){
			pointerString = "int64_t* " + pointerName +
					" = reinterpret_cast<int64_t *>(&(idx[" + gqFastIndexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
			functionParameters += "int64_t* " + pointerName;
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
				mainString += tabString + "int32_t " + sizeName + " = 0;\n";
				functionParameters += ", int32_t " + currentFragmentBytesName;
				functionParameters += ", int32_t & " + sizeName + ")";
			}
			else {
				functionParameters += ", int32_t " + sizeName + ")";
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

			int indexID = currMetaIndex.getIndexID();
			int aliasID = currentAlias.getAliasID();
			int currPool = ++bufferPoolTrackingArray[indexID][currentCol];
			query.setBufferPoolID(aliasID, currentCol, currPool);
			
			currFunction += generateDecodeFunctionBody(preThreading, gqFastIndexID, alias, currentEncoding, k, 
					currentCol, sizeName, currentFragmentBytesName, pointerName, bufferPoolTrackingArray, currPool, currentBytesSize);
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
			List<String> functionsCppCode, int[][] bufferPoolTrackingArray, boolean entityFlag, Alias currentAlias, Alias drivingAlias) {
		String mainString = new String();
		
		String drivAliasString = drivingAlias.getAlias();
		
		// SemiJoin Check
		if (currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
			int drivingAliasID = drivingAlias.getAliasID();
			int drivingPool = query.getBufferPoolID(drivingAliasID, drivingAliasCol);
			int drivingGQFastIndexID = drivingAlias.getAssociatedIndex().getGQFastIndexID();
			
			if (threadID) {
				mainString += "\n" + tabString + "if (!(" + drivAliasString 
						+ "_bool_array[buffer_arrays[" + drivingGQFastIndexID +"][" + drivingAliasCol +"][thread_id]["+drivingPool+"]["+drivAliasString+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivAliasString + 
						"_bool_array[buffer_arrays[" + drivingGQFastIndexID +"][" + drivingAliasCol +"][thread_id]["+drivingPool+"]["+drivAliasString+ "_it]] = true;\n";
			}
			else {
				mainString += "\n" + tabString + "if (!(" + drivAliasString 
					+ "_bool_array[buffer_arrays[" + drivingGQFastIndexID +"][" + drivingAliasCol +"][0]["+drivingPool+"]["+drivAliasString+ "_it]])) {\n";
				closingBraces[0]++;
				tabString.append("\t");
				mainString += tabString + drivAliasString + 
					"_bool_array[buffer_arrays[" + drivingGQFastIndexID +"][" + drivingAliasCol +"][0]["+drivingPool+"]["+drivAliasString+ "_it]] = true;\n";
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
				mainString += tabString + "int32_t " + currentFragmentBytesName + " = " +
						"idx[" + gqFastIndexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}
		
			mainString += joinGenerateDecodeFragmentFunction(preThreading, k, tabString, query, functionHeadersCppCode, functionsCppCode, 
					currentCol, currentFragmentBytesName, bufferPoolTrackingArray, entityFlag, currentAlias);
			
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
			List<String> functionsCppCode, int[][] bufferPoolTrackingArray, boolean entityFlag, List<Integer> previousColumnIDs,
			boolean justStartedThreading, Alias currentAlias, Alias drivingAlias, Alias previousAlias) {
		String mainString = new String();
		
		String prevAlias = previousAlias.getAlias();
		
		if (justStartedThreading) {
			mainString += "\n" + tabString + "for (; " +
					prevAlias + "_it < " + prevAlias + "_fragment_size; " + prevAlias + "_it++) {\n";
		}
		else {
			mainString += "\n" + tabString + "for (int32_t " + prevAlias + "_it = 0; " +
				prevAlias + "_it < " + prevAlias + "_fragment_size; " + prevAlias + "_it++) {\n";
		}
		closingBraces[0]++;
		tabString.append("\t");

		mainString += "\n";
		
		// SemiJoin Check
		if (currentOp.getType() == Optypes.SEMIJOIN_OPERATOR) {
			int drivingAliasID = drivingAlias.getAliasID();
			int drivingPool = query.getBufferPoolID(drivingAliasID, drivingAliasCol);
			int drivingGQFastIndexID = drivingAlias.getAssociatedIndex().getGQFastIndexID();
			String drivAliasString = drivingAlias.getAlias();
			if (threadID) {
				mainString += "\n" + tabString + "if (!(" + drivAliasString 
						+ "_bool_array[buffer_arrays[" + drivingGQFastIndexID +"][" + drivingAliasCol +"][thread_id]["+drivingPool+"]["+drivAliasString+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivAliasString + 
						"_bool_array[buffer_arrays[" + drivingGQFastIndexID +"][" + drivingAliasCol +"][thread_id]["+drivingPool+"]["+drivAliasString+ "_it]] = true;\n";
			}
			else {
				mainString += "\n" + tabString + "if (!(" + drivAliasString 
					+ "_bool_array[buffer_arrays[" + drivingGQFastIndexID +"][" + drivingAliasCol +"][0]["+drivingPool+"]["+drivAliasString+ "_it]])) {\n";
				closingBraces[0]++;
				tabString.append("\t");
				mainString += tabString + drivAliasString + 
					"_bool_array[buffer_arrays[" + drivingGQFastIndexID +"][" + drivingAliasCol +"][0]["+drivingPool+"]["+drivAliasString+ "_it]] = true;\n";
			}
		}
		
		for (int previousColID : previousColumnIDs) {

			MetaIndex previousIndex = previousAlias.getAssociatedIndex();
			int previousIndexID = previousIndex.getIndexID();
			int previousGQFastIndexID = previousIndex.getGQFastIndexID();
			int colBytes = previousIndex.getColumnEncodedByteSizesList().get(previousColID);
			mainString += tabString; 
			if (!preThreading && hasThreading || !hasThreading) {
				mainString += getElementPrimitive(colBytes) + " ";
			}

			if (threadID) {
				mainString += prevAlias + "_col" + previousColID + "_element = " +
					"buffer_arrays[" + previousGQFastIndexID + "][" + previousColID + "][thread_id]" +
					"["+bufferPoolTrackingArray[previousIndexID][previousColID]+ "][" + prevAlias + "_it];\n";
			}
			else {
				mainString += prevAlias + "_col" + previousColID + "_element = " +
						"buffer_arrays[" + previousGQFastIndexID + "][" + previousColID + "][0]" +
						"["+bufferPoolTrackingArray[previousIndexID][previousColID]+ "][" + prevAlias + "_it];\n";
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
				mainString += tabString + "int32_t " + currentFragmentBytesName + " = " +
						"idx[" + gqFastIndexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}

			mainString += joinGenerateDecodeFragmentFunction(preThreading, k, tabString, query, functionHeadersCppCode, functionsCppCode,
					currentCol, currentFragmentBytesName, bufferPoolTrackingArray, entityFlag, currentAlias);

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
			int[][] bufferPoolTrackingArray, boolean justStartedThreading, Alias drivingAlias, Alias currentAlias) {
		
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
		mainString += "\n" + tabString + "int64_t " + previousAliasString + "_col0_element = " + previousAliasString + "_list[" +previousAliasString+"_it];\n";


		String elementString = drivingAlias.getAlias() + "_col" + drivingAliasCol + "_element";


		mainString += "\n" + tabString + getIndexPrimitive(indexByteSize) + "* ";
		mainString += currentFragmentRow + " = idx[" + gqFastIndexID + "]->" +
				"index_map["+ elementString +"];\n"; 
		String currentFragmentBytesName = "";
		for (int k=0; k<columnIDs.size(); k++) {
			int currentCol = columnIDs.get(k);

			if (k == 0 && !entityFlag) {
				currentFragmentBytesName = currAliasString + "_col" + currentCol + "_bytes";
				mainString += tabString + "int32_t " + currentFragmentBytesName + " = " +
						"idx[" + gqFastIndexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}

			mainString += joinGenerateDecodeFragmentFunction(preThreading, k, tabString, query, functionHeadersCppCode, functionsCppCode,currentCol,
					currentFragmentBytesName, bufferPoolTrackingArray, entityFlag, currentAlias);

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
	 * 			bufferPoolTrackingArray:	This tracks which buffer pool to use for what alias in the loader's buffer arrays
	 * 			query:				Meta data on the query
	 * 			functionHeadersCppCode:	A list of strings that make up the headers of the functions that are generated (except the original function)
	 * 			functionCppCode: A list of strings, each of which is a function and function body
	 * Output: 
	 * 			Emits a string that is the next piece of the code for the main function. Also, associated String Lists functionHeadersCppCode and functionCppCode are updated.
	 * 
	 */
		
	private static String evaluateJoin(boolean preThreading, int i, List<Operator> operators, MetaData metadata, 
			StringBuilder tabString, int[] closingBraces, int[][] bufferPoolTrackingArray, MetaQuery query, List<String> functionHeadersCppCode, List<String> functionsCppCode) {

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
								functionsCppCode, bufferPoolTrackingArray, entityFlag, currentAlias, drivingAlias);


					}
					else{
						// Previous operator was relationship table
						mainString += evaluatePreviousJoinRelationshipTable(currentOp, query, threadID, tabString, closingBraces,
								drivingAliasCol, columnIDs, indexByteSize, preThreading, functionHeadersCppCode, 
								functionsCppCode, bufferPoolTrackingArray, entityFlag, previousColumnIDs,  
								justStartedThreading, currentAlias, drivingAlias, previousAlias);

					}
				}
				else if (previousOp.getType() == Optypes.SELECTION_OPERATOR) {
					loopAgain = false;
					mainString += evaluatePreviousSelection(previousOp, query, tabString, closingBraces, drivingAliasCol, 
							indexByteSize, columnIDs, entityFlag, functionHeadersCppCode, functionsCppCode, preThreading,
							bufferPoolTrackingArray, justStartedThreading, drivingAlias, currentAlias);
					

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
		
		
		mainSelectionString += "\n" + tabString + "int64_t " + selectionAlias + "_list[" + numSelections + "];\n";
		
		for (int j=0; j<numSelections; j++) {
			int currSelection = selectionOp.getSelectionsList().get(j);
			mainSelectionString += tabString + selectionAlias + "_list["+j+"] = " + currSelection + ";\n";
		}
		
		
		return mainSelectionString;
	}

	private static String evaluateAggregation(boolean preThreading, int i, List<Operator> operators,
			MetaData metadata, StringBuilder tabString,
			int[] closingBraces, int[][] bufferPoolTrackingArray, MetaQuery query) {
		
		Operator currentOp = operators.get(i);
		
		String mainString = new String();
		
		AggregationOperator aggregationOp = (AggregationOperator) currentOp;
		
		Operator previousOp = operators.get(i-1);
	
		if (previousOp.getType() == Optypes.JOIN_OPERATOR) {
			JoinOperator previousJoinOp = (JoinOperator) previousOp;
			if (!previousJoinOp.isEntityFlag()) {
				
				String previousAlias = previousJoinOp.getAlias().getAlias();
				int previousAliasID = previousJoinOp.getAlias().getAliasID();
				boolean previousThreadID = query.getPreThreading(previousAliasID);
				
				
				mainString += "\n" + tabString + "for (int32_t " + previousAlias + "_it = 0; " + previousAlias + "_it " +
						"< " + previousAlias + "_fragment_size; " + previousAlias + "_it++) {\n";
				closingBraces[0]++;
				tabString.append("\t");

				int previousIndexID = previousJoinOp.getAlias().getAssociatedIndex().getIndexID();
				int previousGQFastIndexID = previousJoinOp.getAlias().getAssociatedIndex().getGQFastIndexID();
				for (int previousColID : previousJoinOp.getColumnIDs()) {
					MetaIndex previousIndex = metadata.getIndexList().get(previousIndexID);
					int colBytes = previousIndex.getColumnEncodedByteSizesList().get(previousColID);
					mainString += tabString + getElementPrimitive(colBytes) + " ";
					
					if (previousThreadID) {
						mainString += previousAlias + "_col" + previousColID + "_element = " +
							"buffer_arrays[" + previousGQFastIndexID + "][" + previousColID + "][0]" +
							"["+bufferPoolTrackingArray[previousIndexID][previousColID]+ "][" + previousAlias + "_it];\n";
					}
					else {
						mainString += previousAlias + "_col" + previousColID + "_element = " +
								"buffer_arrays[" + previousGQFastIndexID + "][" + previousColID + "][thread_id]" +
								"["+bufferPoolTrackingArray[previousIndexID][previousColID]+ "][" + previousAlias + "_it];\n";
					}
				}
			}
		}
		else if (previousOp.getType() == Optypes.SELECTION_OPERATOR){
			
			
		}
				
		String drivingAlias = aggregationOp.getDrivingAlias().getAlias(); 
		int drivingAliasCol = aggregationOp.getDrivingAliasColumn();
		int drivingAliasIndex = aggregationOp.getDrivingAlias().getAssociatedIndex().getGQFastIndexID();
		String elementString = drivingAlias + "_col" + drivingAliasCol + "_element";
		

		mainString += "\n" + tabString + "RC[" + elementString + "] = 1;\n";
		
		if (!preThreading && aggregationOp.getAggregationString() != null) {
			mainString += "\n" + tabString + "pthread_spin_lock(&spin_locks["+ drivingAliasIndex + "]["+ elementString +"]);\n";
		}

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
		else {
			mainString += tabString + "R[" + elementString +"] = 1;";
		}
		
		
		if (!preThreading && aggregationOp.getAggregationString() != null) {
			mainString += "\n" + tabString + "pthread_spin_unlock(&spin_locks["+ drivingAliasIndex + "]["+ elementString +"]);\n";
		}

		mainString += "\n";

			

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
		
		mainString += "\n" + tabString + "int32_t thread_size = " + alias + "_fragment_size/NUM_THREADS;\n"; 
		mainString += tabString + "int32_t position = 0;\n";
		mainString += "\n" + tabString + "for (int i=0; i<NUM_THREADS; i++) {\n";
		tabString.append("\t");
		mainString += tabString + "arguments[i].start = position;\n";
		mainString += tabString + "position += thread_size;\n";
		mainString += tabString + "arguments[i].end = position;\n";
		mainString += tabString + "arguments[i].thread_id = i;\n";
		tabString.setLength(tabString.length() - 1);
		mainString += tabString + "}\n";
		mainString += tabString + "arguments[NUM_THREADS-1].end = " + alias + "_fragment_size;\n";
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
		threadingFunction += "\n\tint32_t " + alias + "_it = args->start;\n";
		threadingFunction += "\tint32_t " + alias + "_fragment_size = args->end;\n";
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
		int[][] bufferPoolTrackingArray = new int[metadata.getMaxIndexID()+1][];
		
		for (int i=0; i<metadata.getMaxIndexID()+1; i++) {	
			bufferPoolTrackingArray[i] = new int[metadata.getMaxColID()+1];
			Arrays.fill(bufferPoolTrackingArray[i], -1);
		}
		
		
		
		boolean preThreadingOp = true;
		int threadingFunctionID = -1;
		StringBuilder threadingTabString = new StringBuilder("\t");
		int[] threadingClosingBraces = {1};
		
		for (int i = 0; i<operators.size(); i++) {
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
							metadata, tabString, closingBraces, bufferPoolTrackingArray, query, 
							functionHeadersCppCode, functionsCppCode));
				}
				else {
					String temp = functionsCppCode.get(threadingFunctionID);
					temp += evaluateJoin(preThreadingOp, i, operators,
							metadata, threadingTabString, threadingClosingBraces, bufferPoolTrackingArray, query,
							functionHeadersCppCode, functionsCppCode);
					functionsCppCode.set(threadingFunctionID, temp);
				}
			}
			else if (opType == Optypes.INTERSECTION_OPERATOR) {
				
			}
			else if (opType == Optypes.AGGREGATION_OPERATOR) {
				if (preThreadingOp) {
					mainCppCode.add(evaluateAggregation(preThreadingOp, i, operators, metadata, tabString, 
							closingBraces, bufferPoolTrackingArray, query));
				}
				else {
					String temp = functionsCppCode.get(threadingFunctionID);
					temp += evaluateAggregation(preThreadingOp, i, operators, metadata, threadingTabString, 
							threadingClosingBraces, bufferPoolTrackingArray, query);
					functionsCppCode.set(threadingFunctionID, temp);
				}
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


	private static void initQueryBufferPool(MetaQuery query,
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
				query.initBufferPoolArray(aliasID, numColumns);

			}
		}
	}
	





	private static void writeToFile(String fullCppCode, String queryName) {

		try(  PrintWriter out = new PrintWriter(new File("./src/gqfast/loader/test_cases/" + queryName + ".cpp"))){
		    out.println(fullCppCode);
		} catch (FileNotFoundException e) {
			System.err.println("Error in writeToFile: FileNotFoundException");
			e.printStackTrace();
		}
	
	}

	public static void generateCode(List<Operator> operators, MetaData metadata) {
		
	
		
		// The last operator should be the aggregate operator
		AggregationOperator aggregation = (AggregationOperator) operators.get(operators.size() - 1); 
		
		int queryID = metadata.getCurrentQueryID();
		MetaQuery query = metadata.getQueryList().get(queryID);
	
		checkThreading(query, operators);

		initQueryBufferPool(query, operators, metadata);
		
		// Initialize Code Segments Strings
		String initCppCode = initialImportsAndConstants(query);
		List<String> globalsCppCode = new ArrayList<String>();
		List<String> functionHeadersCppCode = new ArrayList<String>();
		List<String> functionsCppCode = new ArrayList<String>();
		List<String> mainCppCode = new ArrayList<String>();

		/*** Implementation ***/

		initialDeclarations(globalsCppCode, aggregation, query, operators);
		// Opening Line
		mainCppCode.add(openingLine(query, aggregation));
	
		String benchmarkingString = "\n\tbenchmark_t1 = chrono::steady_clock::now();\n";
		
		mainCppCode.add(benchmarkingString);
		
		// Array initializations
		mainCppCode.add(bufferInitCode(query));
	
		mainCppCode.add(initResultArray(aggregation));
		mainCppCode.add(initSemiJoinArray(operators, metadata, globalsCppCode));
		
		// Initializations for BCA and Huffman Decodes
		initDecodeVars(operators, mainCppCode, globalsCppCode, metadata, query);
		
		// Operator evaluation
	
		evaluateOperators(operators, query, metadata, mainCppCode, functionHeadersCppCode, functionsCppCode);
		mainCppCode.add(bufferDeallocation(query));
		
		
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
	











	
}
