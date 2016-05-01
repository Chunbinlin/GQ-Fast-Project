package codegenerator;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class CodeGenerator {
	
	
	private static String initialImportsAndConstants(MetaQuery query) {
		
	
		String initCppCode = "";
		// Initial Code
		initCppCode += "#ifndef " + query.queryName + "_\n"; 
		initCppCode += "#define " + query.queryName + "_\n";
		initCppCode += "\n#include \"fastr_index.hpp\"\n";
		initCppCode += "#include \"global_vars.hpp\"\n\n";
	
		initCppCode += "#define NUM_THREADS " + query.numThreads + "\n";
		initCppCode += "#define NUM_BUFFERS " + query.numBuffers + "\n";
		initCppCode += "#define BUFFER_POOL_SIZE " + query.bufferPoolSize + "\n";
		initCppCode += "\nusing namespace std;\n";
				
		return initCppCode;
	}


	private static String openingLine(MetaQuery query, AggregationOperator aggregation) {

		String openingCppCode = "\nextern \"C\" ";
		if (aggregation.dataType == AggregationOperator.AGGREGATION_INT) {
			openingCppCode += "int* ";
		}
		else if (aggregation.dataType == AggregationOperator.AGGREGATION_DOUBLE) {
			openingCppCode += "double* ";
		}
		openingCppCode += query.queryName + "(int** null_checks) {\n";
		
		return openingCppCode;
	}

	private static String bufferInitCode(Metadata metadata) {
		
		String bufferInitString = "\n\tint max_frag;\n";
		Iterator<MetaIndex> indexIterator = metadata.indexList.iterator();
		int i = 0;
		while (indexIterator.hasNext()) {
			int curr = indexIterator.next().indexID;
			bufferInitString += "\n\tmax_frag = metadata.idx_max_fragment_sizes[" + curr + "];\n";
			bufferInitString += "\tfor(int i=0; i<metadata.idx_num_encodings[" + curr + "]; i++) {\n";
			bufferInitString += "\t\tfor (int j=0; j<NUM_THREADS; j++) {\n";
			bufferInitString += "\t\t\tbuffer_arrays[" + curr + "][i][j] = new int*[BUFFER_POOL_SIZE];\n";
			bufferInitString += "\t\t\tfor (int k=0; k<BUFFER_POOL_SIZE; k++) {\n";
			bufferInitString += "\t\t\t\tbuffer_arrays[" + curr + "][i][j][k] = new int[max_frag];\n";
			bufferInitString += "\t\t\t}\n";
			bufferInitString += "\t\t}\n";
			bufferInitString += "\t}\n";
			i++;
		}
		return bufferInitString;
		
	}
	
	private static String bufferDeallocation(Metadata metadata) {
		
		String bufferDeallocString = "\n";
		String tabString = "\t";
		
	    bufferDeallocString += tabString + "for (int i=0; i<NUM_BUFFERS; i++) {\n";
	    tabString += "\t";
	    bufferDeallocString += tabString + "for (int j=0; j<metadata.idx_num_encodings[i]; j++) {\n";
	    tabString += "\t";
	    bufferDeallocString += tabString + "for (int k=0; k<NUM_THREADS; k++) {\n";
	    tabString += "\t";            
	    bufferDeallocString += tabString + "for (int l=0; l<BUFFER_POOL_SIZE; l++) {\n";
	    tabString += "\t";
	    bufferDeallocString += tabString +  "delete[] buffer_arrays[i][j][k][l];\n";
	    tabString = tabString.substring(0, tabString.length()-1);
	    bufferDeallocString += tabString + "}\n";
	    bufferDeallocString += tabString + "delete[] buffer_arrays[i][j][k];\n";
	    tabString = tabString.substring(0, tabString.length()-1);
	    bufferDeallocString += tabString + "}\n";
	    tabString = tabString.substring(0, tabString.length()-1);
	    bufferDeallocString += tabString + "}\n";
	    tabString = tabString.substring(0, tabString.length()-1);
	    bufferDeallocString += tabString + "}\n";
		return bufferDeallocString;
		
		
	}
	
	private static String semiJoinBufferDeallocation(List<Operator> operators,
			Metadata metadata) {
		
		String semiDeallocString = "\n";
		for (Operator currentOp : operators) {
			if (currentOp.type == Operator.SEMIJOIN_OPERATOR) {
				SemiJoinOperator semiOp = (SemiJoinOperator)currentOp;
				int aliasID = semiOp.drivingAliasID;
				MetaQuery query = metadata.queryList.get(metadata.currentQueryID);
				String alias = query.aliases.get(aliasID);
				semiDeallocString += "\tdelete[] " + alias + "_bool_array;\n";
				semiDeallocString += "\n";
			}
		
		}
		return semiDeallocString;
		
	}


	
	private static String initResultArray(AggregationOperator aggregation) {
		
		String resultString = "\n\tRC = new int[metadata.idx_domains[" + aggregation.indexID + "][0]]();\n";
		
		if (aggregation.dataType == AggregationOperator.AGGREGATION_INT) {
			resultString += "\tR = new int[metadata.idx_domains[" + aggregation.indexID + "][0]]();\n";
		}
		else if (aggregation.dataType == AggregationOperator.AGGREGATION_DOUBLE) {
			resultString += "\tR = new double[metadata.idx_domains[" + aggregation.indexID + "][0]]();\n";
		}
		return resultString;
	}
	
	/*
	 * Function name: initSemiJoinArray
	 * Description: This function generates the cpp code for the initialization of the boolean array for semijoin ops
	 * Input:	
	 * 			operators: 	A list of operators for the query
	 * 			metadata:	The code generator's accessible metadata for the gq-fast database
	 * Output:	A String of generated code per the description's specification
	 * 
	 */
	private static String initSemiJoinArray(List<Operator> operators,
			Metadata metadata, List<String> globalsCppCode) {
		
		String resultString = "\n";
		MetaQuery query = metadata.queryList.get(metadata.currentQueryID);
		
		for (Operator currentOp: operators) {
			if (currentOp.type == Operator.SEMIJOIN_OPERATOR) {
				SemiJoinOperator currentSemiJoinOp = (SemiJoinOperator) currentOp;
				int indexID = currentSemiJoinOp.drivingAliasIndexID;
				int aliasID = currentSemiJoinOp.drivingAliasID;
				String alias = query.aliases.get(aliasID);
				
				String globalDeclarationString = "\nstatic bool* " + alias + "_bool_array;\n";
				globalsCppCode.add(globalDeclarationString);
				
				resultString += "\t" + alias + "_bool_array = new bool[metadata.idx_domains[" + indexID + "][0]]();\n";
				
			}
		}
		
		return resultString;
	}

	
	
	private static void initDecodeVars(List<Operator> operators, List<String> mainCppCode, 
			List<String> globalsCppCode, Metadata metadata, MetaQuery query) {
		
		
		// Skips last operator since it is the aggregation
		for (int i=0; i<operators.size()-1; i++) {
			Operator currentOperator = operators.get(i);
			
			if (currentOperator.type == Operator.JOIN_OPERATOR || currentOperator.type == Operator.SEMIJOIN_OPERATOR) {
				
				int indexID;
				String alias;
				List<Integer> columnIDs;
				if (currentOperator.type == Operator.JOIN_OPERATOR) {
					JoinOperator tempJoinOp = (JoinOperator) currentOperator;
					indexID = tempJoinOp.indexID;
					int aliasID = tempJoinOp.alias;
		
					alias = query.aliases.get(aliasID);
					columnIDs = tempJoinOp.columnIDs;
				}
				else {
					SemiJoinOperator semiJoinOp = (SemiJoinOperator) currentOperator;
					indexID = semiJoinOp.indexID;
					int aliasID = semiJoinOp.alias;
					
					alias = query.aliases.get(aliasID);
					columnIDs = semiJoinOp.columnIDs;
				}
				
				
				for (int j=0; j<columnIDs.size(); j++) {
					
					int columnID = columnIDs.get(j);
					MetaIndex tempIndex = metadata.indexList.get(indexID);
					int columnEncoding = tempIndex.columnEncodingsList.get(columnID);
				
					if (columnEncoding == Metadata.ENCODING_BCA) {
						String nextGlobal = "\nstatic uint32_t* " + alias + "_col" + j + "_bits_info;\n";
						globalsCppCode.add(nextGlobal);
						String nextMain = "\n\t"+alias+ "_col" + j + "_bits_info = idx[" + indexID + "]->dict[" + columnID + "]->bits_info;\n";
						mainCppCode.add(nextMain);
					}
					else if (columnEncoding == Metadata.ENCODING_HUFFMAN) {
						String nextGlobal = "\nstatic int* "+ alias + "_col" + j + "_huffman_tree_array;\n";
						nextGlobal += "static bool* " + alias + "_col" + j + "_huffman_terminator_array;\n";
						globalsCppCode.add(nextGlobal);
						String nextMain = "\n\t" + alias + "_col" + j + "_huffman_tree_array = idx[" + indexID + "]->huffman_tree_array[" + columnID + "];\n";
						nextMain += "\t"+ alias + "_col" + j + "_huffman_terminator_array = idx[" + indexID + "]->huffman_terminator_array[" + columnID + "];\n";
						mainCppCode.add(nextMain);
	 				}
					
				}
				
			}
			else if (currentOperator.type == Operator.INTERSECTION_OPERATOR) {
					IntersectionOperator tempIntOp = (IntersectionOperator)currentOperator;
					// TODO: Intersection implementation
			}		
		}
		
	}
	

	
	private static void initialDeclarations(List<String> globalsCppCode,
			AggregationOperator aggregation, MetaQuery query) {
		
		if (query.numThreads > 1) {
			String arguments = "\nstatic args_threading arguments[NUM_THREADS];\n";
			globalsCppCode.add(arguments);
		}
		
		String resultsGlobals = "";
		if (aggregation.dataType == AggregationOperator.AGGREGATION_INT)  {		
			resultsGlobals += "\nstatic int* R;\n";
		}
		else if (aggregation.dataType == AggregationOperator.AGGREGATION_DOUBLE) {
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
		
		if (currentEncoding == Metadata.ENCODING_BB) {
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
		else if (currentEncoding == Metadata.ENCODING_HUFFMAN) {
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
		else if (currentEncoding == Metadata.ENCODING_BCA) {
			function += tabString + elementName + " = " + alias + "_col" + currentCol + "_bits_info[1];\n";
			function += tabString + elementName + " &= *" + pointerName + ";\n";
		}
		else if (currentEncoding == Metadata.ENCODING_UA) {
			function += tabString + elementName + " = *" + pointerName + ";\n"; 
		}
		return function;
	}
	
	/*
	 * Function generateDecodeFunctionBody
	 * 
	 * Input:
	 * 			indexID: 	the id of the current index
	 * 			alias:   The alias name of the current operator in the query
	 * 			currentEncoding: 	the encoding type as represented by an int constant
	 * 			columnIteration:	the current column iteration
	 * 			currentCol:			The id of the current column (could be different than 'columnIteration')
	 * 			sizeName:			A string of the cpp that is the variable name for the current fragment cardinality
	 * 			currentFragmentBytesName:	A string of cpp that is the variable name for the current fragment bytes
	 * 			pointerName:		A string of cpp that is the variable name for the current fragment pointer
	 * 			bufferPoolTrackingArray:	Keeps track of the buffer pool array to use
	 * 
	 * Output:
	 * 			A string that represents the body of the function in cpp that is the decoding of a fragment
	 * 
	 */
	private static String generateDecodeFunctionBody(boolean preThreading, int indexID, String alias,
			int currentEncoding, int columnIteration, int currentCol, String sizeName,
			String currentFragmentBytesName, String pointerName,
			int[][] bufferPoolTrackingArray, int currPool, int currentByteSize) {
		
		String function = "\n";
		String tabString = "\t";
		
		String bufferArraysPart = new String();
		if (preThreading) {
			bufferArraysPart = "buffer_arrays[" + indexID + "][" + currentCol + "][0][" + currPool + "]";
		}
		else {
			bufferArraysPart = "buffer_arrays[" + indexID + "][" + currentCol + "][thread_id][" + currPool + "]";
		}
		if (columnIteration == 0) {
			// Size is initially 0, is passed by reference, and will calculated in the function
			if (currentEncoding == Metadata.ENCODING_UA) {			
				function += tabString + sizeName + " = " + currentFragmentBytesName + "/" + currentByteSize + ";\n"; 
				function += "\n" + tabString + "for (uint32_t i=0; i<" + sizeName + "; i++) {\n";
				tabString += "\t";
				function += tabString + bufferArraysPart + "[i] = *" + pointerName + "++;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
					
			}
			else if (currentEncoding == Metadata.ENCODING_BB) {
							
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
			else if (currentEncoding == Metadata.ENCODING_HUFFMAN) {
				
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
			else if (currentEncoding == Metadata.ENCODING_BCA) {
				
				String bitsInfoPrefix = alias + "_col" + currentCol + "_bits_info";
				
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
				
				function += tabString + bufferArraysPart + "[i] = encoded_value;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				
			}
			
		}
		else {
			// Size is pre-calculated and will be used to control the iteration
			if (currentEncoding == Metadata.ENCODING_UA) {
				function += tabString + "for (uint32_t i=0; i<" + sizeName + "; i++) {\n";
				tabString += "\t";
				function += tabString + bufferArraysPart + "[i] = *" + pointerName + "++;\n";
				tabString = tabString.substring(0, tabString.length()-1);
				function += tabString + "}\n";
				
			}
			else if (currentEncoding == Metadata.ENCODING_BB) {
				
				
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
			else if (currentEncoding == Metadata.ENCODING_HUFFMAN) {
				
				function += tabString + "bool* terminate_start = &("+alias+ "_col" + currentCol + "_huffman_terminator_array[0]);\n" ;
				function += tabString + "int* tree_array_start = &("+alias + "_col" + currentCol + "_huffman_tree_array[0]);\n";
				
				function += "\n" + tabString + "int mask = 0x100;\n";
				function += "\n" + tabString + "for (uint32_t i=0; i<"+sizeName+"; i++) {\n";
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
			else if (currentEncoding == Metadata.ENCODING_BCA) {
				String bitsInfoPrefix = alias + "_col" + currentCol + "_bits_info";
				
				function += tabString + "int bit_pos = 0;\n";
				function += tabString + "for (uint32_t i=0; i<" + sizeName + "; i++) {\n";
				tabString += "\t";
				function += tabString + "uint32_t encoded_value = " + bitsInfoPrefix + "[1] << bit_pos;\n";
				function += tabString + "uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(" + pointerName + ");\n";
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
	 * 
	 * 	Description: This function is called by evaluateJoin and deals with the decoding portion of the join evaluation for a particular column.
	 *	Input: 
	 * 			currOp:	The current operator's id (i.e. index in the operators list)
	 * 			columnIteration: The current column iteration	
	 * 			tabString:	A string that keeps track of the tabbing of the generated code (for readability)
	 * 			currMetaIndex: metadata for the current index 
	 * 			query: metadata for the current query
	 * 			functionHeadersCppCode:	A list of strings that make up the headers of the functions that are generated (except the original function)
	 * 			functionCppCode: A list of strings, each of which is a function and function body
	 * 			indexID: The id of the current index 
	 * 			currentCol:	The current column ID (note that this could potentially be different than 'columnIteration' if certain columns were omitted)
	 * 			currentFragmentRow:	A String of the cpp that is the variable name for the current row
	 * 			currentFragmentBytesName:	A String of cpp that is the variable name for the current fragment bytes 
	 * 			bufferPoolTrackingArray:	To keep track of which array in the pool to use
	 * 			entityFlag:		true if current index is an entity table, false if it is a relationship table
	 * 
	 * 
	 *	Output:
	 *			A String that continues the cpp code for the original function
	 */
	private static String joinGenerateDecodeFragmentFunction(boolean preThreading, String alias, int aliasID, int columnIteration, StringBuilder tabString,
			MetaIndex currMetaIndex, MetaQuery query, List<String> functionHeadersCppCode,
			List<String> functionsCppCode, int indexID, int currentCol,
			String currentFragmentRow, String currentFragmentBytesName, int[][] bufferPoolTrackingArray, boolean entityFlag) {
		
		String mainString = "\n";
		String currFunctionHeader = new String();
		String currFunction = new String();
		
		
		int currentBytesSize = currMetaIndex.columnEncodedByteSizesList.get(currentCol);
		int currentEncoding = currMetaIndex.columnEncodingsList.get(currentCol);
		
		String pointerName = alias + "_col" + currentCol + "_ptr";
		String pointerString = new String();
		String functionParameters = "(";
		if (!preThreading && !entityFlag) {
			functionParameters += "int thread_id, ";
		}
		// UA pointer points to type of size of element
		if (currentEncoding == Metadata.ENCODING_UA) {
			switch (currentBytesSize) {
			case Metadata.BYTES_1: 
				pointerString = "unsigned char* " + pointerName + 
				" = &(idx[" + indexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]);\n";
				functionParameters += "unsigned char* " + pointerName;
				break;
			case Metadata.BYTES_2: 
				pointerString = "uint16_t* " + pointerName + 
				" = reinterpret_cast<uint16_t *>(&(idx[" + indexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
				functionParameters += "uint16_t* " + pointerName;
				break;
			case Metadata.BYTES_4:
				pointerString = "uint32_t* " + pointerName + 
				" = reinterpret_cast<uint32_t *>(&(idx[" + indexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
				functionParameters += "uint32_t* " + pointerName;
				break;
			case Metadata.BYTES_8:
				pointerString = "uint64_t* " + pointerName +
				" = reinterpret_cast<uint64_t *>(&(idx[" + indexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
				functionParameters += "uint64_t* " + pointerName;
				break;
			}
		}
		else if (entityFlag && currentEncoding == Metadata.ENCODING_BCA){
			pointerString = "uint64_t* " + pointerName +
					" = reinterpret_cast<uint64_t *>(&(idx[" + indexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]));\n";
			functionParameters += "uint64_t* " + pointerName;
		}
		else {
			pointerString = "unsigned char* " + pointerName + 
					" = &(idx[" + indexID + "]->fragment_data[" + currentCol + "][" + currentFragmentRow + "[" + currentCol + "]]);\n";
			functionParameters += "unsigned char* " + pointerName;
		}
		
		mainString += tabString + pointerString;
		
		
		String functionName = query.queryName +"_"+ alias + "_col" + currentCol;

		switch (currentEncoding) {

		case Metadata.ENCODING_UA:	{
			functionName += "_decode_UA";
			break;
		}
		case Metadata.ENCODING_BCA: {
			functionName += "_decode_BCA";
			break;
		}
		case Metadata.ENCODING_BB: {
			functionName += "_decode_BB";
			break;
		}
		case Metadata.ENCODING_HUFFMAN: {
			functionName += "_decode_Huffman";
			break;
		}

		}

		if (!preThreading && !entityFlag) {
			functionName += "_threaded";
		}
		
		
		if (entityFlag) {
			String elementName = alias + "_col" + currentCol + "_element";
			
			mainString += tabString + getPrimitive(currentBytesSize) + " " + elementName + ";\n";
			functionParameters += ", " + getPrimitive(currentBytesSize) + " & " + elementName + ")";
			
			currFunction += "\nvoid " + functionName + functionParameters + " {\n";
			currFunctionHeader += "\nextern inline void " + functionName + functionParameters + " __attribute__((always_inline));\n";
			
			mainString += tabString + functionName + "(" + pointerName + ", " + elementName + ");\n";
		

			currFunction += generateDecodeFunctionBodyEntityTable(pointerName, elementName, alias, currentEncoding, currentCol);
			currFunction += "}\n";
			
			
		}
		else {
			String sizeName = alias + "_fragment_size";
	
			// First column's decoding determines the fragment size for all subsequent column decodings


			if (columnIteration == 0) {
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
			if (columnIteration == 0 ) {
				mainString += ", " + currentFragmentBytesName; 
			}
			mainString += ", " + sizeName + ");\n";

			int currPool = ++bufferPoolTrackingArray[indexID][currentCol];
			query.setBufferPoolID(aliasID, currentCol, currPool);
			
			currFunction += generateDecodeFunctionBody(preThreading, indexID, alias, currentEncoding, columnIteration, 
					currentCol, sizeName, currentFragmentBytesName, pointerName, bufferPoolTrackingArray, currPool, currentBytesSize);
			currFunction += "}\n";

			

		}
		
		functionHeadersCppCode.add(currFunctionHeader);
		functionsCppCode.add(currFunction);
		return mainString;
	}
	

	/*
	 * Function getPrimitive
	 * 
	 * Input:
	 * 			colBytes:	A flag for how many bytes an associated value would use
	 * Output:
	 * 			A string that gives the primitive type for the corresponding flag. Returns null if no known flag is found.
	 * 
	 */
	private static String getPrimitive(int colBytes) {
		switch (colBytes) {
		case Metadata.BYTES_1:	return "unsigned char";
		case Metadata.BYTES_2:	return "uint16_t";
		case Metadata.BYTES_4:	return "uint32_t";
		case Metadata.BYTES_8:	return "uint64_t";
		}
		return null;
	}
	

	private static String evaluatePreviousJoinEntityFlag(Operator currentOp,
			MetaQuery query, boolean threadID, StringBuilder tabString,
			int[] closingBraces, String drivingAlias, int drivingAliasCol,
			String currAlias, List<Integer> columnIDs, int drivingAliasID, int indexByteSize, 
			String currentFragmentRow, int indexID, boolean preThreading, int currentAliasID, 
			MetaIndex currMetaIndex, List<String> functionHeadersCppCode,
			List<String> functionsCppCode, int[][] bufferPoolTrackingArray, boolean entityFlag) {
		String mainString = new String();
		
		// SemiJoin Check
		if (currentOp.type == Operator.SEMIJOIN_OPERATOR) {
			int drivingPool = query.getBufferPoolID(drivingAliasID, drivingAliasCol);
			SemiJoinOperator tempOp = (SemiJoinOperator)currentOp;
			int drivingIndexID = tempOp.drivingAliasIndexID;
			if (threadID) {
				mainString += "\n" + tabString + "if (!(" + drivingAlias 
						+ "_bool_array[buffer_arrays[" + drivingIndexID +"][" + drivingAliasCol +"][thread_id]["+drivingPool+"]["+drivingAlias+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivingAlias + 
						"_bool_array[buffer_arrays[" + drivingIndexID +"][" + drivingAliasCol +"][thread_id]["+drivingPool+"]["+drivingAlias+ "_it]] = true;\n";
			}
			else {
				mainString += "\n" + tabString + "if (!(" + drivingAlias 
					+ "_bool_array[buffer_arrays[" + drivingIndexID +"][" + drivingAliasCol +"][0]["+drivingPool+"]["+drivingAlias+ "_it]])) {\n";
				closingBraces[0]++;
				tabString.append("\t");
				mainString += tabString + drivingAlias + 
					"_bool_array[buffer_arrays[" + drivingIndexID +"][" + drivingAliasCol +"][0]["+drivingPool+"]["+drivingAlias+ "_it]] = true;\n";
			}
		}
		
		String elementString = drivingAlias  + "_col" + drivingAliasCol + "_element";
		mainString += "\n" + tabString + getPrimitive(indexByteSize) + "* ";

		mainString += currentFragmentRow + " = idx[" + indexID + "]->" +
				"index_map["+ elementString +"];\n"; 
		
		String currentFragmentBytesName = new String();
		for (int k=0; k<columnIDs.size(); k++) {
			int currentCol = columnIDs.get(k);

			if (k == 0) {
				currentFragmentBytesName = currAlias + "_col" + currentCol + "_bytes";
				mainString += tabString + "uint32_t " + currentFragmentBytesName + " = " +
						"idx[" + indexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}
		
			mainString += joinGenerateDecodeFragmentFunction(preThreading, currAlias, currentAliasID, k, tabString, currMetaIndex, query, 
					functionHeadersCppCode, functionsCppCode,indexID, currentCol, currentFragmentRow, 
					currentFragmentBytesName, bufferPoolTrackingArray, entityFlag);
			
		}
		
		return mainString;
	}
	
	private static String evaluatePreviousJoinRelationshipTable(
			Operator currentOp, MetaQuery query, boolean threadID,
			StringBuilder tabString, int[] closingBraces, String drivingAlias,
			int drivingAliasCol, String currAlias, List<Integer> columnIDs,
			int drivingAliasID, int indexByteSize, String currentFragmentRow,
			int indexID, boolean preThreading, int currentAliasID,
			MetaIndex currMetaIndex, List<String> functionHeadersCppCode,
			List<String> functionsCppCode, int[][] bufferPoolTrackingArray,
			boolean entityFlag, String previousAlias, int previousIndexID,
			List<Integer> previousColumnIDs, Metadata metadata, boolean justStartedThreading) {
		String mainString = new String();
		
		if (justStartedThreading) {
			mainString += "\n" + tabString + "for (; " +
					previousAlias + "_it < " + previousAlias + "_fragment_size; " + previousAlias + "_it++) {\n";
		}
		else {
			mainString += "\n" + tabString + "for (uint32_t " + previousAlias + "_it = 0; " +
				previousAlias + "_it < " + previousAlias + "_fragment_size; " + previousAlias + "_it++) {\n";
		}
		closingBraces[0]++;
		tabString.append("\t");

		mainString += "\n";
		
		// SemiJoin Check
		if (currentOp.type == Operator.SEMIJOIN_OPERATOR) {
			int drivingPool = query.getBufferPoolID(drivingAliasID, drivingAliasCol);
			SemiJoinOperator tempOp = (SemiJoinOperator)currentOp;
			int drivingIndexID = tempOp.drivingAliasIndexID;
			if (threadID) {
				mainString += "\n" + tabString + "if (!(" + drivingAlias 
						+ "_bool_array[buffer_arrays[" + drivingIndexID +"][" + drivingAliasCol +"][thread_id]["+drivingPool+"]["+drivingAlias+ "_it]])) {\n";
					closingBraces[0]++;
					tabString.append("\t");
					mainString += tabString + drivingAlias + 
						"_bool_array[buffer_arrays[" + drivingIndexID +"][" + drivingAliasCol +"][thread_id]["+drivingPool+"]["+drivingAlias+ "_it]] = true;\n";
			}
			else {
				mainString += "\n" + tabString + "if (!(" + drivingAlias 
					+ "_bool_array[buffer_arrays[" + drivingIndexID +"][" + drivingAliasCol +"][0]["+drivingPool+"]["+drivingAlias+ "_it]])) {\n";
				closingBraces[0]++;
				tabString.append("\t");
				mainString += tabString + drivingAlias + 
					"_bool_array[buffer_arrays[" + drivingIndexID +"][" + drivingAliasCol +"][0]["+drivingPool+"]["+drivingAlias+ "_it]] = true;\n";
			}
		}
		
		for (int previousColID : previousColumnIDs) {

			int colBytes = metadata.indexList.get(previousIndexID).columnEncodedByteSizesList.get(previousColID);
			mainString += tabString + getPrimitive(colBytes) + " ";

			if (threadID) {
				mainString += previousAlias + "_col" + previousColID + "_element = " +
					"buffer_arrays[" + previousIndexID + "][" + previousColID + "][thread_id]" +
					"["+bufferPoolTrackingArray[previousIndexID][previousColID]+ "][" + previousAlias + "_it];\n";
			}
			else {
				mainString += previousAlias + "_col" + previousColID + "_element = " +
						"buffer_arrays[" + previousIndexID + "][" + previousColID + "][0]" +
						"["+bufferPoolTrackingArray[previousIndexID][previousColID]+ "][" + previousAlias + "_it];\n";
			}
			
		}

		
		
		String elementString = drivingAlias + "_col" + drivingAliasCol + "_element";


		mainString += "\n" + tabString + getPrimitive(indexByteSize) + "* ";
		mainString += currentFragmentRow + " = idx[" + indexID + "]->" +
				"index_map["+ elementString +"];\n"; 
		String currentFragmentBytesName = "";
		for (int k=0; k<columnIDs.size(); k++) {
			int currentCol = columnIDs.get(k);

			if (k == 0 && !entityFlag) {
				currentFragmentBytesName = currAlias + "_col" + currentCol + "_bytes";
				mainString += tabString + "uint32_t " + currentFragmentBytesName + " = " +
						"idx[" + indexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}

			mainString += joinGenerateDecodeFragmentFunction(preThreading, currAlias, currentAliasID, k, tabString, 
					currMetaIndex, query, functionHeadersCppCode, functionsCppCode,indexID, currentCol, currentFragmentRow, 
					currentFragmentBytesName, bufferPoolTrackingArray, entityFlag);

		}
		
		
		
		return mainString;
	}



	

	private static String evaluatePreviousSelection(Operator previousOp, MetaQuery query, StringBuilder tabString, 
			int[] closingBraces, String drivingAlias, int drivingAliasCol, int indexByteSize, String currentFragmentRow,
			int indexID, List<Integer> columnIDs, boolean entityFlag, String currAlias, int currentAliasID,
			MetaIndex currMetaIndex, List<String> functionHeadersCppCode, List<String> functionsCppCode, boolean preThreading,
			int[][] bufferPoolTrackingArray, boolean justStartedThreading) {
		
		String mainString = new String();
		
		SelectionOperator previousSelectionOp = (SelectionOperator) previousOp;

		int previousAliasID = previousSelectionOp.alias;
		String previousAlias = query.aliases.get(previousAliasID);

		if (justStartedThreading) {
			mainString += "\n" + tabString + "for (; " + previousAlias + "_it<" + 
					previousSelectionOp.selectionsList.size() + "; " + previousAlias + "_it++) {\n";
		}
		else {
			mainString += "\n" + tabString + "for (int "+ previousAlias + "_it = 0; " + previousAlias + "_it<" + 
				previousSelectionOp.selectionsList.size() + "; " + previousAlias + "_it++) {\n";
		}
		closingBraces[0]++;
		tabString.append("\t");
		mainString += "\n" + tabString + "uint64_t " + previousAlias + "_col0_element = " + previousAlias + "_list[" +previousAlias+"_it];\n";


		String elementString = drivingAlias + "_col" + drivingAliasCol + "_element";


		mainString += "\n" + tabString + getPrimitive(indexByteSize) + "* ";
		mainString += currentFragmentRow + " = idx[" + indexID + "]->" +
				"index_map["+ elementString +"];\n"; 
		String currentFragmentBytesName = "";
		for (int k=0; k<columnIDs.size(); k++) {
			int currentCol = columnIDs.get(k);

			if (k == 0 && !entityFlag) {
				currentFragmentBytesName = currAlias + "_col" + currentCol + "_bytes";
				mainString += tabString + "uint32_t " + currentFragmentBytesName + " = " +
						"idx[" + indexID + "]->index_map["+ elementString +"+1]" +
						"[" + currentCol + "] - " + currentFragmentRow + "[" + currentCol + "];\n";
				mainString += tabString + "if(" + currentFragmentBytesName + ") {\n";
				closingBraces[0]++;
				tabString.append("\t");
			}

			mainString += joinGenerateDecodeFragmentFunction(preThreading, currAlias, currentAliasID, k, tabString, currMetaIndex, query, functionHeadersCppCode, functionsCppCode,indexID, currentCol, currentFragmentRow, 
					currentFragmentBytesName, bufferPoolTrackingArray, entityFlag);

		}
		
		
		return mainString;
	}

	
	/*
	 * Function evaluateJoin
	 * 
	 * Input: 
	 * 			i:		The current operator iteration
	 * 			operators:	The list of operators to the code generator
	 * 			currentOp:	The current operator that is known to be a JoinOperator
	 * 			metadata:	The metadata for the code generator
	 * 			tabString:	A string that keeps track of the tabbing of the generated code (for readability)
	 * 			closingBraces:	Keeps track of the number of closing braces to place at the end of the code
	 * 			bufferPoolTrackingArray:	This is tracking for the buffer_arrays buffer pool size, it keeps track of which array within the pool to use
	 * 			query:		Metadata on the query
	 * 			functionHeadersCppCode:	A list of strings that make up the headers of the functions that are generated (except the original function)
	 * 			functionCppCode: A list of strings, each of which is a function and function body
	 * Output: 
	 * 			Emits a string that is the next piece of the code for the original function. Also, associated String Lists functionHeadersCppCode and functionCppCode are updated.
	 * 
	 */
		
	private static String evaluateJoin(boolean preThreading, int i, List<Operator> operators, Operator currentOp, Metadata metadata, 
			StringBuilder tabString, int[] closingBraces, int[][] bufferPoolTrackingArray, MetaQuery query, List<String> functionHeadersCppCode, List<String> functionsCppCode) {

		int indexID;
		int drivingAliasID;
		int drivingAliasCol;
		int currentAliasID;
		boolean entityFlag;
		boolean threadID = true;
		List<Integer> columnIDs;
		if (currentOp.type == Operator.JOIN_OPERATOR) {
			JoinOperator currentJoinOp = (JoinOperator) currentOp;
			indexID = currentJoinOp.indexID;
			drivingAliasID = currentJoinOp.drivingAliasID;
			drivingAliasCol = currentJoinOp.drivingAliasColumn;
			currentAliasID = currentJoinOp.alias;
			columnIDs = currentJoinOp.columnIDs;
			entityFlag = currentJoinOp.entityFlag;
		}
		// Assumed to be Semi-Join
		else {
			SemiJoinOperator currentSemiJoinOp = (SemiJoinOperator) currentOp;
			indexID = currentSemiJoinOp.indexID;
			drivingAliasID = currentSemiJoinOp.drivingAliasID;
			drivingAliasCol = currentSemiJoinOp.drivingAliasColumn;
			currentAliasID = currentSemiJoinOp.alias;
			columnIDs = currentSemiJoinOp.columnIDs;
			entityFlag = currentSemiJoinOp.entityFlag;
		}
		
		
		String drivingAlias = query.aliases.get(drivingAliasID);
		String currAlias = query.aliases.get(currentAliasID);
		if (preThreading) {
			query.preThreading[currentAliasID] = true;
		}
		if (query.preThreading[drivingAliasID]) {
			threadID = false;
		}
		MetaIndex currMetaIndex = metadata.indexList.get(indexID);
		int indexByteSize = currMetaIndex.indexMapByteSize;
		String mainString = new String();
		String currentFragmentRow = "row_op" + i;
		
		if (i == 0) {
			//TODO: Implementation when First Operator is a Join, meaning there is no selection
		}
		else {
			Operator previousOp = operators.get(i-1);
			boolean loopAgain = true;
			boolean justStartedThreading = false;
			while(loopAgain) {
				if (previousOp.type == Operator.THREADING_OPERATOR) {
					// It is assumed that a THREADING_OPERATOR will never be the first operator
					// It is also assumed that an Entity Table join will never immediately follow a ThreadingOperator
					// 
					previousOp = operators.get(i-2);
					justStartedThreading = true;
				}
				else if (previousOp.type == Operator.JOIN_OPERATOR || previousOp.type == Operator.SEMIJOIN_OPERATOR) {
					loopAgain = false;
					boolean previousEntityFlag;
					int previousAliasID;
					String previousAlias;
					int previousIndexID;
					List<Integer> previousColumnIDs;
					//int previousLoopColumn;

					if (previousOp.type == Operator.JOIN_OPERATOR) {
						JoinOperator previousJoinOp = (JoinOperator) previousOp;
						previousEntityFlag = previousJoinOp.entityFlag;
						previousAliasID = previousJoinOp.alias;
						previousAlias = query.aliases.get(previousAliasID);
						previousIndexID = previousJoinOp.indexID;
						previousColumnIDs = previousJoinOp.columnIDs;
						//previousLoopColumn = previousJoinOp.loopColumn;

					}
					else {
						SemiJoinOperator previousSemiJoinOp = (SemiJoinOperator) previousOp;
						previousEntityFlag = previousSemiJoinOp.entityFlag;
						previousAliasID = previousSemiJoinOp.alias;
						previousAlias = query.aliases.get(previousAliasID);		
						previousIndexID = previousSemiJoinOp.indexID;
						previousColumnIDs = previousSemiJoinOp.columnIDs;
						//previousLoopColumn = previousSemiJoinOp.loopColumn;
					}
					if (previousEntityFlag) {
						// Previous operator was an entity table
						// There is no need for an additional loop
						
						mainString += evaluatePreviousJoinEntityFlag(currentOp, query, threadID, tabString, closingBraces, drivingAlias, 
								drivingAliasCol, currAlias, columnIDs, drivingAliasID, indexByteSize, currentFragmentRow, indexID,
								preThreading, currentAliasID, currMetaIndex, functionHeadersCppCode, functionsCppCode, 
								bufferPoolTrackingArray, entityFlag);


					}
					else{
						// Previous operator was relationship table
						mainString += evaluatePreviousJoinRelationshipTable(currentOp, query, threadID, tabString, closingBraces, drivingAlias, 
								drivingAliasCol, currAlias, columnIDs, drivingAliasID, indexByteSize, currentFragmentRow, indexID,
								preThreading, currentAliasID, currMetaIndex, functionHeadersCppCode, functionsCppCode, bufferPoolTrackingArray, entityFlag, 
								previousAlias, previousIndexID, previousColumnIDs, metadata, justStartedThreading);

					}
				}
				else if (previousOp.type == Operator.SELECTION_OPERATOR) {
					loopAgain = false;
					mainString += evaluatePreviousSelection(previousOp, query, tabString, 
							closingBraces, drivingAlias, drivingAliasCol, indexByteSize, currentFragmentRow,
							indexID, columnIDs, entityFlag, currAlias, currentAliasID,
							currMetaIndex, functionHeadersCppCode, functionsCppCode, preThreading,
							bufferPoolTrackingArray, justStartedThreading);
					

				}
			} 
		}
		return mainString;
	}
		


	private static String evaluateSelection(int i, Operator currentOp, StringBuilder tabString, MetaQuery query) {
	
		
		String mainSelectionString = new String();
		
		SelectionOperator selectionOp = (SelectionOperator) currentOp;
		int selectionAliasID = selectionOp.alias;
		String selectionAlias = query.aliases.get(selectionAliasID);
		
		int numSelections = selectionOp.selectionsList.size();
		
		
		mainSelectionString += "\n" + tabString + "uint64_t* " + selectionAlias + "_list = new uint64_t[" + numSelections + "];\n";
		
		for (int j=0; j<numSelections; j++) {
			int currSelection = selectionOp.selectionsList.get(j);
			mainSelectionString += tabString + selectionAlias + "_list["+j+"] = " + currSelection + ";\n";
		}
		
		
		return mainSelectionString;
	}

	private static String evaluateAggregation(boolean preThreading, int i, List<Operator> operators,
			Operator currentOp, Metadata metadata, StringBuilder tabString,
			int[] closingBraces, int[][] bufferPoolTrackingArray, MetaQuery query) {
		
		String mainString = new String();
		
		AggregationOperator aggregationOp = (AggregationOperator) currentOp;
		
		Operator previousOp = operators.get(i-1);
	
		boolean useLoop = true;
		if (previousOp.type == Operator.JOIN_OPERATOR) {
			JoinOperator previousJoinOp = (JoinOperator) previousOp;
			if (!previousJoinOp.entityFlag) {
				
				int previousAliasID = previousJoinOp.alias;
				String previousAlias = query.aliases.get(previousAliasID);
				boolean previousThreadID = query.preThreading[previousAliasID];
				
				
				mainString += "\n" + tabString + "for (uint32_t " + previousAlias + "_it = 0; " + previousAlias + "_it " +
						"< " + previousAlias + "_fragment_size; " + previousAlias + "_it++) {\n";
				closingBraces[0]++;
				tabString.append("\t");

				int previousIndexID = previousJoinOp.indexID;
				for (int previousColID : previousJoinOp.columnIDs) {

					int colBytes = metadata.indexList.get(previousIndexID).columnEncodedByteSizesList.get(previousColID);
					mainString += tabString + getPrimitive(colBytes) + " ";

					if (previousThreadID) {
						mainString += previousAlias + "_col" + previousColID + "_element = " +
							"buffer_arrays[" + previousIndexID + "][" + previousColID + "][0]" +
							"["+bufferPoolTrackingArray[previousIndexID][previousColID]+ "][" + previousAlias + "_it];\n";
					}
					else {
						mainString += previousAlias + "_col" + previousColID + "_element = " +
								"buffer_arrays[" + previousIndexID + "][" + previousColID + "][thread_id]" +
								"["+bufferPoolTrackingArray[previousIndexID][previousColID]+ "][" + previousAlias + "_it];\n";
					}
				}
			}
		}
		else if (previousOp.type == Operator.SELECTION_OPERATOR){
			
			
		}
				
		int drivingAliasID = aggregationOp.drivingAlias;
		String drivingAlias = query.aliases.get(drivingAliasID); 
		int drivingAliasCol = aggregationOp.drivingAliasColumn;
		int drivingAliasIndex = aggregationOp.drivingAliasIndexID;
		String elementString = drivingAlias + "_col" + drivingAliasCol + "_element";
		

		mainString += "\n" + tabString + "RC[" + elementString + "] = 1;\n";
		
		if (!preThreading) {
			mainString += "\n" + tabString + "pthread_spin_lock(&spin_locks["+ drivingAliasIndex + "]["+ elementString +"]);\n";
		}
		String delims = "[ ]+";
		String[] tokens = aggregationOp.aggregationString.split(delims);
		String reconstructedString = new String();
		for (int j=0; j< tokens.length;j++) {
			String upTo2Characters = tokens[j].substring(0, Math.min(tokens[j].length(), 2));
			if (upTo2Characters.equals("op")) {
				// Reads the number immediately following "op"
				String num_letter = Character.toString(tokens[j].charAt(2));
				int aggregationAliasNum = Integer.parseInt(num_letter);
				int aliasID = aggregationOp.aggregationVariablesAliases.get(aggregationAliasNum);
				String alias = query.aliases.get(aliasID);
				int aliasCol = aggregationOp.aggregationVariablesColumns.get(aggregationAliasNum);
				String fullElementName = alias + "_col" + aliasCol + "_element";
				reconstructedString += fullElementName;
			}
			else {
				reconstructedString += tokens[j];
			}
		}
		
		
		
		mainString += tabString + "R[" + elementString +"] += " + reconstructedString + ";";
	
		if (!preThreading) {
			mainString += "\n" + tabString + "pthread_spin_unlock(&spin_locks["+ drivingAliasIndex + "]["+ elementString +"]);\n";
		}

		mainString += "\n";

			

		return mainString;
	}
	

	private static String initThreading(Operator currentOp, Metadata metadata,
			StringBuilder tabString, MetaQuery query,
			List<String> functionHeadersCppCode, List<String> functionsCppCode) {
		
		String mainString = new String();
		
		ThreadingOperator threadingOp = (ThreadingOperator) currentOp;
		int aliasID = threadingOp.drivingAliasID;
		String alias = query.aliases.get(aliasID);
		
		mainString += "\n" + tabString + "uint32_t thread_size = " + alias + "_fragment_size/NUM_THREADS;\n"; 
		mainString += tabString + "uint32_t position = 0;\n";
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
		mainString += tabString + "pthread_create(&threads[i], NULL, &pthread_" + query.queryName + 
				"_worker, (void *) &arguments[i]);\n";
		tabString.setLength(tabString.length() - 1);
		mainString += tabString + "}\n";
		mainString += "\n" + tabString + "for (int i=0; i<NUM_THREADS; i++) {\n";
		tabString.append("\t");
		mainString += tabString + "pthread_join(threads[i], NULL);\n";
		tabString.setLength(tabString.length() - 1);
		mainString += tabString + "}\n";
	
		
		String threadingFunction = new String();
		String threadFunctionHeader = "\nvoid* pthread_" + query.queryName + "_worker(void* arguments);\n";
		threadingFunction += "\nvoid* pthread_" + query.queryName + "_worker(void* arguments) {\n";
		threadingFunction += "\n\targs_threading* args = (args_threading *) arguments;\n";
		threadingFunction += "\n\tuint32_t " + alias + "_it = args->start;\n";
		threadingFunction += "\tuint32_t " + alias + "_fragment_size = args->end;\n";
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
			MetaQuery query, Metadata metadata, List<String> mainCppCode, List<String> functionHeadersCppCode,
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
			if (metadata.indexList.get(i) != null) {
				bufferPoolTrackingArray[i] = new int[metadata.indexList.get(i).getMaxColumnID()+1];
				Arrays.fill(bufferPoolTrackingArray[i], -1);
			}
		}
		
		boolean preThreadingOp = true;
		int threadingFunctionID = -1;
		StringBuilder threadingTabString = new StringBuilder("\t");
		int[] threadingClosingBraces = {1};
		
		for (int i = 0; i<operators.size(); i++) {
			Operator currentOp = operators.get(i);
			int opType = currentOp.type;
			if (opType == Operator.SELECTION_OPERATOR) {
				if (preThreadingOp) {
					mainCppCode.add(evaluateSelection(i, currentOp, tabString, query));
				}
				else {
					
				}
			}
			else if (opType == Operator.JOIN_OPERATOR || opType == Operator.SEMIJOIN_OPERATOR) {
				if (preThreadingOp) {
					mainCppCode.add(evaluateJoin(preThreadingOp, i, operators, currentOp, 
							metadata, tabString, closingBraces, bufferPoolTrackingArray, query, 
							functionHeadersCppCode, functionsCppCode));
				}
				else {
					String temp = functionsCppCode.get(threadingFunctionID);
					temp += evaluateJoin(preThreadingOp, i, operators, currentOp,
							metadata, threadingTabString, threadingClosingBraces, bufferPoolTrackingArray, query,
							functionHeadersCppCode, functionsCppCode);
					functionsCppCode.set(threadingFunctionID, temp);
				}
			}
			else if (opType == Operator.INTERSECTION_OPERATOR) {
				
			}
			else if (opType == Operator.AGGREGATION_OPERATOR) {
				if (preThreadingOp) {
					mainCppCode.add(evaluateAggregation(preThreadingOp, i, operators, currentOp, metadata, tabString, 
							closingBraces, bufferPoolTrackingArray, query));
				}
				else {
					String temp = functionsCppCode.get(threadingFunctionID);
					temp += evaluateAggregation(preThreadingOp, i, operators, currentOp, metadata, threadingTabString, 
							threadingClosingBraces, bufferPoolTrackingArray, query);
					functionsCppCode.set(threadingFunctionID, temp);
				}
			}
			else if (opType == Operator.THREADING_OPERATOR) {
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
			List<Operator> operators, Metadata metadata) {
		
		for (Operator currentOp : operators) {

			
			
			if (currentOp.type == Operator.JOIN_OPERATOR || currentOp.type == Operator.SEMIJOIN_OPERATOR) {
				
				int aliasID;
				int indexID;
				
				if (currentOp.type == Operator.JOIN_OPERATOR) {
					JoinOperator tempOp = (JoinOperator) currentOp;
					aliasID = tempOp.alias;
					indexID = tempOp.indexID;
				}
				else {
					SemiJoinOperator tempOp = (SemiJoinOperator) currentOp;
					aliasID = tempOp.alias;
					indexID = tempOp.indexID;
				}

				boolean found = false;
				for (MetaIndex currIndex : metadata.indexList) {
					if (currIndex.indexID == indexID) {
						int numColumns = currIndex.numColumns;
						query.initBufferPoolArray(aliasID, numColumns);
						found = true;
						break;
					}
				}
				
				if (!found) {
					System.err.println("Error! IndexID match not found in initQueryBufferPool function");
				}
				
			}
		}

	}



	public static void generateCode(List<Operator> operators, Metadata metadata) {
		
		// The last operator should be the aggregate operator
		AggregationOperator aggregation = (AggregationOperator) operators.get(operators.size() - 1); 
		
		int queryID = metadata.currentQueryID;
		MetaQuery query = metadata.queryList.get(queryID);
		
		initQueryBufferPool(query, operators, metadata);
		
		// Initialize Code Segments Strings
		String initCppCode = initialImportsAndConstants(query);
		List<String> globalsCppCode = new ArrayList<String>();
		List<String> functionHeadersCppCode = new ArrayList<String>();
		List<String> functionsCppCode = new ArrayList<String>();
		List<String> mainCppCode = new ArrayList<String>();

		/*** Implementation ***/

		initialDeclarations(globalsCppCode, aggregation, query);
		// Opening Line
		mainCppCode.add(openingLine(query, aggregation));
	
		String benchmarkingString = "\n\tbenchmark_t1 = chrono::steady_clock::now();\n";
		
		mainCppCode.add(benchmarkingString);
		
		// Array initializations
		mainCppCode.add(bufferInitCode(metadata));
	
		mainCppCode.add(initResultArray(aggregation));
		mainCppCode.add(initSemiJoinArray(operators, metadata, globalsCppCode));
		
		// Initializations for BCA and Huffman Decodes
		initDecodeVars(operators, mainCppCode, globalsCppCode, metadata, query);
		
		// Operator evaluation
	
		evaluateOperators(operators, query, metadata, mainCppCode, functionHeadersCppCode, functionsCppCode);
		mainCppCode.add(bufferDeallocation(metadata));
		
		
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
		
		
		writeToFile(fullCppCode, query.queryName);
	}
	






	private static void writeToFile(String fullCppCode, String queryName) {

		try(  PrintWriter out = new PrintWriter(new File("./src/gqfast/loader/" + queryName + ".cpp"))){
		    out.println(fullCppCode);
		} catch (FileNotFoundException e) {
			System.err.println("Error in writeToFile: FileNotFoundException");
			e.printStackTrace();
		}
	
	}








	
}
