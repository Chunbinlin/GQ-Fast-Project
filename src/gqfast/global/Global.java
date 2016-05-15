package gqfast.global;


public class Global {
	public enum Encodings { Uncompress_Array, Uncomress_Bitmap, Bit_Compress_Array, Byte_Align_Bitmap, Huffman };
	public enum Optypes {JOIN_OPERATOR, SEMIJOIN_OPERATOR, INTERSECTION_OPERATOR, AGGREGATION_OPERATOR,SELECTION_OPERATOR, THREADING_OPERATOR, RENAME_OPERATOR, PROJECTION_OPERATOR}; 
	
	
//	public static final int JOIN_OPERATOR = 1;
//	public static final int SEMIJOIN_OPERATOR = 2;
//	public static final int INTERSECTION_OPERATOR = 3;
//	public static final int AGGREGATION_OPERATOR = 4;
//	public static final int SELECTION_OPERATOR = 5;
//	public static final int THREADING_OPERATOR = 6;
//	
	public enum Conditions {greater_than, greater_eq, eq, less_than, less_eq};
}
