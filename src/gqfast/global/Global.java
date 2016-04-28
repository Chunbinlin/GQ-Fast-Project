package gqfast.global;


public class Global {
	public enum Encodings { Uncompress_Array, Uncomress_Bitmap, Bit_Compress_Array, Byte_Align_Bitmap, Huffman };
	public enum Optypes {Join, Semi_Join, Rename, Selection, Projection, Aggregate, Intersect}; 
	public enum Conditions {greater_than, greater_eq, eq, less_than, less_eq};
}
