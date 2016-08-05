package gqfast.RQNA2physical;
//1-to-1 mapping
public class MetaIndexDisk
{
	private String table_name;
	private String lookup_col_name;
	private int num_encodings; 
	private String[] column_names; //size is num_encodings+1
	private int[] column_domains;
	private int[] column_mins;
	private int[] column_byte_sizes;
	private int[] column_encoding_flags;
	private int max_fragment_size;
	public String getTable_name()
	{
		return table_name;
	}
	public void setTable_name(String table_name)
	{
		this.table_name = table_name;
	}
	public String getLookup_col_name()
	{
		return lookup_col_name;
	}
	public void setLookup_col_name(String lookup_col_name)
	{
		this.lookup_col_name = lookup_col_name;
	}
	public int getNum_encodings()
	{
		return num_encodings;
	}
	public void setNum_encodings(int num_encodings)
	{
		this.num_encodings = num_encodings;
	}
	public String[] getColumn_names()
	{
		return column_names;
	}
	public void setColumn_names(String[] column_names)
	{
		this.column_names = column_names;
	}
	public int[] getColumn_domains()
	{
		return column_domains;
	}
	public void setColumn_domains(int[] column_domains)
	{
		this.column_domains = column_domains;
	}
	public int[] getColumn_mins()
	{
		return column_mins;
	}
	public void setColumn_mins(int[] column_mins)
	{
		this.column_mins = column_mins;
	}
	public int[] getColumn_byte_sizes()
	{
		return column_byte_sizes;
	}
	public void setColumn_byte_sizes(int[] column_byte_sizes)
	{
		this.column_byte_sizes = column_byte_sizes;
	}
	public int[] getColumn_encoding_flags()
	{
		return column_encoding_flags;
	}
	public void setColumn_encoding_flags(int[] column_encoding_flags)
	{
		this.column_encoding_flags = column_encoding_flags;
	}
	public int getMax_fragment_size()
	{
		return max_fragment_size;
	}
	public void setMax_fragment_size(int max_fragment_size)
	{
		this.max_fragment_size = max_fragment_size;
	}
}
