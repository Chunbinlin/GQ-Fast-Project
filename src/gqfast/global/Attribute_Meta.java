package gqfast.global;
import gqfast.global.Global.Encodings;

public class Attribute_Meta {
	private String column_name;
	private boolean is_key_column;//True: key/foreign key;  False: Measure
	private Encodings encoding;
	
	public boolean checkKey(String table, String column) {
		//TODO: primary key and foreign key
		return is_key_column;
	}

}
