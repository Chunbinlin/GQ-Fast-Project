package gqfast.global;

import gqfast.global.Global.Encodings;

public class IndexMetaMessage {
    
    private String pathAndFilename;
    private int numEncodedCols;
    private String[] encodedColNames;
    private Encodings[] encodedColEncodings;
	public IndexMetaMessage(String pathAndFilename, int numEncodedCols,
			String[] encodedColNames, Encodings[] encodedColEncodings) {
		super();
		this.pathAndFilename = pathAndFilename;
		this.numEncodedCols = numEncodedCols;
		this.encodedColNames = encodedColNames;
		this.encodedColEncodings = encodedColEncodings;
	}
	
	public String getPathAndFilename() {
		return pathAndFilename;
	}
	public void setPathAndFilename(String pathAndFilename) {
		this.pathAndFilename = pathAndFilename;
	}
	public int getNumEncodedCols() {
		return numEncodedCols;
	}
	public void setNumEncodedCols(int numEncodedCols) {
		this.numEncodedCols = numEncodedCols;
	}
	public String[] getEncodedColNames() {
		return encodedColNames;
	}
	public void setEncodedColNames(String[] encodedColNames) {
		this.encodedColNames = encodedColNames;
	}
	public Encodings[] getEncodedColEncodings() {
		return encodedColEncodings;
	}
	public void setEncodedColEncodings(Encodings[] encodedColEncodings) {
		this.encodedColEncodings = encodedColEncodings;
	}
    
    
}
