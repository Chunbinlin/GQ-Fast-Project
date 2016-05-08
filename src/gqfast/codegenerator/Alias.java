package codegenerator;

public class Alias {

	private int aliasID;
	private String alias;
	private MetaIndex associatedIndex;
	
	public Alias(int aliasID, String alias, MetaIndex associatedIndex) {
		this.aliasID = aliasID;
		this.alias = alias;
		this.associatedIndex = associatedIndex;
	}
	
	public Alias(int aliasID, String alias) {
		this.aliasID = aliasID;
		this.alias = alias;
		associatedIndex = null;
	}
	
	public int getAliasID() {
		return aliasID;
	}
	


	public String getAlias() {
		return alias;
	}

	public MetaIndex getAssociatedIndex() {
		return associatedIndex;
	}


	

}
