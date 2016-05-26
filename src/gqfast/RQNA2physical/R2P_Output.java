package gqfast.RQNA2physical;

import gqfast.codeGenerator.Operator;
import gqfast.global.MetaData;

import java.util.List;

public class R2P_Output
{
	private MetaData metaData;
	private List<Operator> operators;
	
	public MetaData getMetaData()
	{
		return metaData;
	}
	public void setMetaData(MetaData metaData)
	{
		this.metaData = metaData;
	}
	public List<Operator> getOperators()
	{
		return operators;
	}
	public void setOperators(List<Operator> operators)
	{
		this.operators = operators;
	}
}
