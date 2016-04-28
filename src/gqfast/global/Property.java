package gqfast.global;

import gqfast.global.Global.Conditions;

public class Property {
	private Term term1;
	private Conditions cond;
	private Term term2;
	
	public Property(Term t1, Conditions condition, Term t2) {
		this.term1 = t1;
		this.cond = condition;
		this.term2 = t2;
	}
	
	public Term getTerm1(){
		return this.term1;
	}
	
	public Term getTerm2(){
		return this.term2;
	}
	
	public Conditions getCond(){
		return this.cond;
	}
	
	public void print(){
		this.term1.print();
		System.out.print(" "+this.cond.toString()+" ");
		this.term2.print();
	}
}
