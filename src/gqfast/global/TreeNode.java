package gqfast.global;

import gqfast.global.Global.Optypes;
import java.util.List;

public class TreeNode {
	private Optypes op;
	private List<Property> prop;
	private List<Term> terms;
	private List<String> aggr;
	private List<Term> aggr_term;
	
	public TreeNode left;
	public TreeNode right;
	
	public TreeNode(Optypes op, List<Property> p){//Selection, Join, Semijoin, Intersect, Rename
		this.op = op;
		this.prop = p;
		this.left = null;
		this.right = null;
		
		this.terms = null;
		this.aggr = null;
		this.aggr_term = null;
	}

	public TreeNode(List<Term> t){//Projection
		this.op = Optypes.Projection;
		this.terms = t;
		this.left = null;
		this.right = null;
		
		this.prop = null;
		this.aggr = null;
		this.aggr_term = null;
	}

	public TreeNode(List<Term> t, List<Term> list_t, List<String> s){//Aggregate
		this.op = Optypes.Aggregate;
		this.aggr_term = t;
		this.terms = list_t;
		this.aggr = s;
		this.left = null;
		this.right = null;
		
		this.prop = null;
	}

	public void print(){
		System.out.print("("+this.op+" ");
		if (this.op == Optypes.Aggregate){
			this.aggr_term.get(0).print();
			System.out.print("; ");
			for (int i = 0; i < this.aggr.size()-1; i++){
				System.out.print(this.aggr.get(i)+",");
			}
			System.out.print(this.aggr.get(this.aggr.size()-1));
		}else if (this.op == Optypes.Projection){
			for (int i = 0; i < this.terms.size()-1; i++){
				this.terms.get(i).print();
				System.out.print(",");
			}
			this.terms.get(this.terms.size()-1).print();
		}else{
			for (int i = 0; i < this.prop.size()-1; i++){
				this.prop.get(i).print();
				System.out.print(",");
			}
			this.prop.get(this.prop.size()-1).print();
		}
		System.out.print(")");
	}
	
	public Optypes get_Optype() {
		return this.op;
	}
	
	public List<Property> get_Property() {
		return this.prop;
	}
	
	public List<Term> get_TermList() {
		return this.terms;
	}
	
	public List<String> get_AggrList() {
		return this.aggr;
	}
	
	public List<Term> get_AggrTerm() {
		return this.aggr_term;
	}
		
}

/* Selection: op, prop(term1(var,col), cond, term2(const));
 * Join: op, prop(term1(var,col), cond=Eq, term2(var,col));
 * Semijoin: op, prop(term1(var,col), cond=Eq, term2(var,col));
 * Intersect: op, prop(term1(var,col), cond=Eq, term2(var,col));
 * Rename: op, prop(term1(var,col=""), cond=Eq, term2(var,col=""));
 
 * Projection: op, list(terms);
 
 * Aggregate: op, term, list(terms), list(String);
  * */
