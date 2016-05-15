package gqfast.unitTest;

import gqfast.global.Global.Optypes;
import gqfast.global.Global.Conditions;
import gqfast.global.TreeNode;
import gqfast.global.Property;
import gqfast.global.Term;

import java.util.ArrayList;
import java.util.List;

/* Selection: op, list(prop(term1(var,col), cond, term2(const)));
 * Join: op, list(prop(term1(var,col), cond=Eq, term2(var,col)));
 * Semijoin: op, list(prop(term1(var,col), cond=Eq, term2(var,col)));
 * Intersect: op, list(prop(term1(var,col), cond=Eq, term2(var,col)));
 * Rename: op, list(prop(term1(var,col=""), cond=Eq, term2(var,col="")));

 * Projection: op, list(terms);

 * Aggregate: op, term, list(terms), list(String);
 * */

public class TestTree_logical2RQNA {
	private TreeNode node;

	public TestTree_logical2RQNA() {
		this.node = null;
	}

	public void Tree1() {//Nothing need to be changed
		Term t1 = new Term("d", "Year");
		Term t2 = new Term("2012");
		Property p1 = new Property(t1, Conditions.eq, t2);
		List<Property> l1 = new ArrayList<Property>();
		l1.add(p1);

		Term t3 = new Term("d", "");
		Term t4 = new Term("Document", "");
		Property p2 = new Property(t3, Conditions.eq, t4);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);

		TreeNode root = new TreeNode(Optypes.SELECTION_OPERATOR, l1);
		root.left = new TreeNode(Optypes.RENAME_OPERATOR, l2);
		this.node = root;
	}

	public void Tree2() {//need to add one projection
		Term t1 = new Term("dt", "Doc");
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		Term t2 = new Term("dt", "Fre");
		String s1 = "AVG(dt.Fre)";
		List<Term> l1 = new ArrayList<Term>();
		l1.add(t2);
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);

		Term t3 = new Term("dt", "Term");
		Term t4 = new Term("t");
		Property p2 = new Property(t3, Conditions.eq, t4);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);

		Term t5 = new Term("dt", "");
		Term t6 = new Term("DT", "");
		Property p3 = new Property(t5, Conditions.eq, t6);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);

		TreeNode root = new TreeNode(l0, l1, l1s);
		root.left = new TreeNode(Optypes.SELECTION_OPERATOR, l2);
		root.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l3);
		this.node = root;
	}

	public void Tree3() {//need to push one selection with one join
		Term t1 = new Term("da", "Author");
		String s1 = "Count(*)";
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		List<Term> l1 = new ArrayList<Term>();
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);
		TreeNode root = new TreeNode(l0, l1, l1s);

		Term t3 = new Term("dt", "Term");
		Term t4 = new Term("t");
		Property p2 = new Property(t3, Conditions.eq, t4);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);
		root.left = new TreeNode(Optypes.SELECTION_OPERATOR, l2);

		Term t5 = new Term("dt", "Doc");
		Term t6 = new Term("da", "Doc");
		Property p3 = new Property(t5, Conditions.eq, t6);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);
		root.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l3);
		
		Term t7 = new Term("dt", "");
		Term t8 = new Term("DT", "");
		Property p4 = new Property(t7, Conditions.eq, t8);
		List<Property> l4 = new ArrayList<Property>();
		l4.add(p4);
		root.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l4);

		Term t9 = new Term("da", "");
		Term t10 = new Term("DA", "");
		Property p5 = new Property(t9, Conditions.eq, t10);
		List<Property> l5 = new ArrayList<Property>();
		l5.add(p5);
		root.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l5);

		this.node = root;
	}

	public void Tree4() {//need to add one projection
		Term t1 = new Term("d", "Year");
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		String s1 = "Count(*)";
		List<Term> l1 = new ArrayList<Term>();
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);
		TreeNode root = new TreeNode(l0, l1, l1s);

		Term t3 = new Term("dt", "Doc");
		Term t4 = new Term("d","ID");
		Property p2 = new Property(t3, Conditions.eq, t4);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);
		root.left = new TreeNode(Optypes.SEMIJOIN_OPERATOR, l2);

		Term t5 = new Term("d", "");
		Term t6 = new Term("Document", "");
		Property p3 = new Property(t5, Conditions.eq, t6);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);
		root.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l3);

		Term t52 = new Term("dt", "Doc");
		Term t62 = new Term("dt", "Doc");
		Property p32 = new Property(t52, Conditions.eq, t62);
		List<Property> l32 = new ArrayList<Property>();
		l32.add(p32);
		root.left.left = new TreeNode(Optypes.INTERSECTION_OPERATOR, l32);

		Term t7 = new Term("dt", "Doc");
		List<Term> l4 = new ArrayList<Term>();
		l4.add(t7);
		root.left.left.right = new TreeNode(l4);
		Term t81 = new Term("dt", "Term");
		Term t82 = new Term("t1");
		Property p42 = new Property(t81, Conditions.eq, t82);
		List<Property> l42 = new ArrayList<Property>();
		l42.add(p42);
		root.left.left.right.left = new TreeNode(Optypes.SELECTION_OPERATOR,l42);
		Term t9 = new Term("dt", "");
		Term t10 = new Term("DT", "");
		Property p5 = new Property(t9, Conditions.eq, t10);
		List<Property> l5 = new ArrayList<Property>();
		l5.add(p5);
		root.left.left.right.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l5);
		
		Term t72 = new Term("dt", "Doc");
		List<Term> l43 = new ArrayList<Term>();
		l43.add(t72);
		root.left.left.left = new TreeNode(l43);
		Term t812 = new Term("dt", "Term");
		Term t822 = new Term("t2");
		Property p422 = new Property(t812, Conditions.eq, t822);
		List<Property> l422 = new ArrayList<Property>();
		l422.add(p422);
		root.left.left.left.left = new TreeNode(Optypes.SELECTION_OPERATOR,l422);
		Term t92 = new Term("dt", "");
		Term t102 = new Term("DT", "");
		Property p52 = new Property(t92, Conditions.eq, t102);
		List<Property> l52 = new ArrayList<Property>();
		l52.add(p52);
		root.left.left.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l52);

		this.node = root;
	}
	
	public void Tree5() {//need to push one selection with two joins
		Term t1 = new Term("da", "Author");
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		String s1 = "COUNT(*)";
		List<Term> l1 = new ArrayList<Term>();
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);

		Term t3 = new Term("dt1", "Doc");
		Term t4 = new Term("d");
		Property p2 = new Property(t3, Conditions.eq, t4);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);

		Term t5 = new Term("dt2", "Doc");
		Term t6 = new Term("da", "Doc");
		Property p3 = new Property(t5, Conditions.eq, t6);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);

		Term t7 = new Term("dt1", "Term");
		Term t8 = new Term("dt2", "Term");
		Property p4 = new Property(t7, Conditions.eq, t8);
		List<Property> l4 = new ArrayList<Property>();
		l4.add(p4);

		Term t91 = new Term("dt1", "");
		Term t101 = new Term("DT", "");
		Property p51 = new Property(t91, Conditions.eq, t101);
		List<Property> l51 = new ArrayList<Property>();
		l51.add(p51);

		Term t92 = new Term("dt2", "");
		Term t102 = new Term("DT", "");
		Property p52 = new Property(t92, Conditions.eq, t102);
		List<Property> l52 = new ArrayList<Property>();
		l52.add(p52);

		Term t93 = new Term("da", "");
		Term t103 = new Term("DA", "");
		Property p53 = new Property(t93, Conditions.eq, t103);
		List<Property> l53 = new ArrayList<Property>();
		l53.add(p53);

		TreeNode root = new TreeNode(l0, l1, l1s);
		root.left = new TreeNode(Optypes.SELECTION_OPERATOR, l2);
		root.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l3);
		root.left.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l4);
		root.left.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l51);
		root.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l52);
		root.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l53);
		this.node = root;
	}
	
	public void Tree6() {//need to push one semijoin
		Term t1 = new Term("sp", "SID");
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		String s1 = "Count(*)";
		List<Term> l1 = new ArrayList<Term>();
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);
		TreeNode root = new TreeNode(l0, l1, l1s);

		Term t3 = new Term("pa", "CSID");
		Term t4 = new Term("cs","CSID");
		Property p2 = new Property(t3, Conditions.eq, t4);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);
		root.left = new TreeNode(Optypes.SEMIJOIN_OPERATOR, l2);

		Term t5 = new Term("pa", "PID");
		Term t6 = new Term("sp", "PID");
		Property p3 = new Property(t5, Conditions.eq, t6);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);
		root.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l3);

		Term t55 = new Term("pa", "");
		Term t65 = new Term("PA", "");
		Property p35 = new Property(t55, Conditions.eq, t65);
		List<Property> l35 = new ArrayList<Property>();
		l35.add(p35);
		root.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l35);

		Term t56 = new Term("sp", "");
		Term t66 = new Term("SP", "");
		Property p36 = new Property(t56, Conditions.eq, t66);
		List<Property> l36 = new ArrayList<Property>();
		l36.add(p36);
		root.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l36);

		Term t52 = new Term("cs", "CSID");
		Term t62 = new Term("cs", "CSID");
		Property p32 = new Property(t52, Conditions.eq, t62);
		List<Property> l32 = new ArrayList<Property>();
		l32.add(p32);
		root.left.right = new TreeNode(Optypes.INTERSECTION_OPERATOR, l32);

		Term t7 = new Term("cs", "CSID");
		List<Term> l4 = new ArrayList<Term>();
		l4.add(t7);
		root.left.right.right = new TreeNode(l4);
		Term t81 = new Term("cs", "CID");
		Term t82 = new Term("a1");
		Property p42 = new Property(t81, Conditions.eq, t82);
		List<Property> l42 = new ArrayList<Property>();
		l42.add(p42);
		root.left.right.right.left = new TreeNode(Optypes.SELECTION_OPERATOR,l42);
		Term t9 = new Term("cs", "");
		Term t10 = new Term("CS", "");
		Property p5 = new Property(t9, Conditions.eq, t10);
		List<Property> l5 = new ArrayList<Property>();
		l5.add(p5);
		root.left.right.right.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l5);
		
		Term t72 = new Term("cs", "CSID");
		List<Term> l43 = new ArrayList<Term>();
		l43.add(t72);
		root.left.right.left = new TreeNode(l43);
		Term t812 = new Term("cs", "CID");
		Term t822 = new Term("a2");
		Property p422 = new Property(t812, Conditions.eq, t822);
		List<Property> l422 = new ArrayList<Property>();
		l422.add(p422);
		root.left.right.left.left = new TreeNode(Optypes.SELECTION_OPERATOR,l422);
		Term t92 = new Term("cs", "");
		Term t102 = new Term("CS", "");
		Property p52 = new Property(t92, Conditions.eq, t102);
		List<Property> l52 = new ArrayList<Property>();
		l52.add(p52);
		root.left.right.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l52);

		this.node = root;
	}

	public void TreeSD() {
		Term t1 = new Term("dt2", "Doc");
		String s1 = "Count(*)";
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		List<Term> l1 = new ArrayList<Term>();
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);
		TreeNode root = new TreeNode(l0, l1, l1s);

		Term t3 = new Term("dt1", "Doc");
		Term t4 = new Term("d0");
		Property p2 = new Property(t3, Conditions.eq, t4);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);
		root.left = new TreeNode(Optypes.SELECTION_OPERATOR, l2);

		Term t5 = new Term("dt1", "Term");
		Term t6 = new Term("dt2", "Term");
		Property p3 = new Property(t5, Conditions.eq, t6);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);
		root.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l3);
		
		Term t7 = new Term("dt1", "");
		Term t8 = new Term("DT", "");
		Property p4 = new Property(t7, Conditions.eq, t8);
		List<Property> l4 = new ArrayList<Property>();
		l4.add(p4);
		root.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l4);

		Term t9 = new Term("dt2", "");
		Term t10 = new Term("DT", "");
		Property p5 = new Property(t9, Conditions.eq, t10);
		List<Property> l5 = new ArrayList<Property>();
		l5.add(p5);
		root.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l5);

		this.node = root;
	}

	public void TreeFSD() {
		Term t1 = new Term("dt2", "Doc");
		String s1 = "SUM(dt1.Fre*dt2.Fre)/(|d1.Year-d2.Year|+1)";
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		Term t21 = new Term("dt1","Fre");
		Term t22 = new Term("dt2","Fre");
		Term t23 = new Term("d1","Year");
		Term t24 = new Term("d2","Year");
		List<Term> l1 = new ArrayList<Term>();
		l1.add(t21); l1.add(t22); l1.add(t23); l1.add(t24);
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);
		TreeNode root = new TreeNode(l0, l1, l1s);
		
		Term t31 = new Term("d1", "ID");
		Term t32 = new Term("d0");
		Property p2 = new Property(t31, Conditions.eq, t32);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);
		root.left = new TreeNode(Optypes.SELECTION_OPERATOR, l2);

		Term t41 = new Term("dt2", "Doc");
		Term t42 = new Term("d2", "ID");
		Property p3 = new Property(t41, Conditions.eq, t42);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);
		root.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l3);

		Term t51 = new Term("dt1", "Term");
		Term t52 = new Term("dt2", "Term");
		Property p4 = new Property(t51, Conditions.eq, t52);
		List<Property> l4 = new ArrayList<Property>();
		l4.add(p4);
		root.left.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l4);

		Term t71 = new Term("d1", "ID");
		Term t72 = new Term("dt1", "Doc");
		Property p6 = new Property(t71, Conditions.eq, t72);
		List<Property> l6 = new ArrayList<Property>();
		l6.add(p6);
		root.left.left.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l6);

		Term t61 = new Term("d2", "");
		Term t62 = new Term("Document", "");
		Property p5 = new Property(t61, Conditions.eq, t62);
		List<Property> l5 = new ArrayList<Property>();
		l5.add(p5);
		root.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l5);

		Term t81 = new Term("dt2", "");
		Term t82 = new Term("DT", "");
		Property p8 = new Property(t81, Conditions.eq, t82);
		List<Property> l8 = new ArrayList<Property>();
		l8.add(p8);
		root.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l8);

		Term t91 = new Term("dt1", "");
		Term t92 = new Term("DT", "");
		Property p9 = new Property(t91, Conditions.eq, t92);
		List<Property> l9 = new ArrayList<Property>();
		l9.add(p9);
		root.left.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l9);

		Term t01 = new Term("d1", "");
		Term t02 = new Term("Document", "");
		Property p10 = new Property(t01, Conditions.eq, t02);
		List<Property> l10 = new ArrayList<Property>();
		l10.add(p10);
		root.left.left.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l10);

		this.node = root;		
	}
	
	public void TreeAD() {//need to add one projection
		Term t1 = new Term("da", "Author");
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		String s1 = "Count(*)";
		List<Term> l1 = new ArrayList<Term>();
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);
		TreeNode root = new TreeNode(l0, l1, l1s);

		Term t3 = new Term("dt", "Doc");
		Term t4 = new Term("da","Doc");
		Property p2 = new Property(t3, Conditions.eq, t4);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);
		root.left = new TreeNode(Optypes.SEMIJOIN_OPERATOR, l2);

		Term t5 = new Term("da", "");
		Term t6 = new Term("DA", "");
		Property p3 = new Property(t5, Conditions.eq, t6);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);
		root.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l3);

		Term t52 = new Term("dt", "Doc");
		Term t62 = new Term("dt", "Doc");
		Property p32 = new Property(t52, Conditions.eq, t62);
		List<Property> l32 = new ArrayList<Property>();
		l32.add(p32);
		root.left.left = new TreeNode(Optypes.INTERSECTION_OPERATOR, l32);

		Term t7 = new Term("dt", "Doc");
		List<Term> l4 = new ArrayList<Term>();
		l4.add(t7);
		root.left.left.right = new TreeNode(l4);
		Term t81 = new Term("dt", "Term");
		Term t82 = new Term("t1");
		Property p42 = new Property(t81, Conditions.eq, t82);
		List<Property> l42 = new ArrayList<Property>();
		l42.add(p42);
		root.left.left.right.left = new TreeNode(Optypes.SELECTION_OPERATOR,l42);
		Term t9 = new Term("dt", "");
		Term t10 = new Term("DT", "");
		Property p5 = new Property(t9, Conditions.eq, t10);
		List<Property> l5 = new ArrayList<Property>();
		l5.add(p5);
		root.left.left.right.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l5);
		
		Term t72 = new Term("dt", "Doc");
		List<Term> l43 = new ArrayList<Term>();
		l43.add(t72);
		root.left.left.left = new TreeNode(l43);
		Term t812 = new Term("dt", "Term");
		Term t822 = new Term("t2");
		Property p422 = new Property(t812, Conditions.eq, t822);
		List<Property> l422 = new ArrayList<Property>();
		l422.add(p422);
		root.left.left.left.left = new TreeNode(Optypes.SELECTION_OPERATOR,l422);
		Term t92 = new Term("dt", "");
		Term t102 = new Term("DT", "");
		Property p52 = new Property(t92, Conditions.eq, t102);
		List<Property> l52 = new ArrayList<Property>();
		l52.add(p52);
		root.left.left.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l52);

		this.node = root;
	}
	
	public void TreeAS() {
		Term t1 = new Term("da2", "Author");
		String s1 = "SUM(dt1.Fre*dt2.Fre)/(2017-d.Year)";
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		Term t21 = new Term("dt1","Fre");
		Term t22 = new Term("dt2","Fre");
		Term t23 = new Term("d","Year");
		List<Term> l1 = new ArrayList<Term>();
		l1.add(t21); l1.add(t22); l1.add(t23);
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);
		TreeNode root = new TreeNode(l0, l1, l1s);
		
		Term t31 = new Term("da1", "Author");
		Term t32 = new Term("a");
		Property p2 = new Property(t31, Conditions.eq, t32);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);
		root.left = new TreeNode(Optypes.SELECTION_OPERATOR, l2);

		Term t41 = new Term("d", "Doc");
		Term t42 = new Term("da2", "Doc");
		Property p3 = new Property(t41, Conditions.eq, t42);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);
		root.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l3);

		Term t51 = new Term("dt2", "Doc");
		Term t52 = new Term("d", "ID");
		Property p4 = new Property(t51, Conditions.eq, t52);
		List<Property> l4 = new ArrayList<Property>();
		l4.add(p4);
		root.left.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l4);

		Term t71 = new Term("dt1", "Term");
		Term t72 = new Term("dt2", "Term");
		Property p6 = new Property(t71, Conditions.eq, t72);
		List<Property> l6 = new ArrayList<Property>();
		l6.add(p6);
		root.left.left.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l6);

		Term t712 = new Term("da1", "Doc");
		Term t722 = new Term("dt1", "Doc");
		Property p62 = new Property(t712, Conditions.eq, t722);
		List<Property> l62 = new ArrayList<Property>();
		l62.add(p62);
		root.left.left.left.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l62);

		Term t61 = new Term("da2", "");
		Term t62 = new Term("DA", "");
		Property p5 = new Property(t61, Conditions.eq, t62);
		List<Property> l5 = new ArrayList<Property>();
		l5.add(p5);
		root.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l5);

		Term t81 = new Term("d", "");
		Term t82 = new Term("Document", "");
		Property p8 = new Property(t81, Conditions.eq, t82);
		List<Property> l8 = new ArrayList<Property>();
		l8.add(p8);
		root.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l8);

		Term t91 = new Term("dt2", "");
		Term t92 = new Term("DT", "");
		Property p9 = new Property(t91, Conditions.eq, t92);
		List<Property> l9 = new ArrayList<Property>();
		l9.add(p9);
		root.left.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l9);

		Term t01 = new Term("dt1", "");
		Term t02 = new Term("DT", "");
		Property p10 = new Property(t01, Conditions.eq, t02);
		List<Property> l10 = new ArrayList<Property>();
		l10.add(p10);
		root.left.left.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l10);

		Term t012 = new Term("da1", "");
		Term t022 = new Term("DA", "");
		Property p102 = new Property(t012, Conditions.eq, t022);
		List<Property> l102 = new ArrayList<Property>();
		l102.add(p102);
		root.left.left.left.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l102);
		
		this.node = root;
	}
	
	public void TreeCS() {
		Term t1 = new Term("c2", "CID");
		String s1 = "COUNT(*)";
		List<Term> l0 = new ArrayList<Term>();
		l0.add(t1);
		List<Term> l1 = new ArrayList<Term>();
		List<String> l1s = new ArrayList<String>();
		l1s.add(s1);
		TreeNode root = new TreeNode(l0, l1, l1s);
		
		Term t31 = new Term("c1", "CID");
		Term t32 = new Term("c");
		Property p2 = new Property(t31, Conditions.eq, t32);
		List<Property> l2 = new ArrayList<Property>();
		l2.add(p2);
		root.left = new TreeNode(Optypes.SELECTION_OPERATOR, l2);

		Term t41 = new Term("p2", "CSID");
		Term t42 = new Term("c2", "CSID");
		Property p3 = new Property(t41, Conditions.eq, t42);
		List<Property> l3 = new ArrayList<Property>();
		l3.add(p3);
		root.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l3);

		Term t51 = new Term("s2", "PID");
		Term t52 = new Term("p2", "PID");
		Property p4 = new Property(t51, Conditions.eq, t52);
		List<Property> l4 = new ArrayList<Property>();
		l4.add(p4);
		root.left.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l4);

		Term t71 = new Term("s1", "SID");
		Term t72 = new Term("s2", "SID");
		Property p6 = new Property(t71, Conditions.eq, t72);
		List<Property> l6 = new ArrayList<Property>();
		l6.add(p6);
		root.left.left.left.left = new TreeNode(Optypes.SEMIJOIN_OPERATOR, l6);

		Term t712 = new Term("p1", "PID");
		Term t722 = new Term("s1", "PID");
		Property p62 = new Property(t712, Conditions.eq, t722);
		List<Property> l62 = new ArrayList<Property>();
		l62.add(p62);
		root.left.left.left.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l62);

		Term t713 = new Term("c1", "CSID");
		Term t723 = new Term("p1", "CSID");
		Property p63 = new Property(t713, Conditions.eq, t723);
		List<Property> l63 = new ArrayList<Property>();
		l63.add(p63);
		root.left.left.left.left.left.left = new TreeNode(Optypes.JOIN_OPERATOR, l63);

		Term t61 = new Term("c2", "");
		Term t62 = new Term("CS", "");
		Property p5 = new Property(t61, Conditions.eq, t62);
		List<Property> l5 = new ArrayList<Property>();
		l5.add(p5);
		root.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l5);

		Term t81 = new Term("p2", "");
		Term t82 = new Term("PA", "");
		Property p8 = new Property(t81, Conditions.eq, t82);
		List<Property> l8 = new ArrayList<Property>();
		l8.add(p8);
		root.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l8);

		Term t91 = new Term("s2", "");
		Term t92 = new Term("SP", "");
		Property p9 = new Property(t91, Conditions.eq, t92);
		List<Property> l9 = new ArrayList<Property>();
		l9.add(p9);
		root.left.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l9);

		Term t01 = new Term("s1", "");
		Term t02 = new Term("SP", "");
		Property p10 = new Property(t01, Conditions.eq, t02);
		List<Property> l10 = new ArrayList<Property>();
		l10.add(p10);
		root.left.left.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l10);

		Term t012 = new Term("p1", "");
		Term t022 = new Term("PA", "");
		Property p102 = new Property(t012, Conditions.eq, t022);
		List<Property> l102 = new ArrayList<Property>();
		l102.add(p102);
		root.left.left.left.left.left.left.right = new TreeNode(Optypes.RENAME_OPERATOR, l102);
		
		Term t013 = new Term("c1", "");
		Term t023 = new Term("CS", "");
		Property p103 = new Property(t013, Conditions.eq, t023);
		List<Property> l103 = new ArrayList<Property>();
		l103.add(p103);
		root.left.left.left.left.left.left.left = new TreeNode(Optypes.RENAME_OPERATOR, l103);

		this.node = root;
	}
	
	public void print(TreeNode t) {
		if (t != null) {
			if (t.get_Optype() == Optypes.JOIN_OPERATOR || t.get_Optype() == Optypes.SEMIJOIN_OPERATOR || t.get_Optype() == Optypes.INTERSECTION_OPERATOR){
			System.out.print("\n{(");
			print(t.left);
			System.out.print(")\n");
			t.print();
			System.out.print("\n(");
			print(t.right);
			System.out.print(")}\n");
			}else{
				t.print();
				print(t.left);
				print(t.right);
			}
		}
	}

	public TreeNode getroot() {
		return this.node;
	}

}
