package gqfast.logical2RQNA;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import gqfast.global.Global.Optypes;
import gqfast.global.Global.Conditions;
//import fastr.global.Attribute_Meta;
import gqfast.global.TreeNode;
import gqfast.global.Property;
import gqfast.global.Term;

import java.util.ArrayList;
import java.util.List;

public class RelationalAlgebra2RQNA {
	private TreeNode RA;
	private TreeNode node;
	private TreeNode push_parent;
	private List<TreeNode> toClear;

	// private Attribute_Meta att;

	/*
	 * Selection: op, list(prop(term1(var,col), cond, term2(const))); 
	 * Join: op, list(prop(term1(var,col), cond=Eq, term2(var,col))); 
	 * Semijoin: op, list(prop(term1(var,col), cond=Eq, term2(var,col))); 
	 * Intersect: op, list(prop(term1(var,col), cond=Eq, term2(var,col))); 
	 * Rename: op, list(prop(term1(var,col=""), cond=Eq, term2(var,col="")));
	 * 
	 * Projection: op, list(terms);
	 * 
	 * Aggregate: op, term, list(terms), list(String);
	 * 
	 * 
	 * Checking validation:
	// 1: if group by column number > 1, error message
	// 2: if semijoin condition column number > 1, error message
	// 3: if intersection column number > 1, error message
	// 4: if (semi)join on neither key or foreign key, error message
	// 5: if join condition column number > 1, error message
	 * 
	 * Checking improvement:
	// return 4 to spin semijoins
	// return 3 to separate selections for different tables
	// return 2 to push selections
	// return 1 to push semijoins
	// return 0 to add projections and push projections
	 * 
	 */


	private int check_valid(TreeNode n) {
		if (n == null)
			return 0;
		int errorNo = 0;
		switch (n.get_Optype()) {
		case JOIN_OPERATOR:
			if (n.get_Property().size() > 1)
				errorNo = 5;
			else {
				// String t1 =
				// n.get_Property().get(0).getTerm1().get_variable();
				// String t2 =
				// n.get_Property().get(0).getTerm2().get_variable();
				// String k1 = n.get_Property().get(0).getTerm1().get_column();
				// String k2 = n.get_Property().get(0).getTerm2().get_column();
				// if (!this.att.checkKey(t1, k1) || !this.att.checkKey(t2, k2))
				// errorNo = 4;
			}
			break;
		case SEMIJOIN_OPERATOR:
			if (n.get_Property().size() > 1)
				errorNo = 2;
			else {
				// String t1 =
				// n.get_Property().get(0).getTerm1().get_variable();
				// String t2 =
				// n.get_Property().get(0).getTerm2().get_variable();
				// String k1 = n.get_Property().get(0).getTerm1().get_column();
				// String k2 = n.get_Property().get(0).getTerm2().get_column();
				// if (!this.att.checkKey(t1, k1) || !this.att.checkKey(t2, k2))
				// errorNo = 4;
			}
			break;
		case INTERSECTION_OPERATOR:
			if (n.get_Property().size() > 1)
				errorNo = 3;
			break;
		case AGGREGATION_OPERATOR:
			if (n.get_AggrTerm().size() > 1)
				errorNo = 1;
			break;
		default:
			break;
		}
		if (errorNo == 0 && n.left != null)
			errorNo = check_valid(n.left);
		if (errorNo == 0 && n.right != null)
			errorNo = check_valid(n.right);
		return errorNo;
	}

	private void print_error(int errorNo) {
		if (errorNo == 5)
			System.out.println("Error: join condition column number > 1!");
		else if (errorNo == 4)
			System.out
					.println("Error: (semi)join on neither key or foreign key!");
		else if (errorNo == 3)
			System.out.println("Error: intersection column number > 1!");
		else if (errorNo == 2)
			System.out.println("Error: semijoin condition column number > 1!");
		else if (errorNo == 1)
			System.out.println("Error: group_by column number > 1!");
	}

	private int check_improve_helper(TreeNode n) {
		if (n == null)
			return 0;
		int improveNo = 0;
		switch (n.get_Optype()) {
		case SELECTION_OPERATOR:
			Set<String> tables = new HashSet<String>();
			for (Property i : n.get_Property()) {
				String t = i.getTerm1().get_variable();
				if (!tables.contains(t))
					tables.add(t);
			}
			if (tables.size() > 1)
				improveNo = 3;
			else {
				int flag = 0;
				String t = n.get_Property().get(0).getTerm1().get_variable();
				if (n.left.get_Optype() == Optypes.RENAME_OPERATOR
						&& n.left.get_Property().get(0).getTerm1()
								.get_variable() == t) {
					flag = 1;
				} else if (n.right != null) {
					if (n.right.get_Optype() == Optypes.RENAME_OPERATOR
							&& n.right.get_Property().get(0).getTerm1()
									.get_variable() == t)
						flag = 1;
				}
				if (flag == 0)
					improveNo = 2;
			}
			break;
		case SEMIJOIN_OPERATOR:
			if (n.left != null) {
				if (n.left.get_Optype() == Optypes.JOIN_OPERATOR){
					improveNo = 1;
					if (n.right != null){
						String semiTr = n.get_Property().get(0).getTerm2().get_variable();
						if (n.right.get_Optype() == Optypes.RENAME_OPERATOR || n.right.get_Optype() == Optypes.SELECTION_OPERATOR){
							if (n.right.get_Property().get(0).getTerm1().get_variable() == semiTr)
								improveNo = 0;
						}else if (n.right.get_TermList().get(0).get_variable() == semiTr)
							improveNo = 0;
					}
				}
				if (n.right.get_Optype() == Optypes.JOIN_OPERATOR){
					improveNo = 1;
					if (n.left != null){
						String semiTl = n.get_Property().get(0).getTerm1().get_variable();
						if (n.left.get_Optype() == Optypes.RENAME_OPERATOR || n.left.get_Optype() == Optypes.SELECTION_OPERATOR){
							if (n.left.get_Property().get(0).getTerm1().get_variable() == semiTl)
								improveNo = 0;
						}else if (n.left.get_TermList().get(0).get_variable() == semiTl)
							improveNo = 0;
					}
				}
					
			}
			if (n.right != null) {
				if (n.right.get_Optype() == Optypes.INTERSECTION_OPERATOR)
					improveNo = 4;
			}
			break;
		default:
			break;
		}
		if (improveNo > 0)
			this.node = n;
		if (improveNo == 0)
			improveNo = check_improve_helper(n.left);
		if (improveNo == 0)
			improveNo = check_improve_helper(n.right);
		return improveNo;
	}

	private int check_improve() {
		int result = 0;
		result = check_improve_helper(this.RA);
		return result;
	}

	private void separate_selection() {
		if (this.node == null)
			return;
		System.out.println("separating selection...");
		Set<String> table_Sel = new HashSet<String>();
		for (Property i : this.node.get_Property()) {
			String t = i.getTerm1().get_variable();
			if (!table_Sel.contains(t))
				table_Sel.add(t);
		}
		while (table_Sel.size() > 1) {
			String thisT = this.node.get_Property().get(0).getTerm1()
					.get_variable();
			List<Property> lp = new ArrayList<Property>();
			for (int i = 0;;) {
				if (this.node.get_Property().get(i).getTerm1().get_variable() == thisT) {
					Term term1 = new Term(this.node.get_Property().get(i)
							.getTerm1().get_variable(), this.node
							.get_Property().get(i).getTerm1().get_column());
					Term term2 = new Term(this.node.get_Property().get(i)
							.getTerm2().get_constant());
					Conditions cd = this.node.get_Property().get(i).getCond();
					Property p1 = new Property(term1, cd, term2);
					lp.add(p1);
					this.node.get_Property().remove(i);
				} else
					i++;
				if (i == this.node.get_Property().size())
					break;
			}
			TreeNode newNode = new TreeNode(Optypes.SELECTION_OPERATOR, lp);
			newNode.left = this.node.left;
			this.node.left = newNode;
			table_Sel.clear();
			for (Property i : this.node.get_Property()) {
				String t = i.getTerm1().get_variable();
				if (!table_Sel.contains(t))
					table_Sel.add(t);
			}
		}
	}

	private int push_selection_helper(TreeNode n, String table) {
		if (n == null)
			return -1;
		if (n.left == null && n.right == null)
			return -1;
		if (n.left != null) {
			if (n.left.get_Optype() == Optypes.RENAME_OPERATOR
					&& n.left.get_Property().get(0).getTerm1().get_variable() == table) {
				this.push_parent = n;
				return 0;
			}
		}
		if (n.right != null) {
			if (n.right.get_Optype() == Optypes.RENAME_OPERATOR
					&& n.right.get_Property().get(0).getTerm1().get_variable() == table) {
				this.push_parent = n;
				return 1;
			}
		}
		int l = push_selection_helper(n.left, table);
		int r = push_selection_helper(n.right, table);
		if (l > -1)
			return l;
		else if (r > -1)
			return r;
		else
			return -1;
	}

	private void ClearNode(TreeNode n, int lr) {// left is 0, right is 1
		if (n == null)
			return;
		if (n == this.node && n == this.RA) {
			if (lr == 0)
				this.RA = this.RA.left;
			else
				this.RA = this.RA.right;
		}
		if (n.left == this.node) {
			if (lr == 0)
				n.left = this.node.left;
			else
				n.left = this.node.right;
			this.toClear.add(this.node);
			return;
		}
		if (n.right == this.node) {
			if (lr == 0)
				n.right = this.node.left;
			else
				n.right = this.node.right;
			this.toClear.add(this.node);
			return;
		}
		ClearNode(n.left, lr);
		ClearNode(n.right, lr);
	}

	private int push_selection() {
		if (this.node == null)
			return 0;
		System.out.println("pushing selection...");
		String table = this.node.get_Property().get(0).getTerm1()
				.get_variable();
		int left_right = push_selection_helper(this.node, table);
		if (left_right < 0) {
			System.out.println("Error: didn't find table to push selection!");
			return 1;
		}
		List<Property> lp = new ArrayList<Property>();
		lp.addAll(this.node.get_Property());
		TreeNode insertNode = new TreeNode(Optypes.SELECTION_OPERATOR, lp);
		if (left_right == 0) {
			insertNode.left = this.push_parent.left;
			this.push_parent.left = insertNode;
		} else if (left_right == 1) {
			insertNode.left = this.push_parent.right;
			this.push_parent.right = insertNode;
		}
		ClearNode(this.RA, 0);
		return 0;
	}

	private int push_semijoin_helper(TreeNode n, String table) {
		if (n == null)
			return -1;
		if (n.left == null && n.right == null)
			return -1;
		if (n.left != null) {
			Optypes opleft = n.left.get_Optype();
			if (((opleft == Optypes.RENAME_OPERATOR || opleft == Optypes.SELECTION_OPERATOR) && n.left
					.get_Property().get(0).getTerm1().get_variable() == table)
					|| (opleft == Optypes.PROJECTION_OPERATOR && n.left.get_TermList()
							.get(0).get_variable() == table)) {
				this.push_parent = n;
				return 0;
			}
		}
		if (n.right != null) {
			Optypes opright = n.right.get_Optype();
			if (((opright == Optypes.RENAME_OPERATOR || opright == Optypes.SELECTION_OPERATOR) && n.right
					.get_Property().get(0).getTerm1().get_variable() == table)
					|| (opright == Optypes.PROJECTION_OPERATOR && n.right.get_TermList()
							.get(0).get_variable() == table)) {
				this.push_parent = n;
				return 1;
			}
		}
		int l = push_semijoin_helper(n.left, table);
		int r = push_semijoin_helper(n.right, table);
		if (l > -1)
			return l;
		else if (r > -1)
			return r;
		else
			return -1;
	}

	private int push_semijoin() {
		if (this.node == null)
			return 0;
		System.out.println("pushing semijoin...");
		String table = "";
		int lr = -1;
		if (this.node.left.get_Optype() == Optypes.INTERSECTION_OPERATOR) {
			lr = 1;
			table = this.node.get_Property().get(0).getTerm2().get_variable();
		} else if (this.node.right.get_Optype() == Optypes.INTERSECTION_OPERATOR) {
			lr = 0;
			table = this.node.get_Property().get(0).getTerm1().get_variable();
		}
		if (table == "") {
			System.out.println("push semijoin new cases!");
			return 1;
		}
		int left_right = push_semijoin_helper(this.node, table);
		if (left_right < 0) {
			System.out.println("Error: didn't find table to push selection!");
			return 1;
		}
		List<Property> lp = new ArrayList<Property>();
		lp.addAll(this.node.get_Property());
		TreeNode insertNode = new TreeNode(Optypes.SEMIJOIN_OPERATOR, lp);
		if (left_right == 0) {
			if (lr == 0) {
				insertNode.left = this.push_parent.left;
				this.push_parent.left = insertNode;
				insertNode.right = this.node.right;
			} else {
				insertNode.right = this.push_parent.left;
				this.push_parent.left = insertNode;
				insertNode.left = this.node.left;
			}
		} else if (left_right == 1) {
			if (lr == 0) {
				insertNode.left = this.push_parent.right;
				this.push_parent.right = insertNode;
				insertNode.right = this.node.right;
			} else {
				insertNode.right = this.push_parent.right;
				this.push_parent.right = insertNode;
				insertNode.left = this.node.left;
			}
		}
		ClearNode(this.RA, lr);
		return 0;
	}

	private void add_projection_helper(TreeNode n) {
		if (n == null)
			return;
		switch (n.get_Optype()) {
		case AGGREGATION_OPERATOR:
			Set<String> table_Pro = new HashSet<String>();
			String aggrt = n.get_AggrTerm().get(0).get_variable();
			String aggrc = n.get_AggrTerm().get(0).get_column();
			if (!table_Pro.contains(aggrt))
				table_Pro.add(aggrt);
			for (Term i : n.get_TermList()) {
				String t = i.get_variable();
				if (!table_Pro.contains(t))
					table_Pro.add(t);
			}
			for (String i : table_Pro) {
				List<Term> l = new ArrayList<Term>();
				if (aggrt == i) {
					Term t2 = new Term(aggrt, aggrc);
					l.add(t2);
				}
				for (Term j : n.get_TermList()) {
					if (j.get_variable() == i) {
						Term t1 = new Term(i, j.get_column());
						l.add(t1);
					}
				}
				TreeNode newNode = new TreeNode(l);
				newNode.left = n.left;
				n.left = newNode;
			}
			break;
		case JOIN_OPERATOR:
			Term j1 = new Term(n.get_Property().get(0).getTerm1()
					.get_variable(), n.get_Property().get(0).getTerm1()
					.get_column());
			Term j2 = new Term(n.get_Property().get(0).getTerm2()
					.get_variable(), n.get_Property().get(0).getTerm2()
					.get_column());
			List<Term> l1 = new ArrayList<Term>();
			List<Term> l2 = new ArrayList<Term>();
			l1.add(j1);
			l2.add(j2);
			TreeNode leftN = new TreeNode(l1);
			TreeNode rightN = new TreeNode(l2);
			leftN.left = n.left;
			n.left = leftN;
			rightN.left = n.right;
			n.right = rightN;
			break;
		case SEMIJOIN_OPERATOR:
			Term j3 = new Term(n.get_Property().get(0).getTerm2()
					.get_variable(), n.get_Property().get(0).getTerm2()
					.get_column());
			Term j4 = new Term(n.get_Property().get(0).getTerm1()
					.get_variable(), n.get_Property().get(0).getTerm1()
					.get_column());
			List<Term> l3 = new ArrayList<Term>();
			List<Term> l4 = new ArrayList<Term>();
			l3.add(j3);
			l4.add(j4);
			TreeNode leftN2 = new TreeNode(l4);
			TreeNode rightN2 = new TreeNode(l3);
			rightN2.left = n.right;
			n.right = rightN2;
			leftN2.left = n.left;
			n.left = leftN2;
			break;
		default:
			break;
		}
		add_projection_helper(n.left);
		add_projection_helper(n.right);
	}

	private void add_projection() {
		System.out.println("adding projection...");
		if (this.RA == null)
			return;
		add_projection_helper(this.RA);
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		push_projection_helper(this.RA, map);
		reset_projection(this.RA, map);
		proj_intersect(this.RA);
	}

	private void push_projection_helper(TreeNode n,
			Map<String, List<String>> map) {
		if (n == null)
			return;
		if (n.get_Optype() == Optypes.PROJECTION_OPERATOR) {
			List<String> l1 = new ArrayList<String>();
			String t = n.get_TermList().get(0).get_variable();
			if (map.containsKey(t)) {
				for (String s : map.get(t)) {
					if (!l1.contains(s))
						l1.add(s);
				}
				map.remove(t);
			}
			for (Term t1 : n.get_TermList()) {
				if (!l1.contains(t1.get_column()))
					l1.add(t1.get_column());
			}
			map.put(t, l1);
			this.node = n;
			ClearNode(this.RA, 0);
		}
		push_projection_helper(n.left, map);
		push_projection_helper(n.right, map);
	}

	private void reset_projection(TreeNode n, Map<String, List<String>> map) {
		if (n == null)
			return;
		if (map.size() == 0)
			return;
		if (n.left != null) {
			if (n.left.get_Optype() == Optypes.RENAME_OPERATOR) {
				String key = n.left.get_Property().get(0).getTerm1()
						.get_variable();
				if (map.containsKey(key)) {
					List<Term> l1 = new ArrayList<Term>();
					for (String s : map.get(key)) {
						Term t1 = new Term(key, s);
						l1.add(t1);
					}
					TreeNode newNode = new TreeNode(l1);
					newNode.left = n.left;
					n.left = newNode;
					map.remove(key);
				}
			} else if (n.left.get_Optype() == Optypes.SELECTION_OPERATOR
					&& n.left.left.get_Optype() == Optypes.RENAME_OPERATOR) {
				String key = n.left.left.get_Property().get(0).getTerm1()
						.get_variable();
				if (map.containsKey(key)) {
					List<Term> l1 = new ArrayList<Term>();
					for (String s : map.get(key)) {
						Term t1 = new Term(key, s);
						l1.add(t1);
					}
					TreeNode newNode = new TreeNode(l1);
					newNode.left = n.left;
					n.left = newNode;
					map.remove(key);
				}
			}
		}
		if (n.right != null) {
			if (n.right.get_Optype() == Optypes.RENAME_OPERATOR) {
				String key = n.right.get_Property().get(0).getTerm1()
						.get_variable();
				if (map.containsKey(key)) {
					List<Term> l1 = new ArrayList<Term>();
					for (String s : map.get(key)) {
						Term t1 = new Term(key, s);
						l1.add(t1);
					}
					TreeNode newNode = new TreeNode(l1);
					newNode.left = n.right;
					n.right = newNode;
					map.remove(key);
				}
			} else if (n.right.get_Optype() == Optypes.SELECTION_OPERATOR
					&& n.right.left.get_Optype() == Optypes.RENAME_OPERATOR) {
				String key = n.right.left.get_Property().get(0).getTerm1()
						.get_variable();
				if (map.containsKey(key)) {
					List<Term> l2 = new ArrayList<Term>();
					for (String s : map.get(key)) {
						Term t2 = new Term(key, s);
						l2.add(t2);
					}
					TreeNode newNode = new TreeNode(l2);
					newNode.left = n.right;
					n.right = newNode;
					map.remove(key);
				}
			}
		}
		reset_projection(n.left, map);
		reset_projection(n.right, map);
	}

	private void proj_intersect(TreeNode n) {
		if (n == null)
			return;
		if (n.get_Optype() == Optypes.INTERSECTION_OPERATOR) {
			if (n.left != null && n.right != null) {
				if (n.left.get_Optype() == Optypes.PROJECTION_OPERATOR
						&& n.right.get_Optype() != Optypes.PROJECTION_OPERATOR) {
					List<Term> l = new ArrayList<Term>();
					for (Term t : n.left.get_TermList()) {
						l.add(new Term(t.get_variable(), t.get_column()));
					}
					TreeNode newNode = new TreeNode(l);
					newNode.left = n.right;
					n.right = newNode;
				} else if (n.right.get_Optype() == Optypes.PROJECTION_OPERATOR
						&& n.left.get_Optype() != Optypes.PROJECTION_OPERATOR) {
					List<Term> l = new ArrayList<Term>();
					for (Term t : n.right.get_TermList()) {
						l.add(new Term(t.get_variable(), t.get_column()));
					}
					TreeNode newNode = new TreeNode(l);
					newNode.left = n.left;
					n.left = newNode;
				}
			}
		}
		proj_intersect(n.left);
		proj_intersect(n.right);

	}

	private void spin_semijoin() {
		System.out.println("spinning semijoin...");
		Property p = new Property(this.node.get_Property().get(0).getTerm2(),
				this.node.get_Property().get(0).getCond(), this.node
						.get_Property().get(0).getTerm1());
		this.node.get_Property().set(0, p);
		TreeNode temp = this.node.left;
		this.node.left = this.node.right;
		this.node.right = temp;
	}

	private void ClearUp() {
		for (TreeNode n : this.toClear) {
			if (n.left != null)
				n.left = null;
			if (n.right != null)
				n.right = null;
		}
	}

	public RelationalAlgebra2RQNA(TreeNode initTree) {
		// do check validation
		int check = check_valid(initTree);
		if (check > 0)
			print_error(check);
		else {
			System.out.println("RelationalAlgebra2RQNA Initialized!");
			this.RA = initTree;
			this.node = null;
			this.toClear = new ArrayList<TreeNode>();
		}
	}

	public TreeNode RA2RQNA(boolean testcheck) {
		System.out.println("RelationalAlgebra to RQNA:");
		int check = 0;
		int errorN = 0;
		if (testcheck == true) {
			check = check_improve();
		}
		while (check > 0) {
			System.out.println("check result: " + check);
			if (check == 4)
				spin_semijoin();
			else if (check == 3)
				separate_selection();
			else if (check == 2)
				errorN = push_selection();
			else if (check == 1)
				errorN = push_semijoin();
			check = check_improve();
			if (errorN > 0) {
				System.out.println("There's an error! Paused!");
				break;
			}
		}
		System.out.println("Transforme done!");
		add_projection();
		ClearUp();
		System.out.println("---------------------------------------------------------------------------------------");
		System.out.println("Output RQNA:");
		return this.RA;
	}
}