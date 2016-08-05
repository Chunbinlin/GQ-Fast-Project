package gqfast.unitTest;

import gqfast.unitTest.TestTree_logical2RQNA;
import gqfast.logical2RQNA.RelationalAlgebra2RQNA;

/* Selection: op, list(prop(term1(var,col), cond, term2(const)));
 * Join: op, list(prop(term1(var,col), cond=Eq, term2(var,col)));
 * Semijoin: op, list(prop(term1(var,col), cond=Eq, term2(var,col)));
 * Intersect: op, list(prop(term1(var,col), cond=Eq, term2(var,col)));
 * Rename: op, list(prop(term1(var,col=""), cond=Eq, term2(var,col="")));
 
 * Projection: op, list(terms);
 
 * Aggregate: op, term, list(terms), list(String);
  * */

public class Test {
	
	public static void main(String[] args) {
        TestTree_logical2RQNA test = new TestTree_logical2RQNA();
        test.TreeSD();
        System.out.println("=======================================");
        System.out.println("Input Relational Algebra:");
        test.print(test.getroot());
        System.out.println("\n---------------------------------------------------------------------------------------");
        RelationalAlgebra2RQNA ra = new RelationalAlgebra2RQNA(test.getroot()); 
        test.print(ra.RA2RQNA(true));
        System.out.println("\n=======================================");
    }
		
}

