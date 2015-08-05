package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;

public class HybridJoinOperator extends JoinOperator{
	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	
	private HashMap<String, List<ArrayList<Tuple>>> joinHash;
	Iterator<ArrayList<Tuple>> currentBag;
	boolean hashed = false;

	public HybridJoinOperator(Operator left, Operator right, Expression expr){
		super(left, right, expr);		
		joinHash = new HashMap<String, List<ArrayList<Tuple>>>();
		currentBag = new ArrayList<ArrayList<Tuple>>().iterator();
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
//		leftTuple = left.readOneTuple();
		
		if (!hashed){
			long start = new Date().getTime();
			leftTuple = left.readOneTuple();		
			while(leftTuple != null){
				String key = leftTuple.get(leftIndex).toString();
				List<ArrayList<Tuple>> prev = joinHash.get(key);
//				System.out.println("current tuples " +prev);
				if (prev == null){
					prev = new ArrayList<ArrayList<Tuple>>();
					joinHash.put(key, prev);
				}
				prev.add(leftTuple);
				leftTuple = left.readOneTuple();
			}
			hashed = true;
//			System.out.println("==== Hashed in " + ((float) (new Date().getTime() - start)/ 1000) + "s");
		}
		
		//try to match more, if the current list is empty
		if (!currentBag.hasNext()){
			rightTuple = right.readOneTuple();
			while (rightTuple != null){
				String key = rightTuple.get(rightIndex).toString();
				List<ArrayList<Tuple>> hashedRight = joinHash.get(key);
				if (hashedRight != null){				
					currentBag = hashedRight.iterator();
//					System.out.println("list size - " +hashedRight.size());
					break;
				}
				else{
					rightTuple = right.readOneTuple();
				}
			}
		}
		
		if (rightTuple != null){
			if (currentBag.hasNext()) {
				ArrayList<Tuple> output = currentBag.next();
				//replace right half of output
				if (output.size() > this.divider){
					output = new ArrayList<Tuple>(output.subList(0, divider+1));
				}
				output.addAll(rightTuple);
//				System.out.println("Returning " + output);
				return output;
			}
		}
		return null;
	}
	
	@Override
	public void reset(){
		super.reset();
		currentBag = new ArrayList<ArrayList<Tuple>>().iterator();		
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		

		StringBuilder b = new StringBuilder("HYBRID JOIN ON " + this.expr +" WITH \n");

		Operator childOfRightBranch = this.right;
		
		while(childOfRightBranch != null)
		{
			b.append('\t' +childOfRightBranch.toString() + '\n');
			childOfRightBranch = childOfRightBranch.getChildOp();
		}
		
		return b.toString();
	}

}
