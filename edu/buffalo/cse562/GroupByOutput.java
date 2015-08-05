/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;

/**
 * @author Sathish
 *
 */
public class GroupByOutput {
	private int count;
	private ArrayList<Tuple> outputData;
	
	public GroupByOutput()
	{
		this.setCount(0);
		this.setOutputData(new  ArrayList<Tuple>());
	}
	public GroupByOutput(ArrayList<Tuple> outputData)
	{
		this.setCount(0);
		this.setOutputData(outputData);
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public ArrayList<Tuple> getOutputData() {
		return outputData;
	}

	public void setOutputData(ArrayList<Tuple> outputData) {
		this.outputData = outputData;
	}
	
	public GroupByOutput clone()
	{
		GroupByOutput gp = new GroupByOutput();
		gp.count = this.count;
		gp.outputData = new  ArrayList<Tuple>();
		
		for(Tuple tp: this.outputData)
		{
			gp.outputData.add(tp.cloneTuple(tp));
		}
		
		return gp;
	}

}
