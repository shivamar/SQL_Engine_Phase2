/**
 * 
 */
package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;

/**
 * @author Sathish
 *
 */


public class GroupByOperator implements Operator {
	private  HashMap<String, ColumnDetail> inputSchema = null;
	private  HashMap<String, ColumnDetail> outputSchema = null;
	private ArrayList<ArrayList<Tuple>> outputDataList =null;
	private Operator input;
	private List<Column> groupByColumns;
	private List<AggregateFunctionColumn> aggregateFunctions;

	private HashMap<String, GroupByOutput> outputData;
	private boolean isGroupByComputed;
	private int rowIndex;

	private Operator parentOperator = null;

	public GroupByOperator(Operator input, List<Column> groupByColumns,
			List<AggregateFunctionColumn> aggregateFunctions) {

		setChildOp(input);

		this.groupByColumns = groupByColumns;
		this.aggregateFunctions = aggregateFunctions;
		this.outputSchema = getOutputSchema();
		//Util.printSchema(outputSchema);
		outputData = new HashMap<String, GroupByOutput>();
		isGroupByComputed = false;
		rowIndex =0;
	}



	@Override
	public ArrayList<Tuple> readOneTuple() {

		ComputeGroupBy();
		ArrayList<Tuple> tuple =null;
		if(outputDataList.size()>rowIndex)
		{
			tuple = outputDataList.get(rowIndex);
			// Util.printTuple(tuple);
		}
		rowIndex ++;

		return tuple;
	}

	@Override
	public void reset() {
		this.inputSchema = input.getOutputTupleSchema();

	}

	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return this.input;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		if(this.outputSchema == null)
		{
			this.outputSchema = getOutputSchema();
		}
		return this.outputSchema ;
	}

	private void ComputeGroupBy()
	{
		if(!isGroupByComputed)
		{
			ArrayList<Tuple> inputtuple = input.readOneTuple();
			ArrayList<Tuple> gropuByCols = null;
			while(inputtuple!=null)
			{
				gropuByCols = getGroupByColumnArrayList(inputtuple, this.groupByColumns);
				String hashKey = getHashKey(gropuByCols);


				int funcIndex = inputtuple.size();
				// System.out.println(funcIndex);
				if(this.aggregateFunctions == null || this.aggregateFunctions.size() == 0)
				{
					GroupByOutput gp = new GroupByOutput();
					gp.setOutputData(inputtuple);
					outputData.put(hashKey,gp);
				}
				else
				{
					for(AggregateFunctionColumn funcCol:this.aggregateFunctions)
					{
						Evaluator evaluator = new Evaluator(inputtuple,inputSchema);
						Function func = funcCol.getFunction();
						ExpressionList exps = func.getParameters();
						Expression exp;
						if (exps != null){
							exp = (Expression) exps.getExpressions().get(0);
							
							Tuple tup = evaluateExpression(evaluator, exp);
							handleAggregateFunctions(func,inputtuple,hashKey,funcIndex,tup);
							funcIndex++;
							
							
							
							
						}
						else{
							handleCountFunction(hashKey,inputtuple,funcIndex);
							funcIndex++;
						}

					}
					}
				inputtuple = input.readOneTuple();
			}

			ComputeAverage();
			outputDataList = getArrayListFromHashMap(this.outputData);
			isGroupByComputed = true;
		}
	}

	private void handleAggregateFunctions(Function func,ArrayList<Tuple> outputtuple, String hashKey, int funcIndex, Tuple tup )
	{
		if(func.getName().equalsIgnoreCase("sum"))
		{
			handleSumFunction(hashKey,outputtuple, funcIndex,tup);
		}

		if(func.getName().equalsIgnoreCase("avg"))
		{
			handleAvgFunction(hashKey,outputtuple,funcIndex,tup);
		}
		if(func.getName().equalsIgnoreCase("min"))
		{
			handleMinFunction(hashKey,outputtuple,funcIndex,tup);
		}
		if(func.getName().equalsIgnoreCase("max"))
		{
			handleMaxFunction(hashKey,outputtuple,funcIndex,tup);
		}
		if(func.getName().equalsIgnoreCase("count"))
		{
			handleCountFunction(hashKey,outputtuple,funcIndex);
		}

	}

	private void handleSumFunction( String hashKey,ArrayList<Tuple> outputtuple,int funcIndex ,Tuple tup)
	{

		if(outputData.get(hashKey) == null)
		{
			outputtuple.add(tup.cloneTuple(tup));
			outputData.put(hashKey, new GroupByOutput( outputtuple));
		}
		else
		{


			ArrayList<Tuple> existingTuple = outputData.get(hashKey).getOutputData();

			if(funcIndex ==existingTuple.size() )
			{
				existingTuple.add(tup.cloneTuple(tup));
			}
			else
			{
				Tuple sumDatum = existingTuple.get(funcIndex);
				sumDatum = sumDatum.add(tup);
			}
			// System.out.println("AVG "+funcIndex+" " +sumDatum.toString() + " "+existingTuple.get(funcIndex) + " "+  outputData.get(hashKey).getOutputData().get(funcIndex) );
		}


	}


	private void handleAvgFunction( String hashKey,
			ArrayList<Tuple> outputtuple, int funcIndex, Tuple tup) {

		// average is nothing but sum divided by count
		// so count variable is incremented each time it finds a match
		// since there is no NULL value in text file, we can have a single count for any column
		// if there are two AVG functions, count will be incremented twice for each tuple
		// it's handled in ComputeAverage function 

		if(outputData.get(hashKey) == null)
		{
			outputtuple.add(tup.cloneTuple(tup));
			outputData.put(hashKey, new GroupByOutput( outputtuple));
		}
		else
		{


			ArrayList<Tuple> existingTuple = outputData.get(hashKey).getOutputData();

			if(funcIndex ==existingTuple.size() )
			{
				existingTuple.add(tup.cloneTuple(tup));
			}
			else
			{
				Tuple sumDatum = existingTuple.get(funcIndex);
				sumDatum = sumDatum.add(tup.cloneTuple(tup));
			}
			// System.out.println("AVG "+funcIndex+" " +sumDatum.toString() + " "+existingTuple.get(funcIndex) + " "+  outputData.get(hashKey).getOutputData().get(funcIndex) );
		}
		outputData.get(hashKey).setCount(outputData.get(hashKey).getCount()+1);

	}


	private void handleMinFunction( String hashKey,
			ArrayList<Tuple> outputtuple, int funcIndex, Tuple tup) {

		if(outputData.get(hashKey) == null)
		{
			outputtuple.add(tup);
			outputData.put(hashKey, new GroupByOutput( outputtuple));
		}
		else
		{
			ArrayList<Tuple> existingTuple = outputData.get(hashKey).getOutputData();


			if(funcIndex ==existingTuple.size() )
			{
				existingTuple.add(tup);
			}
			else
			{
				Tuple existingDatum = existingTuple.get(funcIndex);
				existingDatum = (tup.isLessThan(existingDatum))?tup:existingDatum;
				existingTuple.get(funcIndex).Update(existingDatum);
			}
		}


	}

	private void handleMaxFunction( String hashKey,
			ArrayList<Tuple> outputtuple, int funcIndex, Tuple tup) {

		if(outputData.get(hashKey) == null)
		{
			outputtuple.add(tup);
			outputData.put(hashKey, new GroupByOutput( outputtuple));
		}
		else
		{
			ArrayList<Tuple> existingTuple = outputData.get(hashKey).getOutputData();
			if(funcIndex ==existingTuple.size() )
			{
				existingTuple.add(tup);
			}
			else
			{
				Tuple datum = existingTuple.get(funcIndex);
				datum = (tup.isGreaterThan(datum))?tup:datum;
				existingTuple.get(funcIndex).Update(datum);
			}
			//Util.printTuple(existingTuple);
			//Util.printTuple(outputData.get(hashKey).getOutputData());
		}

	}

	private void handleCountFunction( String hashKey,
			ArrayList<Tuple> outputtuple, int funcIndex) {


		Tuple tup = new Tuple("int","1");
		if(outputData.get(hashKey) == null)
		{
			outputtuple.add(tup);
			outputData.put(hashKey, new GroupByOutput( outputtuple));

			//System.out.println(hashKey);
			//Util.printTuple(outputData.get(hashKey).getOutputData());
			//System.out.println();
		}
		else
		{
			try
			{
				ArrayList<Tuple> existingTuple = outputData.get(hashKey).getOutputData();
				if(funcIndex ==existingTuple.size() )
				{
					existingTuple.add(tup);
				}
				else
				{
					Tuple sumDatum = existingTuple.get(funcIndex);
					sumDatum = sumDatum.add(tup);
				}
				//System.out.println(hashKey);
				//Util.printTuple(outputData.get(hashKey).getOutputData());
				//System.out.println();
				// Util.printTuple(existingTuple);
				// System.out.println("COUNT "+funcIndex+"  "+existingTuple.get(funcIndex) + " "+  outputData.get(hashKey).getOutputData().get(funcIndex) );
			}catch(Exception ex)
			{
				System.out.println("errorrr");
				System.out.println(hashKey);
				Util.printTuple(outputData.get(hashKey).getOutputData());
				ex.printStackTrace();
				throw ex;
			}
		}
	}

	private HashMap<String, ColumnDetail> getOutputSchema() {

		copyInputSchemaToOutputSchema();
		int index =inputSchema.keySet().size();
		for(AggregateFunctionColumn agf :this.aggregateFunctions)
		{

			String key = agf.getFunction().toString();
			ColumnDetail colDet = getColumnDetailForFunction(agf.getFunction());
			colDet.setIndex(index);
			outputSchema.put(key, colDet);

			if(agf.getAliasName()!=null && !agf.getAliasName().equalsIgnoreCase(""))
			{
				outputSchema.put(agf.getAliasName(), colDet.clone());
			}

			index++;
		}

		// System.out.println(inputSchema.keySet().size() + " "+this.aggregateFunctions.size() + " " + index);
		return outputSchema;
	}

	private ColumnDetail getColumnDetailForFunction(Function func)
	{

		//colDet.setColumnDefinition(coldef.setColDataType(););

		ColumnDetail colDet = null;
		ExpressionList exps = func.getParameters();

		if (exps != null){
			for( Object expObj: exps.getExpressions())
			{
				if(expObj instanceof Column)
				{
					colDet = Evaluator.getColumnDetail(outputSchema, (Column) expObj).clone() ;
					if(colDet!=null) return colDet;
				}
			}
		}


		colDet = new ColumnDetail();
		colDet.setColumnDefinition(new ColumnDefinition());
		colDet.getColumnDefinition().setColDataType(new ColDataType());
		colDet.getColumnDefinition().getColDataType().setDataType("decimal");
		return colDet;
	}
	private ArrayList<Tuple> getGroupByColumnArrayList(ArrayList<Tuple> tuple, List<Column> columns )
	{
		ArrayList<Tuple> groupByColArrayList = new ArrayList<>();

		if(columns==null||columns.size() == 0)
			return groupByColArrayList; 

		for(Column col: columns)
		{
			int index =0;
			try
			{
				index = Evaluator.getColumnDetail(inputSchema, col).getIndex() ;
			}
			catch(Exception ex)
			{

				String errorMesage = Util.getSchemaAsString(inputSchema) + "\r\n col: " +col.getWholeColumnName() ; 
				System.out.println(errorMesage);
			}
			groupByColArrayList.add(tuple.get(index));
		}

		return groupByColArrayList;

	}

	private String getHashKey(ArrayList<Tuple> groupByColumnTuple)
	{
		if(groupByColumnTuple == null || groupByColumnTuple.size() ==0)
		{
			return "1";
		}
		StringBuilder sb = new StringBuilder();
		for(Tuple t:groupByColumnTuple)
		{
			sb.append(t.toString());
			sb.append("|");
		}
		return sb.toString();
	}

	private Tuple evaluateExpression(Evaluator evaluator,Expression exp)
	{
		Tuple tup =null;
		try {
			tup = new Tuple(evaluator.eval(exp));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch(IndexOutOfBoundsException e)
		{
			System.out.println(exp);
			e.printStackTrace();
		}

		return tup;
	}

	private List<Integer> getAvgFunctionIndices()
	{
		List<Integer> avgIndices = new ArrayList<Integer>();
		for(Map.Entry<String, ColumnDetail> colDetail: this.outputSchema.entrySet()){

			if(colDetail.getKey().toLowerCase().contains("avg("))
			{
				int index = colDetail.getValue().getIndex();
				// System.out.println("index "+index +" "+ colDetail.getKey());
				avgIndices.add(index);
			}
		}

		return avgIndices;
	}

	private void ComputeAverage()
	{
		List<Integer> avg = getAvgFunctionIndices();
		if(avg.size()>=1)
		{
			for(Map.Entry<String, GroupByOutput> colDetail: this.outputData.entrySet()){
				// System.out.println("Count"+ colDetail.getValue().getCount());
				Integer count = colDetail.getValue().getCount()/avg.size();
				// System.out.println("Count"+ count);
				for(Integer avgIndex :avg)
				{
					// System.out.println("Index:" +avgIndex);
					Tuple sum = colDetail.getValue().getOutputData().get(avgIndex);
					// System.out.println("Before:"+ colDetail.getValue().getOutputData().get(avgIndex));
					sum = sum.divideBy(new Tuple("int",count.toString()));
					// System.out.println("After:"+ colDetail.getValue().getOutputData().get(avgIndex));

				}

			}
		}
	}

	private ArrayList<ArrayList<Tuple>> getArrayListFromHashMap(HashMap<String,GroupByOutput> outputData)
	{
		ArrayList<ArrayList<Tuple>> outputDataList = new ArrayList<ArrayList<Tuple>>();
		for(Map.Entry<String, GroupByOutput> colDetail: outputData.entrySet()){

			//System.out.println(colDetail.getValue().getOutputData().get(8));
			outputDataList.add(colDetail.getValue().getOutputData());

		}
		return outputDataList;

	}

	private void copyInputSchemaToOutputSchema()
	{
		outputSchema = new HashMap<String, ColumnDetail>();

		for(Map.Entry<String, ColumnDetail> colDetail: this.inputSchema.entrySet()){
			outputSchema.put(colDetail.getKey(),colDetail.getValue().clone());

		}
	}

	public String toString(){
		return "GROUP BY " + groupByColumns.toString();
	}

	private ArrayList<Tuple> clone(ArrayList<Tuple> tuple)
	{
		if(tuple == null)
			return null;
		ArrayList<Tuple> clonedTuple = new ArrayList<Tuple>();

		for( Tuple t: tuple)
		{
			clonedTuple.add(t.cloneTuple(t));
		}
		return clonedTuple;
	}

	public void setChildOp(Operator child) {		
		this.input = child;	
		input.setParent(this);

		reset();
	}

	@Override
	public Operator getParent() {
		return this.parentOperator;
	}

	@Override
	public void setParent(Operator parent) {
		this.parentOperator = parent;		
	}

	//TODO to sathish : check if u should include aggregate columns in the output of this method getGroupByColumns()
	public List<Column> getGroupByColumns()
	{
		return this.groupByColumns;		
	}
	
	public List<AggregateFunctionColumn> getAggregateFunctions(){
		return this.aggregateFunctions;
	}
}

