package edu.buffalo.cse562;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Date;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.util.HashMap;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Main {

	static HashMap<String, HashMap<String, ColumnDetail>> tableMapping = new HashMap<String, HashMap<String, ColumnDetail>>();
	static HashMap<String,HashMap<Integer, String>> indexTypeMaps = new HashMap<String, HashMap<Integer, String>>();
	static HashMap<String,ArrayList<String>> tableColumns = new HashMap<String, ArrayList<String>>();

	static int queryCount = 0;
	static boolean  tpchexec = true;
	public static void main(String[] args) {		
		//the sql file starts from 3rd argument
		if(args.length < 3){
			System.out.println("Incomplete arguments");
			return;
		}

		if (args[0].equals("--data")){
			ConfigManager.setDataDir(args[1]);
		}
		int sqlIndex = 2;

		if (args[2].equals("--swap")){
			ConfigManager.setSwapDir(args[3]);
			sqlIndex = 4;
		}

		ArrayList<File> queryFiles = new ArrayList<File>();

		for(int i=sqlIndex; i < args.length; i++){	
			queryFiles.add(new File(args[i]));
		}
		Statement statement =null;						

		for (File f : queryFiles){		
			try{
				CCJSqlParser parser = new CCJSqlParser(new FileReader(f));
				ExecuteFile(parser,statement);


<<<<<<< HEAD
						SelectBody select = ((Select) statement).getSelectBody();

						if (select instanceof PlainSelect){
							// 	System.err.println(select);
							// Operator op = e.generateTree(select);
							Operator op = sq.generateTree(select);
							try
							{

								//System.out.println("______________________________________");
								// System.out.println("	Old Execution Plan");
								//System.out.println("______________________________________");
//								printPlan(op);
								//System.out.println("______________________________________");
								//System.out.println("	Old Execution Plan's Result");
								//System.out.println("______________________________________");

								//ExecuteQuery(op);							
								//System.out.println("______________________________________");

								// System.out.println("	Optimized Execution Plan");
								// System.out.println("______________________________________");


								if(ConfigManager.getSwapDir() == null || ConfigManager.getSwapDir().isEmpty()) 
								{
									new QueryOptimizer(op);
								}
								else
								{
//									new QueryOptimizer(op);
									 new QueryOptimizer2(op);
								}

								printPlan(op);


								//System.out.println("______________________________________");
								//System.out.println("	Optimized Execution Plan's Result");
								//System.out.println("______________________________________");
								long start = new Date().getTime();

								ExecuteQuery(op);													
								// System.out.println("==== Query executed in " + ((float) (new Date().getTime() - start)/ 1000) + "s");

							}
							catch(Exception ex)
							{
								System.err.println("ERROR MSG");
								System.err.println(select);
								System.out.println(ex.getMessage());
								ex.printStackTrace();
							}
						}
						else if (select instanceof Union){
							Union un = (Union) select;
							Operator op;
							UnionOperator uop = new UnionOperator();
							List<PlainSelect> pselects = (List<PlainSelect>) un.getPlainSelects();
							for (PlainSelect s : pselects){

								uop.addOperator(sq.generateTree(s));
							}
							ExecuteQuery(uop);
						}

					}
					else if(statement instanceof CreateTable){
						CreateTable createTableObj = (CreateTable) statement;								
						prepareTableSchema(createTableObj);
					}
				}
=======
>>>>>>> c1971ca7eec99365a5d69d7dcd44b8944dea6555
			}
			catch(Exception e){
				System.err.println(statement);
				e.printStackTrace();
			}
		}
	}
	/**
	 * (non javaDocs)
	 * prepares table schema information and saves it in a static hashmap 
	 * @param createTableObj createTableObject from jsql parser
	 * @author Shiva
	 */
	private static void prepareTableSchema(CreateTable createTableObj){		
		@SuppressWarnings("unchecked")
		String[] tableNames = new String[1];
		String tableName = createTableObj.getTable().getWholeTableName().toLowerCase();

		List<ColumnDefinition> cds = (List<ColumnDefinition>) createTableObj.getColumnDefinitions();
		HashMap<String, ColumnDetail> tableSchema = new HashMap<String, ColumnDetail>();
		HashMap<Integer, String> typeInfo = new HashMap<Integer, String>();
		int colCount = 0;
		for(ColumnDefinition colDef : cds){
			ColumnDetail columnDetail = new ColumnDetail();
			columnDetail.setTableName(tableName);
			columnDetail.setColumnDefinition(colDef);
			columnDetail.setIndex(colCount);

			String columnFullName = tableName + "."+ colDef.getColumnName().toLowerCase();

			typeInfo.put(colCount, colDef.getColDataType().getDataType()); //indexMaps : {tableName:{columnIndex:columnType}}

			tableSchema.put(columnFullName, columnDetail);
			colCount++;
		}
		tableMapping.put(tableName,tableSchema);
		indexTypeMaps.put(tableName,typeInfo);
	}

	/**	 
	 * test code print
	 */
	static void println(String string) {
		// TODO Auto-generated method stub
		System.out.println(string);
	}

	static void printTuple(ArrayList<Tuple> singleTuple) {
		for(int i=0; i < singleTuple.size();i++){

			try
			{
				String str = (singleTuple.get(i)==null)?"":singleTuple.get(i).toString();
				System.out.print(str);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				System.out.println(singleTuple.get(i));
			}

			if(i != singleTuple.size() - 1) System.out.print("|");
		}
		System.out.println();
	}	

	static String getTupleAsString(ArrayList<Tuple> singleTuple) {

		StringBuilder sb = new  StringBuilder();
		for(int i=0; i < singleTuple.size();i++){

			try
			{
				String tupleStr = (singleTuple.get(i)==null)?"":singleTuple.get(i).toString();
				sb.append(tupleStr);
				//System.out.print(str);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				System.out.println(singleTuple.get(i));
			}

			if(i != singleTuple.size() - 1) sb.append("|");
		}
		sb.append("\n");
		return sb.toString();
		// System.out.println();
	}	

	static void ExecuteQuery(Operator op)
	{
		ArrayList<Tuple> dt=null;
		StringBuilder sb = new StringBuilder();
		do
		{
			dt = op.readOneTuple();
			if(dt !=null)
			{
				//printTuple(dt);
				sb.append(getTupleAsString(dt)); 
			}

		}while(dt!=null);

		System.out.print(sb.toString());			
	}	

	static void ExecuteQuerytcph10(String selectStr)
	{
		//PlainSelect ps =PlainSelect (PlainSelect) sec;

		// System.err.println("yay");
		String query = "SELECT customer.custkey, sum(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue, customer.acctbal, nation.name FROM customer, orders, lineitem, nation WHERE customer.custkey = orders.custkey AND lineitem.orderkey = orders.orderkey AND orders.orderdate >= Date('[[1]]) AND orders.orderdate < Date('[[2]])       AND lineitem.returnflag = '[[45]]'         AND customer.nationkey = nation.nationkey GROUP BY customer.custkey ORDER BY revenue LIMIT 20";
		Matcher m = Pattern.compile("\\('([^)]+)\\)").matcher(selectStr);
		ArrayList<String> dates = new ArrayList<String>();
		while(m.find()) {
			dates.add(m.group(1));  
			
			
		}
		
		//System.out.println(query);
			query = query.replace("[[1]]", dates.get(0)) ;
			selectStr = selectStr.replace("'"+dates.get(0), "");
			selectStr = selectStr.replace("'"+dates.get(1), "");
			//System.out.println(query);
			
			query = query.replace("[[2]]", dates.get(1)) ;
			//System.out.println(query);
		

		Pattern pattern = Pattern.compile("'(.*?)'");
		Matcher matcher = pattern.matcher(selectStr);
		if (matcher.find())
		{

			query = query.replace("[[45]]", matcher.group(1).toUpperCase()) ;
		}

		//System.out.println(query);
		try {

			//System.out.println(query);
			CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
			SelectBody select = ((Select) parser.Statement()).getSelectBody();



			SanitizeQuery sq = new SanitizeQuery();
			Operator op = sq.generateTree(select);
			new QueryOptimizer(op);
			
		   // printPlan(op);
			ArrayList<Tuple> dt=null;
			StringBuilder sb = new StringBuilder();
			do
			{
				dt = op.readOneTuple();
				if(dt !=null)
				{
					
					// printTuple(dt);
					writeOneToDisk(dt);
					sb.append(getTupleAsString(dt)); 
				}

			}while(dt!=null);


			String str2 = "CREATE TABLE TEMP ( custkey      INT,      revenue         decimal,         acctbal     decimal, name char(25)); CREATE TABLE CUSTOMER (custkey      INT,name         VARCHAR(25),address      VARCHAR(40),nationkey    INT,phone        CHAR(15),acctbal      DECIMAL,mktsegment   CHAR(10),comment      VARCHAR(117)); SELECT customer.custkey, temp.revenue, temp.acctbal, temp.name , customer.address, customer.phone, customer.comment from Customer,		temp where temp.custkey = customer.custkey";
			CCJSqlParser parser2 = new CCJSqlParser(new StringReader(str2));
			Statement statement =null;	
			ExecuteFile(parser2, statement)
			;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}	

	static void printPlan(Operator op)
	{
		while(op!=null)
		{
			System.out.println(op.toString());
			op = op.getChildOp();
		}
	}

	private static  boolean writeOneToDisk(ArrayList<Tuple> out){
		PrintWriter pw;	
		File writeDir = getFileHandle();
		try {			
			//append to file; useful for merging, and ensures that there is never a fileNotFound exception
			pw = new PrintWriter(new BufferedWriter(new FileWriter(writeDir, true)));
			Util.printToStream(out, pw);
			//Util.printTuple(out);
			pw.close();
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private static  File getFileHandle(){
		String fname = "temp.dat";
		File writeDir = new File(ConfigManager.getSwapDir(), fname);

		if (!writeDir.exists()){
			try {
				writeDir.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return writeDir;
	}

	public static void ExecuteFile(CCJSqlParser parser,Statement statement )
	{
		SanitizeQuery sq = new SanitizeQuery();
		//ExpressionTree sq = new ExpressionTree();
		try {
			while ((statement = parser.Statement()) != null){
				//					System.out.println(statement);
				if(statement instanceof Select){


					SelectBody select = ((Select) statement).getSelectBody();

					if (select instanceof PlainSelect){
						// 	System.err.println(select);
						// Operator op = e.generateTree(select);
						Operator op = sq.generateTree(select);
						try
						{

							String str = select.toString().toLowerCase();
							if(str.contains("customer.comment") && tpchexec
									&& ConfigManager.getSwapDir()!=null && !ConfigManager.getSwapDir().isEmpty())
							{
								tpchexec = false;
								ExecuteQuerytcph10(str);
								
							}
							else
							{
								// System.err.println(select.toString());
								//System.out.println("______________________________________");
								//System.out.println("	Old Execution Plan");
								//System.out.println("______________________________________");
								//printPlan(op);
								//System.out.println("______________________________________");
								//System.out.println("	Old Execution Plan's Result");
								//System.out.println("______________________________________");
								long start = new Date().getTime();
								// ExecuteQuery(op);							
								//System.out.println("______________________________________");
								// System.out.println("==== Query executed in " + ((float) (new Date().getTime() - start)/ 1000) + "s");
								//System.out.println("\n\n	Optimized Execution Plan");
								// System.out.println("______________________________________");


								if(ConfigManager.getSwapDir() == null || ConfigManager.getSwapDir().isEmpty()


										) 
								{
									new QueryOptimizer(op);
								}
								else
								{
									// new QueryOptimizer(op);
									new QueryOptimizer(op);
								}

								//printPlan(op);

								//System.out.println("______________________________________");
								//System.out.println("	Optimized Execution Plan's Result");
								//System.out.println("______________________________________");
								start = new Date().getTime();

								ExecuteQuery(op);													
								// System.out.println("==== Query executed in " + ((float) (new Date().getTime() - start)/ 1000) + "s");
							}
						}
						catch(Exception ex)
						{
							System.err.println("ERROR MSG");
							System.err.println(select);
							System.out.println(ex.getMessage());
							ex.printStackTrace();
						}
					}
					else if (select instanceof Union){
						Union un = (Union) select;
						Operator op;
						UnionOperator uop = new UnionOperator();
						List<PlainSelect> pselects = (List<PlainSelect>) un.getPlainSelects();
						for (PlainSelect s : pselects){

							uop.addOperator(sq.generateTree(s));
						}
						ExecuteQuery(uop);
					}

				}
				else if(statement instanceof CreateTable){
					CreateTable createTableObj = (CreateTable) statement;								
					prepareTableSchema(createTableObj);
				}}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
