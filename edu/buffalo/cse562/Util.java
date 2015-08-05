/**
 * 
 */
package edu.buffalo.cse562;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Util {

	public static boolean DEBUG = true;

	public static void printSchema(HashMap<String, ColumnDetail> inputSchema)
	{
		if(DEBUG)
		{
			System.out.println("________________________________");
			for(Map.Entry<String, ColumnDetail> colDetail: inputSchema.entrySet()){
				
				try
				{

				System.out.println(colDetail.getKey() + "   " + colDetail.getValue().getIndex() + "  " + colDetail.getValue().getColumnDefinition().getColDataType());
				}
				catch(Exception ex)
				{
					System.out.println("error starts");
					System.out.println("Column name: "+ colDetail.getKey());
					System.out.println("Column index: " + colDetail.getValue().getIndex());
					if(colDetail.getValue().getColumnDefinition() == null)
					{
						System.out.println("colDetail.getValue().getColumnDefinition() is null");
					}
					
					System.out.println("Column ends: "+ colDetail.getValue().getColumnDefinition().getColDataType());
					
					System.out.println("error ends");
					
				}
			}
			System.out.println("________________________________");
		}

	}

	public static String getSchemaAsString(HashMap<String, ColumnDetail> inputSchema)
	{

		StringBuilder str = new StringBuilder();
		for(Map.Entry<String, ColumnDetail> colDetail: inputSchema.entrySet()){

			str.append(colDetail.getKey()) ;
			str.append("|");
		}

		return str.toString();
	}
	static void printTuple(ArrayList<Tuple> singleTuple) {
		if(DEBUG)
		{
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
	}
	public static void printToStream(ArrayList<Tuple> singleTuple, PrintWriter printStream) {
		StringBuilder b = new StringBuilder();
		for(int i=0; i < singleTuple.size();i++){
			b.append(singleTuple.get(i));
			b.append("|");
		}
		printStream.println(b);
	}	

}
