
package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.sf.jsqlparser.schema.Table;

/**
 * @author Shiva
 *
 */ 
public class ScanOperator implements Operator {

	//private File tableSource;
	private BufferedReader buffer;
	private Path dataFile;
	private String tableName = "";
	private String tableAlias = "";
	private HashMap<String,ColumnDetail> operatorTableSchema = null; 
	private HashMap<Integer, String> indexMaps = null;
	private TreeMap<Integer,Integer> shrinkedIndexMap = null;

	private Operator parentOperator = null;

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */	
	ScanOperator(Table table){	

		this.tableName = table.getName();
		this.tableAlias = table.getAlias();	
		shrinkedIndexMap = new TreeMap<Integer, Integer>();

		HashMap<String,ColumnDetail> intSchema = Main.tableMapping.get(this.tableName.toLowerCase());
		this.indexMaps = Main.indexTypeMaps.get(this.tableName.toLowerCase());		
		if (intSchema == null){
			intSchema = Main.tableMapping.get(this.tableName.toUpperCase());
			this.indexMaps = Main.indexTypeMaps.get(this.tableName.toUpperCase());
		}
		// old one
		// this.operatorTableSchema = this.initialiseOperatorTableSchema(intSchema);

		// new one
		this.operatorTableSchema = this.initialiseOutputTableSchema(intSchema);

		if(!tableName.toLowerCase().equalsIgnoreCase("temp"))
		{
			this.dataFile = FileSystems.getDefault().getPath(ConfigManager.getDataDir(), tableName.toLowerCase() +".dat");
		}
		else
		{
			this.dataFile = FileSystems.getDefault().getPath(ConfigManager.getSwapDir(), tableName.toLowerCase() +".dat");
		}

		reset();
	}

	@Override
	public ArrayList<Tuple> readOneTuple() {
		if(buffer == null) return null;
		String line = null;


		try {
			line = buffer.readLine();
		} 
		catch (IOException e) {
			//			e.printStackTrace();
			return null;
		}

		if(line == null || line.isEmpty()) { try {
			buffer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null; 
		}

		String col[] = line.split("\\|");	
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();

		// this for loop is previous naive method
		/*for(int counter = 0;counter < col.length;counter++) {
			if(indexMaps.containsKey(counter)){		
				String type = indexMaps.get(counter);			
				tuples.add(new Tuple(type.toLowerCase(), col[counter]));	
			}
		}
		 */


		// this for loop is optimised one. pick only the columns necessary
		for(Entry<Integer,Integer> ind: shrinkedIndexMap.entrySet())
		{
			String type = indexMaps.get(ind.getValue());			
			tuples.add(new Tuple(type.toLowerCase(), col[ind.getValue()]));	

		}


		return tuples;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		try {
			Charset charset = Charset.forName("US-ASCII");			
			this.buffer = Files.newBufferedReader(dataFile, charset);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}			
	}

	public String toString(){

		return "SCAN TABLE " + dataFile.getFileName().toString();
	}

	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setChildOp(Operator child) {		
		//null		
	}

	// deep copies the map from static table schema object to operatorTableSchema
	//Replaces table aliases
	private HashMap<String,ColumnDetail> initialiseOperatorTableSchema(HashMap<String,ColumnDetail>  createTableSchemaMap)
	{
		HashMap<String,ColumnDetail> opT = new HashMap<String,ColumnDetail>();		
		for(Entry<String, ColumnDetail> es : createTableSchemaMap.entrySet())
		{
			String nameKey = es.getKey();

			if(tableAlias != null) 
			{
				if(nameKey.contains("."))
				{
					String[] columnWholeTableName = nameKey.split("\\.");				
					nameKey = tableAlias +"."+columnWholeTableName[1]; 
				}
			}
			opT.put(nameKey,es.getValue().clone());
		}

		return opT;
	}



	private HashMap<String,ColumnDetail> initialiseOutputTableSchema(HashMap<String,ColumnDetail>  createTableSchemaMap)
	{
		HashMap<String,ColumnDetail> opT = new HashMap<String,ColumnDetail>();		

		String columnName = "";
		int counter = 0;
		for(Entry<String, ColumnDetail> es : createTableSchemaMap.entrySet())
		{
			String nameKey = es.getKey();

			if(tableAlias != null) 
			{
				if(nameKey.contains("."))
				{
					String[] columnWholeTableName = nameKey.split("\\.");				
					nameKey = tableAlias +"."+columnWholeTableName[1]; 
				}
			}

			if(nameKey.contains("."))
			{
				String[] columnWholeTableName = nameKey.split("\\.");	
				columnName = columnWholeTableName[1];
			}
			else
			{
				columnName = es.getKey();
			}

			if(Main.tableColumns.get(tableName.toLowerCase()).contains(columnName))
			{
				shrinkedIndexMap.put(counter, es.getValue().getIndex());
				ColumnDetail cd = es.getValue().clone();
				cd.setIndex(counter);
				opT.put(nameKey,cd);
				counter++;

			}



			//opT.put(nameKey,es.getValue().clone());
		}

		return opT;
	}

	public String getTableName()
	{
		return this.tableName;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		return this.operatorTableSchema;
	}

	@Override
	public Operator getParent() {

		return parentOperator;
	}

	@Override
	public void setParent(Operator parent) {
		this.parentOperator = parent;

	}
}
