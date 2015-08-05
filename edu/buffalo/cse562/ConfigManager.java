package edu.buffalo.cse562;

public class ConfigManager {
	private static String DATA_DIR;
	private static String STATIC_DIR;
	
	public static void setDataDir(String dir){
		DATA_DIR = dir;
	}
	
	public static String getDataDir(){
		return DATA_DIR;
	}
	
	public static void setSwapDir(String swDir){
		STATIC_DIR = swDir;
	}
	
	public static String getSwapDir(){
		return STATIC_DIR;
	}
}
