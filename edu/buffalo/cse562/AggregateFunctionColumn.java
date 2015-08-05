/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Function;

/**
 * @author Sathish
 *
 */
public class AggregateFunctionColumn {
	
	private Function function;
	private String aliasName;

	public Function getFunction() {
		return function;
	}

	public void setFunction(Function function) {
		this.function = function;
	}

	public String getAliasName() {
		return aliasName;
	}

	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}

}
