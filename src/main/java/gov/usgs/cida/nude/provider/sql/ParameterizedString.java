package gov.usgs.cida.nude.provider.sql;


import gov.usgs.cida.nude.resultset.inmemory.TypedValue;
import java.util.ArrayList;
import java.util.List;

public class ParameterizedString {
	private StringBuffer clause;
	private List<TypedValue> values;
	private String valKey;
	
	/**
	 * Creates a new ParameterizedString that uses "?" as the default value replacement
	 */
	public ParameterizedString() {
		this.clause = new StringBuffer();
		this.values = new ArrayList<TypedValue>(); 
		this.valKey = "?";
	}
	
	/**
	 * Creates a new ParameterizedString that uses valKey as the value replacement
	 * @param valKey 
	 */
	public ParameterizedString(String valKey) {
		this.clause = new StringBuffer();
		this.values = new ArrayList<TypedValue>(); 
		this.valKey = valKey;
	}
	
	//Getters
	public String getClause() {
		return this.clause.toString();
	}
	public List<TypedValue> getValues() {
		return this.values;
	}
	
	//Setters
	public void setClause(String clause) {
		this.clause = new StringBuffer();
		this.clause.append(clause);
	}
	public void setValues(List<TypedValue> values) {
		this.values = values;
	}
	

	//Append methods
	public ParameterizedString append(String clause) {
		this.clause.append(clause);
		return this;
	}
	/**
	 * Appends a value keyword to the clause and adds the value to be set.
	 * @param value 
	 */
	public ParameterizedString append(TypedValue value) {
		this.clause.append(valKey);
		this.values.add(value);
		return this;
	}
	/**
	 * Appends a comma-space-separated list of value keywords to the clause and adds the values to be set.
	 * @param value 
	 */
	public ParameterizedString append(List<TypedValue> values) {
		if (null != values) {
			StringBuffer str = new StringBuffer();
			for (TypedValue val : values) {
				if (str.length() > 0) {
					str.append(", ");
				}
				str.append("?");
			}
			this.clause.append(str.toString());
			this.values.addAll(values);
		}
		return this;
	}
	public ParameterizedString append(ParameterizedString sql) {
		if (null != sql) {
			this.clause.append(sql.getClause());
			this.addValues(sql.getValues());
		}
		return this;
	}
	
	/**
	 * 
	 * @param value
	 * @deprecated use {@code append} instead
	 */
	@Deprecated
	public void addParam(TypedValue value) {
		this.clause.append(valKey);
		this.values.add(value);
	}
	/**
	 * 
	 * @param value
	 * @deprecated use {@code append} instead
	 */
	@Deprecated
	public void addValue(TypedValue value) {
		this.values.add(value);
	}
	/**
	 * 
	 * @param values
	 * @deprecated use {@code append} instead
	 */
	@Deprecated
	public void addValues(List<TypedValue> values) {
		this.values.addAll(values);
	}
	/**
	 * 
	 * @param clause
	 * @deprecated use {@code append} instead
	 */
	@Deprecated
	public void addClause(String clause) {
		this.clause.append(clause);
	}
	/**
	 * 
	 * @param sql
	 * @deprecated use {@code append} instead
	 */
	@Deprecated
	public void addSQL(ParameterizedString sql) {
		if (null != sql) {
			this.clause.append(sql.getClause());
			this.addValues(sql.getValues());
		}
	}
	
	
	@Override
	public String toString() {
		return this.clause.toString();
	}
	public String toEvaluatedString() {
		String result = this.clause.toString();
		
		for (TypedValue val : this.values) {
			result = result.replaceFirst(this.valKey, val.toString());
		}
		
		return result;
	}
}
