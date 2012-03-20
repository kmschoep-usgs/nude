package gov.usgs.cida.nude.provider.sql;


import gov.usgs.cida.nude.resultset.inmemory.TypedValue;
import java.util.ArrayList;
import java.util.List;

public class ParameterizedString {
	private StringBuffer clause;
	private List<TypedValue> values;
	
	public ParameterizedString() {
		this.clause = new StringBuffer();
		this.values = new ArrayList<TypedValue>(); 
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
	

	//Add methods
	public void addValue(TypedValue value) {
		this.values.add(value);
	}
	public void addValues(List<TypedValue> values) {
		this.values.addAll(values);
	}
	public void addClause(String clause) {
		this.clause.append(clause);
	}
	public void addSQL(ParameterizedString sql) {
		if (null != sql) {
			this.clause.append(sql.getClause());
			this.addValues(sql.getValues());
		}
	}
	
	//Overrides
	@Override
	public String toString() {
		return this.clause.toString();
	}
}
