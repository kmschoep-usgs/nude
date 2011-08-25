package gov.usgs.cida.values;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TableRow {
	protected Map<String, String> row;
	
	public TableRow() {
		this.row = new HashMap<String, String>();
	}
	
	public TableRow(String key, String value) {
		this.row = new HashMap<String, String>(3);
		this.row.put(key, value);
	}
	
	public void set(String key, String value) {
		this.row.put(key, value);
	}
	
	public String getValue(String key) {
		return this.row.get(key);
	}
	
	public Set<Entry<String, String>> getEntries() {
		return row.entrySet();
	}
}
