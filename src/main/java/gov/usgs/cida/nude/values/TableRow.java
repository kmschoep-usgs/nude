package gov.usgs.cida.nude.values;

import java.util.EnumMap;
import java.util.Map.Entry;
import java.util.Set;

/**
 * TODO Change Generic to ColumnGrouping
 * @author dmsibley
 *
 * @param <K>
 */
public class TableRow<K extends Enum<K>> implements Comparable<TableRow<K>>{
	protected final EnumMap<K, String> row;
	protected final K primaryKey;
	
	public TableRow(K primaryKey, String value) {
		if (null == primaryKey || null == value) {
			throw new RuntimeException("Primary key cannot be null");
		}
		this.row = new EnumMap<K, String>(primaryKey.getDeclaringClass());
		this.row.put(primaryKey, value);
		
		this.primaryKey = primaryKey;
	}
	
	public void set(K key, String value) {
		this.row.put(key, value);
	}
	
	public String getValue(K column) {
		return this.row.get(column);
	}
	
	public K getPrimaryKey() {
		return this.primaryKey;
	}
	
	public Set<Entry<K, String>> getEntries() {
		return row.entrySet();
	}

	/**
	 * Compares the values of the primary keys. (Values are compared as Strings)
	 */
	@Override
	public int compareTo(TableRow<K> o) {
		return this.getValue(this.getPrimaryKey()).compareTo(o.getValue(o.getPrimaryKey()));
	}
}
