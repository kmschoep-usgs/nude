package gov.usgs.cida.values;

import gov.usgs.cida.resultset.ITableView;
import gov.usgs.cida.spec.out.Closers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.naming.OperationNotSupportedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuiltTable implements ITableView, Iterable<TableRow> {
	private static Logger log = LoggerFactory.getLogger(BuiltTable.class);
	protected Map<Integer, Column> rows;
	protected ArrayList<String> columnNames;
	
	public BuiltTable() {
		this.rows = new TreeMap<Integer, Column>();
		this.columnNames = new ArrayList<String>();
	}
	
	public void readIn(ResultSet in) {
		//TODO
		read(this, in);
	}
	
	protected static void read(BuiltTable tb, ResultSet in) {
//		while (rs.next()) {
//		for (int i = 0; i < columns.length; i++) {
//			if (i >= result.size()) {
//				result.add(new ArrayList<String>());
//			}
//			result.get(i).add(rs.getString(columns[i]));
//		}
//	}
	}
	
	public static BuiltTable consume(ResultSet in) {
		BuiltTable result = new BuiltTable();
		
		try {
			read(result, in);
		} finally {
			Closers.closeQuietly(in);
		}
		
		return result;
	}
	
	public void addRow(TableRow row) {
		//TODO
	}
	
	public int getRowCount() {
		return 0;
	}
	
	public int getColumnCount() {
		return 0;
	}
	
	public ResultSet getResultSet() {
		//TODO
		return null;
	}
	
	protected static class Column {
		private ArrayList<String> data;
		
		public Column() {
			this.data = new ArrayList<String>();
		}
		
		public int size() {
			return this.data.size();
		}
		
		public void addRow(String value) {
			data.add(value);
		}
	}

	@Override
	public Iterator<TableRow> iterator() {
		return new TableIterator(this);
	}
	
	private static class TableIterator implements Iterator<TableRow> {
		protected BuiltTable table;
		
		public TableIterator(BuiltTable bt) {
			this.table = bt;
		}
		
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public TableRow next() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void remove() {
			
		}
	}
}
