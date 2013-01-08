package gov.usgs.cida.nude.resultset.inmemory;

import gov.usgs.cida.nude.column.CGResultSetMetaData;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.out.Closers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuxResultSet extends PeekingResultSet {
	private static final Logger log = LoggerFactory
			.getLogger(MuxResultSet.class);
		
	protected final Map<ResultSet, TableRow> rsetRows;
	
	public MuxResultSet(Iterable<ResultSet> inputs) {
		this.closed = false;
		
		if (null == inputs) {
			inputs = new ArrayList<ResultSet>();
		}
		
		this.rsetRows = new HashMap<ResultSet, TableRow>();
		for (ResultSet rs : inputs) {
			this.rsetRows.put(rs, null);
		}
		
		ColumnGrouping columns = null;
		for (ResultSet rs : inputs) {
			ColumnGrouping cg = ColumnGrouping.getColumnGrouping(rs);
			if (null == columns) {
				columns = new ColumnGrouping(cg.getPrimaryKey());
			}
			columns = columns.join(cg);
		}
		this.columns = columns;
		
		this.metadata = new CGResultSetMetaData(this.columns);
		
	}

	protected void addNextRow() throws SQLException {
		TableRow result = null;
		Map<Column, String> row = new HashMap<Column, String>();
		
		List<Entry<ResultSet, TableRow>> usedEntries = new ArrayList<Entry<ResultSet, TableRow>>();
		TableRow minPrimaryKey = null;
		for (Entry<ResultSet, TableRow> rsTr : this.rsetRows.entrySet()) {
			ResultSet rs = rsTr.getKey();
			TableRow tr = rsTr.getValue();
			
			if (null == tr) { //Build next row
				boolean isClosed = false;
				try {
					isClosed = rs.isClosed();
				} catch (AbstractMethodError t) {
					log.trace("Cannot tell if ResultSet is closed.");
				}
				
				if (!isClosed && rs.next()) {
					tr = buildRow(rs);
					rsTr.setValue(tr);
				}
			}
			
			if (null != tr) {
				if (null == minPrimaryKey) {
					minPrimaryKey = tr;
					usedEntries.add(rsTr);
				} else {
					int negIfBetter = tr.compareTo(minPrimaryKey);
					if (0 > negIfBetter) {
						// replace current row
						minPrimaryKey = tr;
						usedEntries.clear();
						usedEntries.add(rsTr);
					} else if (0 == negIfBetter) {
						// add to current row
						usedEntries.add(rsTr);
					} else if (0 < negIfBetter) {
						// Skip
					}
				}
			}
		}
		
		for (Entry<ResultSet, TableRow> ent : usedEntries) {
			row.putAll(ent.getValue().getMap());
			ent.setValue(null);
		}
		
		if (0 < row.size()) {
			result = new TableRow(this.columns, row);
			this.nextRows.add(result);
		}
	}
	
	protected static TableRow buildRow(ResultSet rs) throws SQLException {
		TableRow result = null;
		Map<Column, String> row = new HashMap<Column, String>();
		
		for (Column col : ColumnGrouping.getColumnGrouping(rs)) {
			row.put(col, rs.getString(col.getName()));
		}
		
		result = new TableRow(ColumnGrouping.getColumnGrouping(rs), row);
		
		return result;
	}
	
	@Override
	public void close() throws SQLException {
		this.closed = true;
		for (ResultSet rs : this.rsetRows.keySet()) {
			try {
				Closers.closeQuietly(rs);
			} catch (Exception e) {
				log.error("Exception closing muxed ResultSet", e);
			}
		}
	}

	@Override
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		return "" + this.hashCode();
	}

}
