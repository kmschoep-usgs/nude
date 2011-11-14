/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.usgs.cida.nude.resultset.inmemory;

import gov.usgs.cida.nude.column.CGResultSetMetaData;
import gov.usgs.cida.nude.column.ColumnGrouping;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class ResultSetCloner {
	private static final Logger log = LoggerFactory.getLogger(ResultSetCloner.class);
	
	protected final ResultSet mainRS;
	
	protected List<TableRow> mainRows;
	
	public ResultSetCloner(ResultSet rs) {
		this.mainRS = rs;
		
		this.mainRows = new LinkedList<TableRow>();
	}
	
	public ResultSet cloneResultSet() {
		return new IteratorWrappingResultSet(new ResultSetCloneIterator());
	}
	
	public class ResultSetCloneIterator implements Iterator<TableRow> {
		protected Iterator<TableRow> it;
		
		public ResultSetCloneIterator() {
			super();
			this.it = mainRows.iterator();
		}
		
		protected void addNextRow() throws SQLException {
			TableRow result = null;
			
			result = TableRow.buildTableRow(mainRS);
			
			if (null != result) {
				mainRows.add(result);
			}
		}
		
		@Override
		public boolean hasNext() {
			if (!this.it.hasNext()) {
				try {
					this.addNextRow();
				} catch (SQLException ex) {
					log.debug("Tried to add another row to mainRows", ex);
				}
			}
			return this.it.hasNext();
		}

		@Override
		public TableRow next() {
			TableRow result = null;
			if (this.hasNext()) {
				result = this.it.next();
			} else {
				throw new NoSuchElementException("No More Rows.");
			}
			
			return result;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Not supported.");
		}
		
	}
	
}
