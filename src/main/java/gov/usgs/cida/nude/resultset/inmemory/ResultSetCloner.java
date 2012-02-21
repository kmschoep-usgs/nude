/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.usgs.cida.nude.resultset.inmemory;

import gov.usgs.cida.spec.out.Closers;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
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
		
		this.mainRows = new ArrayList<TableRow>();
	}
	
	public ResultSet cloneResultSet() {
		return new ResultSetCloneRS(new ResultSetCloneIterator());
	}
	
	public class ResultSetCloneRS extends IteratorWrappingResultSet {

		public ResultSetCloneRS(Iterator<TableRow> rows) {
			super(rows);
		}

		@Override
		public void close() throws SQLException {
			super.close();
			
			Closers.closeQuietly(mainRS);
		}
		
	}
	
	public class ResultSetCloneIterator implements Iterator<TableRow> {
		private int currIndex;
		
		public ResultSetCloneIterator() {
			super();
			currIndex = -1;
		}
		
		protected void addNextRow() throws SQLException {
			TableRow result = null;
			
			if (!mainRS.isClosed() && mainRS.next()) {
				result = TableRow.buildTableRow(mainRS);
			}
			
			if (null != result) {
				mainRows.add(result);
			}
		}
		
		@Override
		public boolean hasNext() {
			if (currIndex + 1 >= mainRows.size()) {
				try {
					this.addNextRow();
				} catch (SQLException ex) {
					log.debug("Tried to add another row to mainRows", ex);
				}
			}
			return currIndex + 1 < mainRows.size();
		}

		@Override
		public TableRow next() {
			TableRow result = null;
			if (this.hasNext()) {
				currIndex = currIndex + 1;
				result = mainRows.get(currIndex);
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
