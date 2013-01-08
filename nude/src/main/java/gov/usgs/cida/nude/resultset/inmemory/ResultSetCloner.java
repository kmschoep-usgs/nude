package gov.usgs.cida.nude.resultset.inmemory;

import gov.usgs.cida.nude.out.Closers;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [UNIT-TESTABLE]
 * @author dmsibley
 */
public class ResultSetCloner {
	private static final Logger log = LoggerFactory.getLogger(ResultSetCloner.class);
	
	protected final ResultSet mainRS;
	
	protected final List<ResultSetCloneIterator> its;
	protected AtomicInteger gottenClones;
	
	public ResultSetCloner(ResultSet rs, int numClones) {
		this.mainRS = rs;
		
		this.its = new ArrayList<ResultSetCloneIterator>();
		
		for (int i = 0; i < numClones; i++) {
			this.its.add(new ResultSetCloneIterator());
		}
		
		this.gottenClones = new AtomicInteger(-1);
	}
	
	public ResultSet cloneResultSet() {
		ResultSet result = null;
		int cloneIndex = gottenClones.incrementAndGet();
		
		if (cloneIndex < this.its.size()) {
			result = new ResultSetCloneRS(its.get(cloneIndex));
		}
		
		return result;
	}
	
//	public List<ResultSet> getClones() {
//		List<ResultSet> result = new ArrayList<ResultSet>();
//		
//		for (ResultSetCloneIterator it : its) {
//			result.add(new ResultSetCloneRS(it));
//		}
//		
//		return result;
//	}
	
	protected void addNextRow() throws SQLException {
		TableRow result = null;

		boolean isClosed = false;
		try {
			isClosed = mainRS.isClosed();
		} catch (AbstractMethodError t) {
			log.trace("Cannot tell if ResultSet is closed.");
		}
		
		boolean isNext = false;
		try {
			isNext = mainRS.next();
		} catch (SQLException e) {
			log.trace("couldn't get next", e);
		}
		
		if (!isClosed && isNext) {
			result = TableRow.buildTableRow(mainRS);
		}

		if (null != result) {
			for (ResultSetCloneIterator it : its) {
				it.buff.add(result);
			}
		}
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
		protected final List<TableRow> buff;
		
		public ResultSetCloneIterator() {
			super();
			buff = Collections.synchronizedList(new LinkedList<TableRow>());
		}
		
		@Override
		public boolean hasNext() {
			if (buff.isEmpty()) { //currIndex + 1 >= mainRows.size()
				try {
					addNextRow();
				} catch (SQLException ex) {
					log.debug("Tried to add another row to mainRows", ex);
				}
			}
			return !buff.isEmpty(); //currIndex + 1 < mainRows.size()
		}

		@Override
		public TableRow next() {
			TableRow result = null;
			if (this.hasNext()) {
				result = buff.remove(0);
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
