package gov.usgs.cida.nude.time;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.PeekingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class TimeFilteringResultSet extends PeekingResultSet {
	private static final Logger log = LoggerFactory.getLogger(TimeFilteringResultSet.class);
	
	protected final DateRange drc;
	protected final ResultSet in;
	
	protected final LinkedList<DateTime> requestedTimes;
	protected final Period duration;
	
	protected final LinkedList<TableRow> nextVals;
	
	public TimeFilteringResultSet(ResultSet input, DateRange drc) {
		try {
			this.closed = input.isClosed();
		} catch (Exception e) {
			this.closed = true;
		}
		
		this.in = input;
		this.drc = drc;
		
		this.nextVals = new LinkedList<TableRow>();
		
		this.requestedTimes = new LinkedList<DateTime>();
		Period period = Period.ZERO;
		if (DateRange.RangeType.DISCRETE == this.drc.type) {
			this.requestedTimes.addAll(this.drc.getDiscreteTimesteps());
			period = this.drc.getAcceptibleLag();
		}
		this.duration = period;
		
		this.columns = ColumnGrouping.getColumnGrouping(input);
		
	}
	
	@Override
	protected void addNextRow() throws SQLException {
		TableRow nextRow = null;
		
		if (DateRange.RangeType.DISCRETE == this.drc.type) {
			while (null == nextRow && !in.isClosed() && !requestedTimes.isEmpty()) {
				if (in.next()) {
					//build next input row, and then sync
					TableRow nextInRow = null;
					try {
						nextInRow = TableRow.buildTableRow(in);
					} catch (Exception e) {
						log.error("Could not build TableRow from in", e);
					}

					addToNextVals(nextInRow);
				} else {
					in.close();
				}

				if (isReadyToFlush() || in.isClosed()) {
					nextRow = buildNextOutRow();
				}
			}
		} else {
			if (!in.isClosed()) {
				if (in.next()) {
					nextRow = TableRow.buildTableRow(in);
				} else {
					in.close();
				}
			}
		}
		
		if (null != nextRow) {
			this.nextRows.add(nextRow);
		} else {
			log.trace("Could not add a next row");
		}
	}
	
	protected boolean isReadyToFlush() {
		boolean result = false;
		DateTime reqTime = requestedTimes.peekFirst();
		DateTime latestTime = getPrimaryKey(nextVals.peekLast());
		
		if (null != reqTime && null != latestTime) {
			result = reqTime.compareTo(latestTime) <= 0;
		} else {
			log.trace("req:" + reqTime + " next:" + latestTime);
		}
		
		return result;
	}
	
	/**
	 * adds a row into the nextVals if it's after the next cutoff
	 * @param nextInRow 
	 */
	protected void addToNextVals(TableRow nextInRow) {
		if (null != nextInRow) {
			DateTime nextInTime = getPrimaryKey(nextInRow);
			DateTime cutoff = requestedTimes.getFirst().minus(duration);

			if (null != nextInTime && cutoff.compareTo(nextInTime) <= 0) {
				this.nextVals.add(nextInRow);
			}
		}
		
	}
	
	/**
	 * pops all rows in the nextVals that are before or on the next requestedTime.
	 * @return the row created from those values
	 */
	protected TableRow buildNextOutRow() {
		TableRow result = null;
		DateTime reqTime = requestedTimes.pollFirst();
		Map<Column, String> outRow = new HashMap<Column, String>();
		
		if (null != reqTime) {
			int i = -1;
			for (TableRow t : nextVals) {
				DateTime nextTime = getPrimaryKey(t);
				if (reqTime.compareTo(nextTime) >= 0) {
					i++;
					putAllNonNull(outRow, t.getMap());
				}
			}
			for (; i > -1; i--) {
				nextVals.removeFirst();
			}
			
			outRow.put(this.columns.getPrimaryKey(), "" + reqTime.getMillis());
			
			result = new TableRow(columns, outRow);
		}
		
		return result;
	}

	@Override
	public void close() throws SQLException {
		try {
			in.close();
		} finally {
			this.closed = true;
		}
	}

	@Override
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		return in.getCursorName();
	}

	/**
	 * OPERATES VIA SIDE EFFECTS!
	 * Kinda like an "apply" function.  Only overwrites if their values are non-null
	 * @param <K>
	 * @param <V>
	 * @param that
	 * @param m 
	 */
	public static <K, V> void putAllNonNull(Map<K, V> that, Map<K, V> m) {
		if (null != that && null != m) {
			for(Map.Entry<K, V> mEnt : m.entrySet()) {
				if (null != mEnt.getValue() || !that.containsKey(mEnt.getKey())) {
					that.put(mEnt.getKey(), mEnt.getValue());
				}
			}
		}
	}
	
	public static DateTime getPrimaryKey(TableRow t) {
		DateTime result = null;
		
		if (null != t) {
			Column primaryKeyCol = t.getColumns().getPrimaryKey();
			String primKeyStr = t.getValue(primaryKeyCol);
			Long primKeyLong = new Long(primKeyStr);
			
			result = new DateTime(primKeyLong);
		}
		
		return result;
	}
}
