package gov.usgs.cida.nude.filter.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.math.BigDecimal;
import java.util.LinkedList;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO [UNIT-TESTABLE]  I already found a bug in this code once. freakin unit test it.
 * @author dmsibley
 */
public class WindowedMathTransform implements ColumnTransform {
	private static final Logger log = LoggerFactory.getLogger(WindowedMathTransform.class);
	
	protected final Column col;
	protected final Column timeCol;
	protected final MathFunction func;
	protected final Period duration;
	protected LinkedList<DateTime> times;
	protected LinkedList<BigDecimal> vals;
	
	protected boolean started;
	
	public WindowedMathTransform(Column time, Column val, MathFunction func, Period duration) {
		this.times = new LinkedList<DateTime>();
		this.vals = new LinkedList<BigDecimal>();
		this.col = val;
		this.timeCol = time;
		this.func = func;
		this.duration = duration;
		
		this.started = false;
	}
	
	@Override
	public String transform(TableRow row) {
		String result = null;
		String val = row.getValue(col);
		String time = row.getValue(timeCol);
		
		BigDecimal bigVal = null;
		if (null != val) {
			try {
				bigVal = new BigDecimal(val);
			} catch (NumberFormatException e) {
				log.trace("NumberFormatException for " + val);
			}
		}
		
		boolean fullDuration = sync(new DateTime(Long.parseLong(time)), bigVal, times, vals, duration);
		started = started || fullDuration;
		
		if (!started) {
			started = checkLogicallyFull(times, duration);
		}
		
		if (started) {
			BigDecimal run = func.run(vals);
			if (null != run) {
				result = run.toPlainString();
			}
		}
		
		return result;
	}
	
	public static boolean sync(DateTime nextDate, BigDecimal nextVal, LinkedList<DateTime> dates, LinkedList<BigDecimal> values, Period duration) {
		if (null != nextVal) {
			dates.addLast(nextDate);
			values.addLast(nextVal);
		}
		
		DateTime cutoff = nextDate.minus(duration);
		boolean isDurationFull = cutoff.isAfter(dates.peekFirst()) || cutoff.isEqual(dates.peekFirst());
		
		while (cutoff.isAfter(dates.peekFirst()) || cutoff.isEqual(dates.peekFirst())) {
			dates.pollFirst();
			values.pollFirst();
		}
		
		return isDurationFull;
	}
	
	public static boolean checkLogicallyFull(LinkedList<DateTime> dates, Period duration) {
		boolean result = false;
		
		if (null != dates && null != duration && 1 < dates.size()) {
			int periods = 0;
			for (int i = 0; i < (dates.size() - 1); i++) {
				periods += new Period(dates.get(i), dates.get(i + 1)).toStandardSeconds().getSeconds();
			}

			int mean = periods / (dates.size() - 1);
			Period meanPeriod = Seconds.seconds(mean).toPeriod();

			DateTime nextDate = dates.getLast().plus(meanPeriod);
			DateTime cutoff = nextDate.minus(duration);

			if (cutoff.isAfter(dates.getFirst()) || cutoff.isEqual(dates.getFirst())) {
				result = true;
			}
		}
		
		
		return result;
	}
}
