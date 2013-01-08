package gov.usgs.cida.nude.time;

import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [UNIT-TESTABLE]
 * @author dmsibley
 */
public class DateRange {
	private static final Logger log = LoggerFactory.getLogger(DateRange.class);
	
	public static enum StandardDateParams {
		beginPosition,
		endPosition
	}
	
	public static enum RangeType {
		CONTINUOUS,
		DISCRETE
	}
	
	public final RangeType type;
	public final Period acceptibleLag;
	
	public final DateTime begin;
	public final DateTime end;
	public final Period every;
	
	public final List<DateTime> discreteTimesteps;
	
	public DateRange() {
		this(DateTime.now().minusMonths(1), DateTime.now());
	}
	
	public DateRange(DateTime begin, DateTime end) {
		this.type = RangeType.CONTINUOUS;
		this.acceptibleLag = Period.ZERO;
		
		this.begin = begin;
		this.end = end;
		
		this.discreteTimesteps = null;
		this.every = null;
	}
	
	public DateRange(List<DateTime> timesteps, Period acceptibleLag) {
		this(timesteps, acceptibleLag, null, null);
	}
	
	public DateRange(List<DateTime> timesteps, Period acceptibleLag, Period prefill, Period every) {
		this.type = RangeType.DISCRETE;
		this.acceptibleLag = acceptibleLag;
		
		Period _prefill = Period.months(1);
		if (null != prefill) {
			_prefill = prefill;
		}
		
		if (null != timesteps && 0 < timesteps.size()) {
			this.begin = timesteps.get(0).minus(_prefill);
			this.end = timesteps.get(timesteps.size() - 1);
		} else {
			throw new IllegalArgumentException("No timesteps were found!");
		}
		
		List<DateTime> d = new ArrayList<DateTime>();
		d.addAll(timesteps);
		this.discreteTimesteps = Collections.unmodifiableList(timesteps);
		
		this.every = every;
	}

	public DateTime getBegin() {
		return begin;
	}

	public DateTime getEnd() {
		return end;
	}
	
	public Period getEvery() {
		return every;
	}

	public RangeType getType() {
		return type;
	}

	public Period getAcceptibleLag() {
		return acceptibleLag;
	}

	public List<DateTime> getDiscreteTimesteps() {
		return discreteTimesteps;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 79)
				.append(this.getBegin())
				.append(this.getEnd())
				.append(this.getType())
				.append(this.getAcceptibleLag())
				.append(this.getDiscreteTimesteps())
				.toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) { return false; }
		if (obj == this) { return true; }
		if (obj instanceof DateRange) {
			DateRange rhs = (DateRange) obj;
			return new EqualsBuilder()
				.append(this.getBegin(), rhs.getBegin())
				.append(this.getEnd(), rhs.getEnd())
				.append(this.getType(), rhs.getType())
				.append(this.getAcceptibleLag(), rhs.getAcceptibleLag())
				.append(this.getDiscreteTimesteps(), rhs.getDiscreteTimesteps())
				.isEquals();
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(DateRange.class)
				.add("startDate", this.getBegin())
				.add("endDate", this.getEnd())
				.add("type", this.getType().name())
				.add("every", this.getEvery())
				.toString();
	}
	
	public static DateRange parseContinuousDateRange(String beginTime, String endTime, DateTimeFormatter dtf, DateTimeZone dtz) {
		DateRange result = null;

		DateTime now = DateTime.now(dtz).withMillisOfSecond(0);

		boolean hasEnd = StringUtils.isNotEmpty(endTime);
		boolean hasBegin = StringUtils.isNotEmpty(beginTime);

		DateTime begin = null;
		Period beginPeriod = null;

		DateTime end = null;
		Period endPeriod = null;


		if (hasBegin || hasEnd) {
			if (hasEnd) {
				try {
					end = dtf.parseDateTime(endTime).withZoneRetainFields(dtz);
					end = end.withTime(23, 59, 59, 0);
				} catch (Exception e) {
					log.debug("endPosition " + endTime + " is not a Date, trying DateTime");
				}
				if (null == end) {
					try {
						end = DateTime.parse(endTime);
					} catch (Exception e) {
						log.debug("endPosition " + endTime + " is not a DateTime, trying Period");
					}
				}
				if (null == end) {
					try {
						endPeriod = Period.parse(endTime);
					} catch (Exception e) {
						log.info("endPosition " + endTime + " is not a Period, defaulting to now");
					}
				}
			}
			if (null == end && null == endPeriod) {
				end = now;
			}

			if (hasBegin) {
				try {
					begin = dtf.parseDateTime(beginTime).withZoneRetainFields(dtz);
				} catch (Exception e) {
					log.debug("beginPosition " + beginTime + " is not a Date, trying DateTime");
				}
				if (null == begin) {
					try {
						begin = DateTime.parse(beginTime);
					} catch (Exception e) {
						log.debug("beginPosition " + beginTime + " is not a DateTime, trying Period");
					}
				}
				if (null == begin) {
					try {
						beginPeriod = Period.parse(beginTime);
					} catch (Exception e) {
						log.debug("beginPosition " + beginTime + " is not a Period, defaulting to one month before end");
					}
				}
			}
			if (null == begin && null == beginPeriod) {
				beginPeriod = Period.months(1);
			}


			if (null != begin && null != end) {
				//We're good to go, continue
			} else if (null != beginPeriod && null != end) {
				begin = end.minus(beginPeriod);
			} else if (null != begin && null != endPeriod) {
				end = begin.plus(endPeriod);
			} else if (null != beginPeriod && null != endPeriod) {
				//Evaluate end from now and begin from end
				end = now.minus(endPeriod);
				begin = end.minus(beginPeriod);
			} else if (null == begin && null == end) {
				log.info("Time range  " + beginTime + " " + endTime + " cannot be parsed!");
			} else {
				log.error("what the heck? " + beginTime + " " + endTime);
			}
		} else {
			log.info("No continuous date range specified!");
		}


		if (null == end) {
			end = now;
		}
		if (null == begin) {
			begin = end.minus(Period.months(1));
		}
		if (begin.isBefore(end)) {
			result = new DateRange(begin, end);
		} else {
			log.info("Begin time isn't before end! " + begin + " to " + end);
		}

		return result;
	}

	public static DateRange createRegularDiscreteDateRange(DateRange continuousConfig, String every, int acceptPeriod, Period prefill, DateTimeZone dtz) {
		DateRange result = continuousConfig;

		DateTime begin = continuousConfig.getBegin();
		DateTime end = continuousConfig.getEnd();
		Period everyPeriod = null;
		
		List<DateTime> timesteps = new ArrayList<DateTime>();
		timesteps.add(begin);
		timesteps.add(end);

		DateTime timeOffset = null;
		Period periodOffset = null;

		//Try Period
		try {
			periodOffset = Period.parse(every);
		} catch (Exception e) {
			log.debug("Could not parse Period " + every + ", trying Time.");
		}
		if (null == periodOffset) {
			//Try Relative Time
			try {
				timeOffset = ISODateTimeFormat.tTimeNoMillis().parseDateTime(every).withZone(dtz);
			} catch (Exception e) {
				log.debug("Could not parse Time " + every + ", trying DateTime");
			}
			if (null == timeOffset) {
				log.info("Could not parse " + every + ", defaulting to Midnight");
				timeOffset = DateTime.now(dtz).withTimeAtStartOfDay();
			}

			timesteps = createTimesteps(begin, timeOffset, end);
			everyPeriod = Period.hours(24);
		} else {
			timesteps = createTimesteps(begin, periodOffset, end);
			everyPeriod = periodOffset;
		}

		result = new DateRange(timesteps, Period.hours(acceptPeriod), prefill, everyPeriod);
		return result;
	}

	protected static List<DateTime> createTimesteps(DateTime begin, DateTime dailyTime, DateTime end) {
		List<DateTime> result = new ArrayList<DateTime>();

		DateTime seedDT = begin.withTime(dailyTime.getHourOfDay(), dailyTime.getMinuteOfHour(), dailyTime.getSecondOfMinute(), dailyTime.getMillisOfSecond());

		boolean allFilled = false;
		for (int i = 0; !allFilled; i++) {
			DateTime currDT = seedDT.plusDays(i);
			if (currDT.compareTo(end) <= 0) {
				result.add(currDT);
			} else {
				allFilled = true;
			}
		}

		return result;
	}

	protected static List<DateTime> createTimesteps(DateTime begin, Period every, DateTime end) {
		List<DateTime> result = new ArrayList<DateTime>();

		DateTime seedDT = begin;
		result.add(seedDT);

		boolean allFilled = false;
		for (int i = 0; !allFilled; i++) {
			DateTime currDT = seedDT.plus(every);
			if (currDT.compareTo(end) <= 0) {
				result.add(currDT);
				seedDT = currDT;
			} else {
				allFilled = true;
			}
		}

		return result;
	}
}
