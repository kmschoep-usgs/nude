package gov.usgs.cida.nude.time;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.plan.PlanStep;
import java.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class TimeFilteringPlanStep implements PlanStep {
	private static final Logger log = LoggerFactory.getLogger(TimeFilteringPlanStep.class);

	protected final ColumnGrouping colGroup;
	protected final DateRange timeRange;

	public TimeFilteringPlanStep(ColumnGrouping colGroup, DateRange timeRange) {
		this.colGroup = colGroup;
		this.timeRange = timeRange;
	}

	@Override
	public ResultSet runStep(ResultSet input) {
		ResultSet result = null;

		if (null != input) {
			result = new TimeFilteringResultSet(input, timeRange);
		}

		return result;
	}

	@Override
	public ColumnGrouping getExpectedColumns() {
		return this.colGroup;
	}
}
