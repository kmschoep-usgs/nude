package gov.usgs.cida.nude.time;

import java.sql.ResultSet;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.plan.PlanStep;

/**
 *
 * @author dmsibley
 */
public class TimeFilteringPlanStep implements PlanStep {
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
