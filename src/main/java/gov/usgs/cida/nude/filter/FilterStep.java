package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.plan.PlanStep;
import java.sql.ResultSet;

/**
 *
 * @author dmsibley
 */
public class FilterStep implements PlanStep {
	public final NudeFilter filter;
	
	public FilterStep(NudeFilter filter) {
		this.filter = filter;
	}
	
	@Override
	public ResultSet runStep(ResultSet input) {
		return this.filter.filter(input);
	}

	@Override
	public ColumnGrouping getExpectedColumns() {
		return this.filter.getOutputColumns();
	}
	
}
