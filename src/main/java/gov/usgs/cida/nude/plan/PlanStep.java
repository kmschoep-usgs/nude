package gov.usgs.cida.nude.plan;

import gov.usgs.cida.nude.column.ColumnGrouping;
import java.sql.ResultSet;

/**
 *
 * @author dmsibley
 */
public interface PlanStep {
	
	public ResultSet runStep(ResultSet input);
	public ColumnGrouping getExpectedColumns();
	
}
