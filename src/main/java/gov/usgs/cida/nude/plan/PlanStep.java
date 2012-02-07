package gov.usgs.cida.nude.plan;

import java.sql.ResultSet;
import java.util.List;

/**
 *
 * @author dmsibley
 */
public interface PlanStep {
	
	public ResultSet runStep(ResultSet input);
	
}
