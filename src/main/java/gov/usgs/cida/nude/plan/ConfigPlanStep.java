/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.usgs.cida.nude.plan;

import gov.usgs.cida.nude.resultset.inmemory.IteratorWrappingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * [UNIT-TESTABLE]
 * @author dmsibley
 */
public class ConfigPlanStep implements PlanStep {

	private Iterable<TableRow> config;
	
	public ConfigPlanStep(Iterable<TableRow> config) {
		this.config = config;
	}
	
	@Override
	public ResultSet runStep(ResultSet input) {
		ResultSet result = null;
		
		result = new IteratorWrappingResultSet(config.iterator());
		
		if (null != input) {
			List<ResultSet> mux = new ArrayList<ResultSet>();
			mux.add(input);
			mux.add(result);
			
			result = new MuxResultSet(mux);
		}
		
		return result;
	}
	
}
