/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.usgs.cida.nude.plan;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.IteratorWrappingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

/**
 * [UNIT-TESTABLE]
 * @author dmsibley
 */
public class ConfigPlanStep implements PlanStep {

	private Iterable<TableRow> config;
	private ColumnGrouping cg;
	
	public ConfigPlanStep(Iterable<TableRow> config) {
		this.config = config;
		
		List<ColumnGrouping> colGroups = new ArrayList<ColumnGrouping>();
		for (TableRow row : config) {
			colGroups.add(row.getColumns());
		}
		this.cg = ColumnGrouping.join(colGroups);
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

	@Override
	public ColumnGrouping getExpectedColumns() {
		return this.cg;
	}

}
