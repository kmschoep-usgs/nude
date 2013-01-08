package gov.usgs.cida.nude.plan;

import gov.usgs.cida.nude.column.ColumnGrouping;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author dmsibley
 */
public class Plan implements Iterable<PlanStep> {
	protected List<PlanStep> steps;

	public Plan() {
		this(null);
	}

	public Plan(List<PlanStep> steps) {
		List<PlanStep> s = steps;
		
		if (null == s) {
			s = new ArrayList<PlanStep>();
		}
		
		this.steps = Collections.unmodifiableList(s);
	}

	public ColumnGrouping getExpectedColumns() {
		return this.steps.get(this.steps.size() - 1).getExpectedColumns();
	}
	
	@Override
	public Iterator<PlanStep> iterator() {
		return this.steps.iterator();
	}
	
	public static ResultSet runPlan(Plan plan) {
		ResultSet result = null;
		for(PlanStep step : plan) {
			result = step.runStep(result);
		}
		return result;
	}
}
