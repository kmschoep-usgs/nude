/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.column.ColumnGrouping;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author dmsibley
 */
public class NudeFilterBuilder {
	List<FilterStage> filterStages;
	ColumnGrouping initialCols;
	
	public NudeFilterBuilder(ColumnGrouping initialColumns) {
		this.initialCols = initialColumns;
		this.filterStages = new LinkedList<FilterStage>();
	}
	
	public NudeFilterBuilder addFilterStage(FilterStage stage) {
		this.filterStages.add(stage);
		return this;
	}
	
	public ColumnGrouping getCurrOutCols() {
		ColumnGrouping result = null;
		if (0 < this.filterStages.size()) {
			result = this.filterStages.get(this.filterStages.size() - 1).getOutputColumns();
		} else {
			result = this.initialCols;
		}
		return result;
	}
	
	public NudeFilter buildFilter() {
		return new NudeFilter(this.filterStages);
	}
	
}
