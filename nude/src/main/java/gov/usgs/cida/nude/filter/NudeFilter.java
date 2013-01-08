package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.column.ColumnGrouping;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

public class NudeFilter {
	
	protected List<FilterStage> filterStages;
	
	public NudeFilter() {
		this.filterStages = new LinkedList<FilterStage>();
	}
	
	public NudeFilter(List<FilterStage> filterStages) {
		this.filterStages = filterStages;
	}

	public FilteredResultSet filter(ResultSet input) {
		FilteredResultSet result = null;
		
		for (FilterStage filterStage : this.filterStages) {
			if (null == result) {
				result = new FilteredResultSet(input, filterStage);
			} else {
				result = new FilteredResultSet(result, filterStage);
			}
		}
		
		return result;
	}
	
	public ColumnGrouping getOutputColumns() {
		return this.filterStages.get(this.filterStages.size() - 1).getOutputColumns();
	}
}
