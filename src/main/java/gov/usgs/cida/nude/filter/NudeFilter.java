package gov.usgs.cida.nude.filter;

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

}
