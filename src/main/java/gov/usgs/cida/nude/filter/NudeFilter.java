package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

public class NudeFilter {
	
	protected List<FilterStage> filterStages;
	
	public NudeFilter() {
		this.filterStages = new LinkedList<FilterStage>();
	}
	
	public void addFilterStage(FilterStage stage) {
		this.filterStages.add(stage);
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
