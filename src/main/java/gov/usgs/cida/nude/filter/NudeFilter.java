package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

public class NudeFilter {
	
	protected List<FilterStage> gels;
	
	public NudeFilter() {
		this.gels = new LinkedList<FilterStage>();
	}
	
	public void addGel(FilterStage buildGel) {
		this.gels.add(buildGel);
	}

	public FilteredResultSet filter(List<ResultSet> input) {
		FilteredResultSet result = null;
		
		for (FilterStage gel : this.gels) {
			if (null == result) {
				result = new FilteredResultSet(new MuxResultSet(input), gel);
			} else {
				result = new FilteredResultSet(result, gel);
			}
		}
		
		return result;
	}

}
