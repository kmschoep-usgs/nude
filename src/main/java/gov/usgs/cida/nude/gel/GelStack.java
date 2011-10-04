package gov.usgs.cida.nude.gel;

import gov.usgs.cida.nude.resultset.CGResultSet;
import gov.usgs.cida.nude.resultset.MuxResultSet;

import java.util.LinkedList;
import java.util.List;

public class GelStack {
	
	protected List<Gel> gels;
	
	public GelStack() {
		this.gels = new LinkedList<Gel>();
	}
	
	public void addGel(Gel buildGel) {
		this.gels.add(buildGel);
	}

	public GelledResultSet filter(List<CGResultSet> input) {
		GelledResultSet result = null;
		
		for (Gel gel : this.gels) {
			if (null == result) {
				result = new GelledResultSet(new MuxResultSet(input), gel);
			} else {
				result = new GelledResultSet(result, gel);
			}
		}
		
		return result;
	}

}
