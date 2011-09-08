package examples.ida;

import gov.usgs.cida.nude.gel.GelStack;
import gov.usgs.cida.nude.overseer.Overseer;

import java.io.Writer;
import java.sql.ResultSet;
import java.util.List;

public class IdaOverseer extends Overseer {
	
	protected GelStack gelledInput;
	protected List<ResultSet> inputs;
	
	public IdaOverseer() {
		this.gelledInput = new GelStack();
		// Configure the input gelstack
		
	}
	
	@Override
	public void addInput(ResultSet in) {
		this.inputs.add(in);
	}

	@Override
	public void dispatch(Writer out) {
		// configure the outGelStack
		
		// run the inputs through the GelStack
		
		// pass gelled input to the Connectors.
		
		// Get the ResultSets from the Connectors.
		
		// GelStack the ResultSets
		
		// pass the output to the dispatcher
		
	}

}
