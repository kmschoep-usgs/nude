package examples.ida;

import gov.usgs.cida.nude.gel.GelStack;
import gov.usgs.cida.nude.overseer.Overseer;

import java.io.Writer;
import java.sql.ResultSet;

public class IdaOverseer extends Overseer {
	
	protected GelStack gelledInput;
	
	public IdaOverseer() {
		this.gelledInput = new GelStack();
		// Configure the input gelstack
		
	}
	
	@Override
	public void addInput(ResultSet in) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dispatch(Writer out) {
		// configure the outGelStack
		
		// GelStack the inputs
		
		// pass gelled inputs to the connectors.
		
		// Get the resultsets from the connectors.
		
		// GelStack the resultsets
		
		// pass the output to the dispatcher
		
	}

}
