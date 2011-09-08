package examples.ida;

import examples.ida.request.IdaConnectorParams;
import gov.usgs.cida.nude.gel.GelBuilder;
import gov.usgs.cida.nude.gel.GelStack;
import gov.usgs.cida.nude.gel.GelledResultSet;
import gov.usgs.cida.nude.overseer.Overseer;
import gov.usgs.cida.nude.params.OutputFormat;
import gov.usgs.cida.nude.resultset.ColumnGroupedResultSet;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;
import gov.usgs.cida.nude.table.DummyColumn;

import java.io.Writer;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IdaOverseer extends Overseer {
	
	protected static ColumnGrouping paramCols;
	
	static {
		List<Column> cols = new ArrayList<Column>();
		cols.add(DummyColumn.DUMMY);
		cols.addAll(Arrays.asList(OutputFormat.values()));
		cols.addAll(Arrays.asList(IdaConnectorParams.values()));
		paramCols = new ColumnGrouping(DummyColumn.DUMMY, cols);
	}
	
	protected GelStack gelParamsIn;
	protected List<ColumnGroupedResultSet> inputs;
	
	public IdaOverseer() {
		this.inputs = new ArrayList<ColumnGroupedResultSet>();
		this.gelParamsIn = new GelStack();
		// Configure the input gelstack
		GelBuilder gb = new GelBuilder(paramCols);
		this.gelParamsIn.addGel(gb.buildGel());
	}
	
	@Override
	public void addInput(ColumnGroupedResultSet in) {
		this.inputs.add(in);
	}

	@Override
	public void dispatch(Writer out) {
		// configure the outGelStack
		GelStack gelOut;
		// run the inputs through the GelStack
		GelledResultSet params = this.gelParamsIn.gel(inputs);
		// pass gelled input to the Connectors.
		
		// Get the ResultSets from the Connectors.
		
		// GelStack the ResultSets
		
		// pass the output to the dispatcher
		
	}

}
