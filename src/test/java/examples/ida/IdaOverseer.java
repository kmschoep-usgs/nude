package examples.ida;

import examples.ida.request.IdaConnectorParams;
import gov.usgs.cida.nude.gel.GelBuilder;
import gov.usgs.cida.nude.gel.GelStack;
import gov.usgs.cida.nude.gel.GelledResultSet;
import gov.usgs.cida.nude.out.Dispatcher;
import gov.usgs.cida.nude.out.TableResponse;
import gov.usgs.cida.nude.overseer.Overseer;
import gov.usgs.cida.nude.params.OutputFormat;
import gov.usgs.cida.nude.provider.http.HttpProvider;
import gov.usgs.cida.nude.resultset.ColumnGroupedResultSet;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;
import gov.usgs.cida.nude.table.DummyColumn;
import gov.usgs.cida.spec.formatting.ReturnType;
import gov.usgs.cida.spec.out.StreamResponse;
import gov.usgs.webservices.framework.basic.FormatType;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamException;

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
	
	protected HttpProvider httpProvider;
	
	public IdaOverseer(HttpProvider httpProvider) {
		this.inputs = new ArrayList<ColumnGroupedResultSet>();
		this.gelParamsIn = new GelStack();
		// Configure the input gelstack
		GelBuilder gb = new GelBuilder(paramCols);
		this.gelParamsIn.addGel(gb.buildGel());
		this.httpProvider = httpProvider;
	}
	
	@Override
	public void addInput(ColumnGroupedResultSet in) {
		this.inputs.add(in);
	}

	@Override
	public void dispatch(Writer out) throws SQLException, XMLStreamException, IOException {
		// configure the outGelStack
		GelStack gelOut = new GelStack();
		// run the inputs through the GelStack
		GelledResultSet params = this.gelParamsIn.gel(inputs); //TODO this can only be read in once. (hmm.)
		// pass gelled input to the Connectors.
		IdaMetadataConnector imc = new IdaMetadataConnector(httpProvider);
		IdaDataConnector idc = new IdaDataConnector(httpProvider);
		
		//TODO
		
		// Get the ResultSets from the Connectors.
		List<ColumnGroupedResultSet> outputs = new ArrayList<ColumnGroupedResultSet>();
		
		// GelStack the ResultSets
		GelledResultSet results = gelOut.gel(outputs);
		// pass the output to the dispatcher
		StreamResponse outStrm = Dispatcher.buildFormattedResponse(ReturnType.xml, FormatType.XML, new TableResponse(results));
		StreamResponse.dispatch(outStrm, out);
	}

}
