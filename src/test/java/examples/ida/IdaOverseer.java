package examples.ida;

import examples.ida.request.IdaConnectorParams;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.DummyColumn;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.connector.http.HttpConnector;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.FilteredResultSet;
import gov.usgs.cida.nude.filter.NudeFilter;
import gov.usgs.cida.nude.filter.NudeFilterBuilder;
import gov.usgs.cida.nude.out.Dispatcher;
import gov.usgs.cida.nude.out.TableResponse;
import gov.usgs.cida.nude.overseer.Overseer;
import gov.usgs.cida.nude.params.OutputFormat;
import gov.usgs.cida.nude.provider.http.HttpProvider;
import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;
import gov.usgs.cida.nude.resultset.inmemory.ResultSetCloner;
import gov.usgs.cida.spec.formatting.ReturnType;
import gov.usgs.cida.spec.out.StreamResponse;
import gov.usgs.webservices.framework.basic.FormatType;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdaOverseer extends Overseer {

	private static final Logger log = LoggerFactory.getLogger(IdaOverseer.class);
	protected static NudeFilter filterParamsIn;
	/**
	 * Use like this:<br>
	 * <code>connectors.get("something").getConstructor(HttpProvider.class).newInstance(httpProvider);</code>
	 */
	protected static Map<String, Class<? extends HttpConnector>> connectors;

	static {
		connectors = buildAvailableConnectors();

		filterParamsIn = buildInputFilter();
	}
	protected HttpProvider httpProvider;

	public IdaOverseer(HttpProvider httpProvider) {
		this.httpProvider = httpProvider;
	}

	@Override
	public void dispatch(Writer out) throws SQLException, XMLStreamException, IOException {
		// run the inputs through the Filter
		FilteredResultSet params = filterParamsIn.filter(new MuxResultSet(inputs));

		OverseerRequest req = configureRequest(params, this.httpProvider);
		if (null != req) {
			List<? extends IConnector> requestedConnectors = req.reqConnectors;
			NudeFilter outFilter = req.outputFilter;

			// Get the ResultSets from the Connectors.
			List<ResultSet> outputs = queryConnectors(requestedConnectors);

			// Filter the ResultSets
			FilteredResultSet results = outFilter.filter(new MuxResultSet(outputs));
			// pass the output to the dispatcher
			StreamResponse outStrm = Dispatcher.buildFormattedResponse(ReturnType.xml, FormatType.XML, new TableResponse(results));
			StreamResponse.dispatch(outStrm, out);
		} else {
			//TODO dispatch error
		}
	}

	public static OverseerRequest configureRequest(
			FilteredResultSet params, HttpProvider provider) {
		OverseerRequest result = null;

		try {
			ResultSetCloner rsc = new ResultSetCloner(params);

			List<HttpConnector> cons = getConnectors(rsc, provider);

			NudeFilter outFilter = getOutputFilter(cons);

			result = new OverseerRequest(cons, outFilter);
		} catch (Exception e) {
			log.debug("Something Weird Happened!", e);
		}

		return result;
	}

	public static NudeFilter getOutputFilter(List<HttpConnector> connectors) {
		NudeFilter result = new NudeFilter();
		
		
		
		return result;
	}
	
	public static List<HttpConnector> getConnectors(ResultSetCloner inputCloner, HttpProvider provider) {
		List<HttpConnector> result = new ArrayList<HttpConnector>();
		for (Class<? extends HttpConnector> con : connectors.values()) {
			HttpConnector httpCon = null;
			try {
				httpCon = con.getConstructor(HttpProvider.class).newInstance(inputCloner, provider);
			} catch (Exception ex) {
				log.debug("Exception while constructing possible HttpConnectors", ex);
			}
			if (null != httpCon) {
				httpCon.addInput(inputCloner.cloneResultSet());
				if (httpCon.isValidInput()) {
					result.add(httpCon);
				}
			}
		}
		return result;
	}

	public static class OverseerRequest {

		public final List<? extends IConnector> reqConnectors;
		public final NudeFilter outputFilter;

		public OverseerRequest(List<? extends IConnector> reqConnectors, NudeFilter outputFilter) {
			this.reqConnectors = reqConnectors;
			this.outputFilter = outputFilter;
		}
	}

	public static List<ResultSet> queryConnectors(List<? extends IConnector> reqConnectors) {
		List<ResultSet> result = new ArrayList<ResultSet>();

		if (null != reqConnectors) {
			for (IConnector con : reqConnectors) {
				if (con.isReady()) {
					ResultSet resp = con.getResultSet();
					if (null != resp) {
						result.add(resp);
					}
				} else {
					throw new RuntimeException(con.getClass().getName() + " is not ready.");
				}
			}
		}

		return result;
	}

	public static Map<String, Class<? extends HttpConnector>> buildAvailableConnectors() {
		Map<String, Class<? extends HttpConnector>> result = null;

		result = new HashMap<String, Class<? extends HttpConnector>>();
		result.put("METADATA_REQUEST", IdaMetadataConnector.class);
		result.put("DATA_REQUEST", IdaDataConnector.class);

		return result;
	}

	public static ColumnGrouping buildExpectedUserInput() {
		ColumnGrouping result = null;

		List<Column> cols = new ArrayList<Column>();
		cols.add(DummyColumn.JOIN);
		cols.addAll(Arrays.asList(OutputFormat.values()));
		cols.addAll(Arrays.asList(IdaConnectorParams.values()));
		result = new ColumnGrouping(DummyColumn.JOIN, cols);

		return result;
	}

	public static NudeFilter buildInputFilter() {
		NudeFilter result = null;

		NudeFilterBuilder nfb = new NudeFilterBuilder(buildExpectedUserInput());
		result = nfb
				.addFilterStage(
					new FilterStageBuilder(nfb.getCurrOutCols())
						.buildFilterStage())
				.buildFilter();

		//TODO add more transforms for configuration


		return result;
	}
}
