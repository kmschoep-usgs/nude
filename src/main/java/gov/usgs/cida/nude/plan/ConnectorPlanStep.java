package gov.usgs.cida.nude.plan;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.Await;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.jsr166y.ThreadLocalRandom;
import akka.pattern.Patterns;
import akka.routing.RoundRobinRouter;
import akka.util.Duration;
import akka.util.Timeout;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.NudeFilterBuilder;
import gov.usgs.cida.nude.filter.transform.ColumnAlias;
import gov.usgs.cida.nude.provider.IProvider;
import gov.usgs.cida.nude.provider.Provider;
import gov.usgs.cida.nude.provider.actor.ActorProvider;
import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;
import gov.usgs.cida.nude.resultset.inmemory.ResultSetCloner;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [UNIT-TESTABLE]?
 * @author dmsibley
 */
public class ConnectorPlanStep implements PlanStep {
	private static final Logger log = LoggerFactory.getLogger(ConnectorPlanStep.class);
	private final List<IConnector> connectors;
	private final ActorSystem sys;
	
	private static final int MAX_RUNNING_CONNECTORS = 256;
	private static final Duration CONNECTOR_TIMEOUT = Duration.create(6, TimeUnit.MINUTES);

	public ConnectorPlanStep(Map<Provider, IProvider> providers, List<IConnector> connectors) {
		this.sys = ((ActorProvider) providers.get(Provider.ACTOR)).getSystem();
		this.connectors = connectors;
	}
	
	@Override
	public ResultSet runStep(ResultSet input) {
		ResultSet result = null;
		log.trace("Starting ConnectorPlanStep");
		
		ActorRef runner = this.sys.actorOf(new Props(ConnectorRunner.class)
				.withRouter(new RoundRobinRouter(MAX_RUNNING_CONNECTORS)));
		
		List<Future<ResultSet>> responses = new ArrayList<Future<ResultSet>>();
		
		log.trace("Starting mapping requests");
		Multimap<String, IConnector> stmts = ArrayListMultimap.<String, IConnector>create();
		ResultSetCloner rsc = new ResultSetCloner(input, connectors.size());
		for (IConnector connector : connectors) {
			connector.addInput(rsc.cloneResultSet());
			String st = connector.getStatement();
//			if (null != st) {
			stmts.put(st, connector);
//			} else {
//				log.error("Can't get statement? fill inputs could not work?");
//			}
		}
		
		for (String stmt : stmts.keySet()) {
			Collection<IConnector> conn = stmts.get(stmt);
			Future<Object> resp = null;
			Future<ResultSet> respRS = null;
			for (IConnector connector : conn) {
				if (null == respRS) {
					resp = Patterns.ask(runner, connector, new Timeout(CONNECTOR_TIMEOUT));
					respRS = resp.map(new Mapper<Object, ResultSet>() {
						@Override
						public ResultSet apply(Object t1) {
							return (ResultSet) t1;
						}
					});
				} else {
					respRS = respRS.map(new ExpandResultsMapper(connector.getExpectedColumns()));
				}
			}
			responses.add(respRS);
		}
		log.trace("Ended mapping requests");
		
		Future<Iterable<ResultSet>> futureResults = Futures.sequence(responses, sys.dispatcher());
		Future<ResultSet> futureResult = futureResults.map(new Mapper<Iterable<ResultSet>, ResultSet>() {
			@Override
			public ResultSet apply(Iterable<ResultSet> arg0) {
				return new MuxResultSet(arg0);
			}
		});
		
		log.trace("Starting Wait");
		try {
			result = Await.result(futureResult, CONNECTOR_TIMEOUT);
		} catch (Exception ex) {
			log.error("Waiting for result ended in exception!", ex);
		}
		log.trace("Ended Wait");
		
		log.trace("Ending ConnectorPlanStep");
		return result;
	}
	
	public static class ConnectorRunner extends UntypedActor {

		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof IConnector) {
				IConnector connector = (IConnector) arg0;
				
				long randSleepTime = ThreadLocalRandom.current().nextLong();
				randSleepTime = Math.abs(randSleepTime) % 500;
				Thread.sleep(randSleepTime);
				
				ResultSet result = connector.getResultSet();
				
				getSender().tell(result, null);
			} else {
				unknownMessage(arg0);
			}
		}
		
		protected static void unknownMessage(Object msg) {
			throw new IllegalArgumentException("Unknown message [" + msg + "]");
		}
		
	}
	
	public static class ExpandResultsMapper extends Mapper<ResultSet, ResultSet> {
		protected ColumnGrouping cg;
		
		public ExpandResultsMapper(ColumnGrouping cg) {
			this.cg = cg;
		}
		
		@Override
		public ResultSet apply(ResultSet inRs) {
			ResultSet result = inRs;
			
			ColumnGrouping inCg = ColumnGrouping.getColumnGrouping(inRs);
			List<Column> inC = inCg.getColumns();
			List<Column> addC = cg.getColumns();
			
			NudeFilterBuilder nfb = new NudeFilterBuilder(inCg);
			for (int i = 0; i < addC.size() && i < inC.size(); i++) {
				Column in = inC.get(i);
				Column add = addC.get(i);
				if (!in.equals(add)) {
//					log.trace(in + " transform to " + add);
					nfb.addFilterStage(new FilterStageBuilder(nfb.getCurrOutCols())
							.addTransform(add, new ColumnAlias(in))
							.buildFilterStage());
				} else {
//					log.trace(in + " no-op " + add);
				}
			}
			
			return nfb.buildFilter().filter(result);
		}
		
	}
	
}
