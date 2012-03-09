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
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.provider.IProvider;
import gov.usgs.cida.nude.provider.Provider;
import gov.usgs.cida.nude.provider.actor.ActorProvider;
import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;
import gov.usgs.cida.nude.resultset.inmemory.ResultSetCloner;
import java.sql.ResultSet;
import java.util.ArrayList;
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
	
	private static final int MAX_RUNNING_CONNECTORS = 128;
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
		
		log.trace("Starting Dispatching");
		ResultSetCloner rsc = new ResultSetCloner(input);
		for (IConnector connector : connectors) {
			connector.addInput(rsc.cloneResultSet());
			Future<Object> resp = Patterns.ask(runner, connector, new Timeout(CONNECTOR_TIMEOUT));
			Future<ResultSet> respRS = resp.map(new Mapper<Object, ResultSet>() {
				@Override
				public ResultSet apply(Object t1) {
					return (ResultSet) t1;
				}
			});
			responses.add(respRS);
		}
		log.trace("Ended Dispatching");
		
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
				getContext().stop(getSelf());
			} else {
				unknownMessage(arg0);
			}
		}
		
		protected static void unknownMessage(Object msg) {
			throw new IllegalArgumentException("Unknown message [" + msg + "]");
		}
		
	}
	
}
