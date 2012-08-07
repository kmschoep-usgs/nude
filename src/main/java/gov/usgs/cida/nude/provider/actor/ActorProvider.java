package gov.usgs.cida.nude.provider.actor;

import akka.actor.ActorSystem;
import akka.util.Duration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gov.usgs.cida.nude.provider.IProvider;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class ActorProvider implements IProvider {
	private static final Logger log = LoggerFactory.getLogger(ActorProvider.class);

	protected ActorSystem actorSystem = null;

	public ActorProvider() {
	}
	
	public ActorProvider(ActorSystem existingActorSystem) {
		this.actorSystem = existingActorSystem;
	}
	
	@Override
	public void init() {
		if (null == this.actorSystem) {
			this.actorSystem = getActorSystem();
		} else {
			log.debug("init called on full ActorProvider " + this.actorSystem.hashCode());
		}
	}
	
	public ActorSystem getSystem() {
		return this.actorSystem;
	}

	@Override
	public void destroy() {
		if (null != this.actorSystem) {
			killActorSystem(this.actorSystem);
		} else {
			log.debug("destroy called on empty ActorProvider");
		}
	}
	
	private static AtomicInteger numSystemsRunning = new AtomicInteger(0);
	
	public static ActorSystem getActorSystem() {
		ActorSystem result = null;
		
		try {
			log.trace("Creating ActorSystem");
			
			Config conf = ConfigFactory.load();
			result = ActorSystem.create("nude", conf.getConfig("nude").withFallback(conf));
			if (null != result) {
				int numSys = numSystemsRunning.incrementAndGet();
				log.info("Created ActorSystem " + result.hashCode()  + ". Currently Running: " + numSys);
			} else {
				log.error("ActorSystem was not created?!");
			}
		} catch (Exception e) {
			log.error("Exception while creating ActorSystem!", e);
		}
		
		return result;
	}
	
	public static void killActorSystem(ActorSystem actorSystem) {
		killActorSystem(actorSystem, null);
	}
	
	public static void killActorSystem(ActorSystem actorSystem, Duration timeout) {
		Duration inTime = timeout;
		if (null == inTime) {
			inTime = Duration.create(30, TimeUnit.SECONDS);
		}
		
		try {
			if (null != actorSystem) {
				String sysName = "" + actorSystem.hashCode();
				log.trace("Destroying ActorSystem " + sysName);
				actorSystem.shutdown();
				actorSystem.awaitTermination(inTime);
				int numSys = numSystemsRunning.decrementAndGet();
				log.info("Destroyed ActorSystem " + sysName + ". Currently Running: " + numSys);
			}
		} catch (Exception e) {
			log.error("Exception while destroying ActorSystem!", e);
		}
	}
	
	public static void unknownMessage(Object msg) {
		throw new IllegalArgumentException("Unknown message [" + msg + "]");
	}
}
