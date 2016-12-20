package gov.usgs.cida.nude.provider;

public interface IProvider {
	
	/**
	 * Call this before you start.
	 * @throws InterruptedException 
	 */
	public void init() throws InterruptedException;
	
	/**
	 * Always call this when you're done!
	 */
	public void destroy();
	
}
