package gov.usgs.cida.provider;

public interface IProvider {
	
	/**
	 * Call this before you start.
	 */
	public void init();
	
	/**
	 * Always call this when you're done!
	 */
	public void destroy();
	
}
