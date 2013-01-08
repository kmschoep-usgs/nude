package gov.usgs.cida.nude.provider;

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
