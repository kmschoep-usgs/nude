package gov.usgs.cida.resultset;

public class CursorLocation {
	public static enum Location {
		BEFOREFIRST,
		FIRST,
		MIDDLE,
		LAST,
		AFTERLAST;
	}
	
	public Location loc;
	
	public CursorLocation() {
		this.loc = Location.BEFOREFIRST;
	}
	
	public Location getLocation() {
		return this.loc;
	}
	
	public void setLocation(Location loc) {
		this.loc = loc;
	}
}
