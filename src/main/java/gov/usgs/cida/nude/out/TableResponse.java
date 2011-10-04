package gov.usgs.cida.nude.out;

import gov.usgs.cida.nude.resultset.CGResultSet;
import gov.usgs.cida.spec.jsl.mapping.NodeAttribute;

import javax.xml.stream.XMLStreamReader;

public class TableResponse {

	protected final CGResultSet rs;
	protected final String docTag;
	protected final String rowTag;
	protected final String fullRowCount;
	
	public TableResponse(CGResultSet rset) {
		this.rs = rset;
		this.docTag = "success";
		this.rowTag = "data";
		this.fullRowCount = "-1";
	}
	
	public XMLStreamReader makeXMLReader() {
		return new CGXmlReader(rs, this.getDocTag(), this.getRowTag(), new NodeAttribute[] {new NodeAttribute("rowCount", this.getFullRowCount(), 0, false, null)}, null);
	}

	public String getDocTag() {
		return this.docTag;
	}

	public String getRowTag() {
		return this.rowTag;
	}
	
	public String getFullRowCount() {
		return this.fullRowCount;
	}
	
	
}
