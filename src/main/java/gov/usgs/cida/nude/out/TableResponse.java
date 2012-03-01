package gov.usgs.cida.nude.out;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.spec.jsl.mapping.ColumnMapping;
import gov.usgs.cida.spec.jsl.mapping.NodeAttribute;

import java.sql.ResultSet;

import javax.xml.stream.XMLStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableResponse {
	private static final Logger log = LoggerFactory.getLogger(TableResponse.class);

	protected final ResultSet rs;
	protected final String docTag;
	protected final String rowTag;
	protected final String fullRowCount;
	
	public TableResponse(ResultSet rset) {
		this.rs = rset;
		this.docTag = "success";
		this.rowTag = "data";
		this.fullRowCount = "-1";
	}
	
	public XMLStreamReader makeXMLReader() {
		XMLStreamReader result = new EmptyTableXmlReader(null, this.getDocTag(), this.getRowTag(), new NodeAttribute[] {new NodeAttribute("rowCount", "0", 0, true, null)}, null);
		
		try {
			if (null != this.rs && !this.rs.isClosed()) {
				result = new TableXmlReader(this.rs, this.getDocTag(), this.getRowTag(), new NodeAttribute[] {new NodeAttribute("rowCount", this.getFullRowCount(), 0, true, null)}, null);
				if (!result.hasNext()) {
					result = new EmptyTableXmlReader(this.rs, this.getDocTag(), this.getRowTag(), new NodeAttribute[] {new NodeAttribute("rowCount", "0", 0, true, null)}, null);
				}
			}
		} catch (Exception ex) {
			log.error("Could not make TableXmlReader", ex);
		}
		
		return result;
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
	
	public ColumnMapping[] getColumns() {
		return ColumnGrouping.getColumnMappings(ColumnGrouping.getColumnGrouping(this.rs));
	}
}
