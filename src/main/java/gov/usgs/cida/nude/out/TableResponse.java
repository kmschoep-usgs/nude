package gov.usgs.cida.nude.out;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.out.mapping.ColumnToXmlMapping;
import gov.usgs.cida.nude.out.mapping.XmlNodeAttribute;

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
	
	protected final String emptyValues;
	protected final boolean showHiddenColumns;
	
	public TableResponse(ResultSet rset) {
		this(rset, null, false);
	}
	
	public TableResponse(ResultSet rset, String emptyValueString, boolean showHiddenColumns) {
		this.rs = rset;
		this.docTag = "success";
		this.rowTag = "data";
		this.fullRowCount = "-1";
		
		this.emptyValues = emptyValueString;
		this.showHiddenColumns = showHiddenColumns;
	}
	
	public XMLStreamReader makeXMLReader() {
		XMLStreamReader result = null;
		
		try {
			if (null != this.rs && !this.rs.isClosed()) {
				result = new TableXmlReader(this.rs, this.getDocTag(), this.getRowTag(), new XmlNodeAttribute[] {new XmlNodeAttribute("rowCount", this.getFullRowCount(), 0, true, null)}, null, this.emptyValues, this.showHiddenColumns);
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
	
	public ColumnToXmlMapping[] getColumns() {
		return ColumnToXmlMapping.getColumnMappings(ColumnGrouping.getColumnGrouping(this.rs), this.showHiddenColumns);
	}
}
