package gov.usgs.cida.nude.out;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.out.mapping.ColumnToXmlMapping;
import gov.usgs.cida.nude.out.mapping.XmlNodeAttribute;

import java.sql.ResultSet;
import java.util.Arrays;

import javax.xml.stream.XMLStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableResponse {
	private static final Logger log = LoggerFactory.getLogger(TableResponse.class);

	protected final ResultSet rs;
	protected final String docTag;
	protected final String rowTag;
	protected final String fullRowCount;
	protected final ColumnToXmlMapping[] columnMappings;
	
	protected final String emptyValues;
	protected final boolean showHiddenColumns;
	
	public TableResponse(ResultSet rset) {
		this(rset, null, null, false);
	}
	
	public TableResponse(ResultSet rset, String emptyValueString, boolean showHiddenColumns) {
		this(rset, emptyValueString, null, showHiddenColumns);
	}
	
	public TableResponse(ResultSet rset, String emptyValueString, ColumnToXmlMapping[] outColumnMapping, boolean showHiddenColumns) {
		this.rs = rset;
		this.docTag = "success";
		this.rowTag = "data";
		this.fullRowCount = "-1";
		
		this.emptyValues = emptyValueString;
		this.showHiddenColumns = showHiddenColumns;
		
		ColumnToXmlMapping[] cm = new ColumnToXmlMapping[0];
		if (null == outColumnMapping || 1 > outColumnMapping.length) {
			cm = ColumnToXmlMapping.getColumnMappings(ColumnGrouping.getColumnGrouping(this.rs), this.showHiddenColumns);
		} else {
			cm = Arrays.copyOf(outColumnMapping, outColumnMapping.length);
		}
		
		columnMappings = cm;
	}
	
	public XMLStreamReader makeXMLReader() {
		XMLStreamReader result = null;
		
		try {
			if (null != this.rs) {
				boolean isClosed = false;
				try {
					isClosed = this.rs.isClosed();
				} catch (AbstractMethodError t) {
					log.trace("Cannot tell if ResultSet is closed.");
				}
				if (!isClosed) {
					result = new TableXmlReader(this.rs, this.columnMappings, this.getDocTag(), this.getRowTag(), new XmlNodeAttribute[] {new XmlNodeAttribute("rowCount", this.getFullRowCount(), 0, true, null)}, null, this.emptyValues, this.showHiddenColumns);
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
	
	public ColumnToXmlMapping[] getColumns() {
		return Arrays.copyOf(this.columnMappings, this.columnMappings.length);
	}
}
