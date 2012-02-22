package gov.usgs.cida.nude.out;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.spec.jsl.mapping.ColumnMapping;
import gov.usgs.cida.spec.jsl.mapping.NodeAttribute;
import gov.usgs.webservices.framework.basic.Transformer;
import gov.usgs.webservices.framework.transformer.InsertHeaderRowTransformer;

import java.sql.ResultSet;

import javax.xml.stream.XMLStreamReader;

public class TableResponse {

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
		return new TableXmlReader(this.rs, this.getDocTag(), this.getRowTag(), new NodeAttribute[] {new NodeAttribute("rowCount", this.getFullRowCount(), 0, true, null)}, null);
	}
	
	public XMLStreamReader makeEmptyXMLReader() {
		return new EmptyTableXmlReader(this.rs, this.getDocTag(), this.getRowTag(), new NodeAttribute[] {new NodeAttribute("rowCount", "0", 0, true, null)}, null);
	}
	
	public XMLStreamReader makeXMLReaderWithEmptyHeaderRow() {
		XMLStreamReader coreReader = this.makeXMLReader();
		Transformer transformer = new InsertHeaderRowTransformer(this.getDocTag(), this.getRowTag());
		return transformer.transform(coreReader, this.makeEmptyXMLReader());
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
