package gov.usgs.cida.nude.out;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.spec.jsl.mapping.ColumnMapping;
import gov.usgs.cida.spec.jsl.mapping.NodeAttribute;
import gov.usgs.cida.spec.resultset.JoiningResultSet;
import gov.usgs.testing.statemock.MockResultSet;
import gov.usgs.webservices.framework.reader.BasicTagEvent;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.xml.stream.XMLStreamConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated !!! probably shouldn't use this.  this doesn't really work.
 * @author dmsibley
 */
public class EmptyTableXmlReader extends TableXmlReader {

	private static final Logger log = LoggerFactory.getLogger(EmptyTableXmlReader.class);
	private ColumnMapping[] colMap;

	public EmptyTableXmlReader(ResultSet rset, String docElement, String rowElement, NodeAttribute[] docAttributes, NodeAttribute[] rowAttributes) {
		super(rset, docElement, rowElement, docAttributes, rowAttributes, null, false);
//		MockResultSet rs = new MockResultSet();
//		rs.setColumnNames("one", "two");
//		rs.addRow("1", "2");
//		try {
//			this._rset = new JoiningResultSet(rs, "one");
//		} catch (SQLException e) {
//			this._rset = rset;
//			log.error("Something went wrong creating EmptyTableXmlReader", e);
//		}
		
		this.colMap = ColumnGrouping.getColumnMappings(ColumnGrouping.getColumnGrouping(rset), false);
	}

	@Override
	protected void readRow(ResultSet rset) throws SQLException {
		ColumnMapping[] columnMap = this.colMap;

		readRowAux(rset, columnMap);

		//close any dangling elements left behind
		while (getElementStack().size() > 0) {
			addCloseTag(getElementStack().pop());
		}
	}

	@Override
	protected void readRowAux(ResultSet rset, ColumnMapping[] columnMap) throws SQLException {
		for (int columnMapIndex = 0; columnMapIndex < columnMap.length; columnMapIndex++) {
			ColumnMapping currentCol = columnMap[columnMapIndex];
			if (!"".equals(currentCol.getXmlElementString()) && null == currentCol.getSpec()) { //Allows us to add columns to look up, but not to output.
				closeDanglingElements(currentCol);
				openXmlElements(currentCol);

				String columnMapXMLElement = currentCol.getXmlElement(columnMap[columnMapIndex].getDepth() - 1);
				Attribute[] attributes = getAttributeArray(currentCol, currentCol.getDepth() - 1);
				writeBasicTag(columnMapXMLElement, attributes);
				if (currentCol.getInjectXmlAt() != null) {
					checkForExtraXml(currentCol, columnMapXMLElement);
				}
				closeXmlElements(currentCol);
			} else if (null != currentCol.getSpec()) {
				closeDanglingElements(currentCol);
				readRowAux(rset, currentCol.getSpec().getColumns());
				closeDanglingElements(currentCol);
			}
		}
	}

	protected void writeBasicTag(String tag, Attribute... attributes) {
		if (attributes == null) {
			addNonNullBasicTag(tag, "");
		} else {
			addNonNullBasicTag(tag, "", attributes);
		}
	}

	@Override
	protected void documentEndAction() {
		this.events.add(new BasicTagEvent(XMLStreamConstants.END_DOCUMENT));
	}

	@Override
	protected BasicTagEvent documentStartAction() {
		return new BasicTagEvent(XMLStreamConstants.START_DOCUMENT);
	}
}
