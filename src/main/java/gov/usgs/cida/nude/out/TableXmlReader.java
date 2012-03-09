package gov.usgs.cida.nude.out;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.out.mapping.ColumnToXmlMapping;
import gov.usgs.cida.nude.out.mapping.XmlNodeAttribute;
import gov.usgs.webservices.framework.reader.BasicTagEvent;
import gov.usgs.webservices.framework.reader.BasicXMLStreamReader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableXmlReader extends BasicXMLStreamReader {
	private static final Logger log = LoggerFactory
			.getLogger(TableXmlReader.class);
	
	protected final String docElement;
	protected final String rowElement;
	protected final XmlNodeAttribute[] docAttributes;
	protected final XmlNodeAttribute[] rowAttributes;
	
	protected final String emptyValues;
	
	protected final ColumnToXmlMapping[] columnMappings;
	
	protected final Stack<String> elementStack = new Stack<String>();
	
	public static final boolean WRITE_EMPTY_TAGS = true;
	
	public TableXmlReader(ResultSet rset, String docElement, String rowElement, XmlNodeAttribute[] docAttributes, XmlNodeAttribute[] rowAttributes, String emptyValueString, boolean showHiddenColumns) {
		this._rset = rset;
		this.docElement = docElement;
		this.rowElement = rowElement;
		this.docAttributes = docAttributes;
		this.rowAttributes = rowAttributes;
		
		if (null == emptyValueString) {
			emptyValueString = "";
		}
		this.emptyValues = emptyValueString;
		
		this.columnMappings = ColumnToXmlMapping.getColumnMappings(ColumnGrouping.getColumnGrouping(rset), showHiddenColumns);
	}
	
	@Override
	protected void rowEndAction() {
		addCloseTag(getRowElement());
		super.rowEndAction();
	}

	@Override
	protected BasicTagEvent rowStartAction() throws SQLException {
		addOpenTag(getRowElement());
		return super.rowStartAction();
	}

	@Override
	protected void documentEndAction() {
		addCloseTag(getDocElement());
		super.documentEndAction();
	}

	@Override
	protected BasicTagEvent documentStartAction() {
		addOpenTag(getDocElement());
		return super.documentStartAction();
	}
	
	@Override
	public void addOpenTag(String tagName) {
		BasicTagEvent tagEvent = new BasicTagEvent(START_ELEMENT, tagName);
		if (tagName.equals(getDocElement()) && getDocAttributes() != null) {
			for (int attribCount = 0; attribCount < getDocAttributes().length; attribCount++){
				XmlNodeAttribute attribute = getDocAttributes()[attribCount];
				tagEvent.addAttribute(attribute.name, attribute.value);
			}
		} else if (tagName.equals(getRowElement()) && getRowAttributes() != null) {
			for (int attribCount = 0; attribCount < getRowAttributes().length; attribCount++){
				XmlNodeAttribute attribute = getDocAttributes()[attribCount];
				tagEvent.addAttribute(attribute.name, attribute.value);
			}
		}
		this.events.add(tagEvent);
	}

	/**
	 * Returns the attribute array of the passed ColumnMap at a given depth
	 * 
	 * @param columnMap
	 * @param depth
	 * @return
	 */
	protected BasicXMLStreamReader.Attribute[] getAttributeArray(ColumnToXmlMapping columnMap, int depth){
		if(columnMap.getAttribute(depth) == null) return null;
		List<BasicXMLStreamReader.Attribute> attributes = new ArrayList<BasicXMLStreamReader.Attribute>();
		
		for (XmlNodeAttribute attribute : columnMap.getAttribute(depth)){
			attributes.add(new BasicXMLStreamReader.Attribute(attribute.name, attribute.value));
		}
		
		if(attributes.size() > 0) return attributes.toArray(new BasicXMLStreamReader.Attribute[attributes.size()]);
		return null;
	}

	@Override
	protected void readRow(final ResultSet rset) throws SQLException {
		ColumnToXmlMapping[] columnMap = this.columnMappings;
		
		for(int columnMapCounter = 0; columnMapCounter < columnMap.length;columnMapCounter++){
			ColumnToXmlMapping currentCol = columnMap[columnMapCounter];
			
			if (!"".equals(currentCol.getXmlElementString())) { // && null == currentCol.getSpec() //Allows us to add columns to look up, but not to output.
				if(WRITE_EMPTY_TAGS || hasValue(rset, currentCol.getColumnName())){
					closeDanglingElements(currentCol);
					openXmlElements(currentCol);

					int columnMapDepth 				= currentCol.getDepth() - 1;
					String xmlElement 				= currentCol.getXmlElement(columnMapDepth);
					String columnName 				= currentCol.getColumnName();
					BasicXMLStreamReader.Attribute[] attributeArray 
					= getAttributeArray(currentCol, columnMapDepth);

					//change the attribute value
					for(int index = 0; attributeArray != null && attributeArray.length > index; index++) {
						if (currentCol.getAttributes()[index] != null && currentCol.getAttributes()[index].dynamicValueColumn != null) {
							attributeArray[index].value = rset.getString(currentCol.getAttributes()[index].dynamicValueColumn);
						}
					}
					writeBasicTag(rset, xmlElement, columnName, attributeArray);
					if(currentCol.getInjectXmlAt() != null) {
						checkForExtraXml(currentCol, xmlElement);
					}
					closeXmlElements(currentCol);
				}
			} 
		}
		
		//close any dangling elements left behind
		while(getElementStack().size() > 0) {
			addCloseTag(getElementStack().pop());
		}
	}

	protected void openXmlElements(ColumnToXmlMapping currentCol) {
	  for(int columnDepthCount = 0; columnDepthCount < currentCol.getDepth() - 1; columnDepthCount++){
	  	if(columnDepthCount >= getElementStack().size()){
	  		String xmlElement 	= currentCol.getXmlElement(columnDepthCount);	  		
	  		BasicXMLStreamReader.Attribute[] attributeArray = getAttributeArray(currentCol, columnDepthCount);
			addOpenTagWithAttributes(xmlElement, attributeArray);
	  		getElementStack().push(xmlElement);
	  	}
	  }
	}

	protected void closeXmlElements(ColumnToXmlMapping currentCol) {
	  for(int columnDepthCount = currentCol.getDepth() - 2; columnDepthCount > currentCol.getGroupBase(); columnDepthCount--){
	  	addCloseTag(currentCol.getXmlElement(columnDepthCount));
	  	String popped = getElementStack().pop();
	  	if(currentCol.getInjectXmlAt() != null) checkForExtraXml(currentCol, popped);
	  }
	}

	protected void checkForExtraXml(ColumnToXmlMapping currentCol, String popped) {
	  for(int injectIndex = 0; injectIndex < currentCol.getInjectXmlAt().length; injectIndex=injectIndex+3){
	  	if(popped.equals(currentCol.getInjectXmlAt()[injectIndex+2])) { 
	  		addNonNullBasicTag(currentCol.getInjectXmlAt()[injectIndex+1], currentCol.getInjectXmlAt()[injectIndex]);
	  	}
	  }
  }

	protected void closeDanglingElements(ColumnToXmlMapping currentCol) {
		while (getElementStack().size() > currentCol.getGroupBase() + 1
				|| (currentCol.getDepth() > getElementStack().size()
						&& getElementStack().size() > 0 
						&& !currentCol.getXmlElement(getElementStack().size() - 1).equals(
								getElementStack().peek())
								)) {
			addCloseTag(getElementStack().pop());
		}
	}

	protected void writeBasicTag(ResultSet rset, String tag, String column, Attribute... at) throws SQLException{
		String res = null;
		if(!hasValue(rset, column)) {
			res = this.emptyValues;
		} else {
			res = rset.getString(column);
		}
		
		if(at == null){
			addNonNullBasicTag(tag, res);
		} else {
			addNonNullBasicTag(tag, res, at);
		}
	}

	protected boolean hasValue(ResultSet rset, String column) {
		try {
			String value = null;
			if (!"".equals(column)) {
				value = rset.getString(column);
			}
			if(value==null || value.equals("")) return false;
		} catch (Exception e) {
			log.error("hasValue(rset," + column +"): " + e);
			return false;
		}
		return true;
	}

	public void addOpenTagWithAttributes(String tagName, Attribute... attributes) {
		BasicTagEvent result = new BasicTagEvent(START_ELEMENT, tagName);
		if(attributes != null){
			for (Attribute attribute: attributes) {
				if (attribute != null && !attribute.isEmpty()) {
					result.addAttribute(attribute.name, attribute.value);
				}
			}
		}
		this.events.add(result);
	}

	@Override
	public void close() throws XMLStreamException {
		// Empty block
	}

	public String getRowElement() {
		return this.rowElement;
	}

	public String getDocElement() {
		return this.docElement;
	}

	public XmlNodeAttribute[] getDocAttributes() {
		return this.docAttributes;
	}

	public XmlNodeAttribute[] getRowAttributes() {
		return this.rowAttributes;
	}
	
	public Stack<String> getElementStack() {
		return this.elementStack;
	}
}
