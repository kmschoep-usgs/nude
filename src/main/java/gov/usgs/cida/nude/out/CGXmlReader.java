package gov.usgs.cida.nude.out;

import gov.usgs.cida.nude.table.ColumnGrouping;
import gov.usgs.cida.spec.jsl.mapping.ColumnMapping;
import gov.usgs.cida.spec.jsl.mapping.NodeAttribute;
import gov.usgs.cida.spec.resultset.JoiningResultSet;
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

public class CGXmlReader extends BasicXMLStreamReader {
	private static final Logger log = LoggerFactory
			.getLogger(CGXmlReader.class);
	
	protected final String docElement;
	protected final String rowElement;
	protected final NodeAttribute[] docAttributes;
	protected final NodeAttribute[] rowAttributes;
	
	protected final ColumnMapping[] columnMappings;
	
	protected final Stack<String> elementStack = new Stack<String>();
	
	public CGXmlReader(ResultSet rset, String docElement, String rowElement, NodeAttribute[] docAttributes, NodeAttribute[] rowAttributes) {
		this._rset = rset;
		this.docElement = docElement;
		this.rowElement = rowElement;
		this.docAttributes = docAttributes;
		this.rowAttributes = rowAttributes;
		
		this.columnMappings = ColumnGrouping.getColumnMappings(ColumnGrouping.getColumnGrouping(rset));
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
				NodeAttribute attribute = getDocAttributes()[attribCount];
				tagEvent.addAttribute(attribute.name, attribute.value);
			}
		} else if (tagName.equals(getRowElement()) && getRowAttributes() != null) {
			for (int attribCount = 0; attribCount < getRowAttributes().length; attribCount++){
				NodeAttribute attribute = getDocAttributes()[attribCount];
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
	protected BasicXMLStreamReader.Attribute[] getAttributeArray(ColumnMapping columnMap, int depth){
		if(columnMap.getAttribute(depth) == null) return null;
		List<BasicXMLStreamReader.Attribute> attributes = new ArrayList<BasicXMLStreamReader.Attribute>();
		
		for (NodeAttribute attribute : columnMap.getAttribute(depth)){
			attributes.add(new BasicXMLStreamReader.Attribute(attribute.name, attribute.value));
		}
		
		if(attributes.size() > 0) return attributes.toArray(new BasicXMLStreamReader.Attribute[attributes.size()]);
		return null;
	}

	@Override
	protected void readRow(final ResultSet rset) throws SQLException {
		ColumnMapping[] columnMap = this.columnMappings;
		
		readRowAux(rset, columnMap);
		
		//close any dangling elements left behind
		while(getElementStack().size() > 0) {
			addCloseTag(getElementStack().pop());
		}
	}

	protected void readRowAux(final ResultSet rset, ColumnMapping[] columnMap)
			throws SQLException {
		for(int columnMapCounter = 0; columnMapCounter < columnMap.length;columnMapCounter++){
			ColumnMapping currentCol = columnMap[columnMapCounter];
			
			if (!"".equals(currentCol.getXmlElementString()) && null == currentCol.getSpec()) { //Allows us to add columns to look up, but not to output.
				if(hasValue(rset, currentCol.getColumnName())){
					closeDanglingElements(currentCol);
					openXmlElements(currentCol);

					int columnMapDepth 				= currentCol.getDepth() - 1;
					String xmlElement 				= currentCol.getXmlElement(columnMapDepth);
					String columnName 				= currentCol.getColumnName();
					BasicXMLStreamReader.Attribute[] attributeArray 
					= getAttributeArray(currentCol, columnMapDepth);

					//Iterative portion: all depends on countColumn!!
					if (null != currentCol.getCountColumn()) {
						int linkCount = 0;
						try {
							linkCount = rset.getInt(currentCol.getCountColumn());
						} catch (Exception e) {
							e.printStackTrace();
							linkCount = 0;
						}
						if (linkCount >= 1) {
							do {
								//change the attribute value
								for(int index = 0; attributeArray != null && attributeArray.length > index; index++) {
									if (currentCol.getAttributes()[index] != null && currentCol.getAttributes()[index].dynamicValueColumn != null) {
										attributeArray[index].value = rset.getString(currentCol.getAttributes()[index].dynamicValueColumn);
									}
								}

								//write the tag
								writeBasicTag(rset, xmlElement, columnName, attributeArray);
								if(currentCol.getInjectXmlAt() != null) {
									checkForExtraXml(currentCol, xmlElement);
								}

								linkCount--;
							} while (linkCount > 0 && rset.next()); //moves it to the next row
						}
						closeXmlElements(currentCol);
					} else {
						// normal

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
			} else if (null != currentCol.getSpec()) {
				while (((JoiningResultSet) rset).isSynched(currentCol.getSpec().getColumns()[1].getColumnName())) {
					readRowAux(rset, currentCol.getSpec().getColumns());
					closeDanglingElements(currentCol);
					((JoiningResultSet) rset).next(currentCol.getSpec().getColumns()[1].getColumnName());
				}
			}
		}
	}

	protected void openXmlElements(ColumnMapping currentCol) {
	  for(int columnDepthCount = 0; columnDepthCount < currentCol.getDepth() - 1; columnDepthCount++){
	  	if(columnDepthCount >= getElementStack().size()){
	  		String xmlElement 	= currentCol.getXmlElement(columnDepthCount);	  		
	  		BasicXMLStreamReader.Attribute[] attributeArray = getAttributeArray(currentCol, columnDepthCount);
			addOpenTagWithAttributes(xmlElement, attributeArray);
	  		getElementStack().push(xmlElement);
	  	}
	  }
	}

	protected void closeXmlElements(ColumnMapping currentCol) {
	  for(int columnDepthCount = currentCol.getDepth() - 2; columnDepthCount > currentCol.getGroupBase(); columnDepthCount--){
	  	addCloseTag(currentCol.getXmlElement(columnDepthCount));
	  	String popped = getElementStack().pop();
	  	if(currentCol.getInjectXmlAt() != null) checkForExtraXml(currentCol, popped);
	  }
	}

	protected void checkForExtraXml(ColumnMapping currentCol, String popped) {
	  for(int injectIndex = 0; injectIndex < currentCol.getInjectXmlAt().length; injectIndex=injectIndex+3){
	  	if(popped.equals(currentCol.getInjectXmlAt()[injectIndex+2])) { 
	  		addNonNullBasicTag(currentCol.getInjectXmlAt()[injectIndex+1], currentCol.getInjectXmlAt()[injectIndex]);
	  	}
	  }
  }

	protected void closeDanglingElements(ColumnMapping currentCol) {
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
		if(!hasValue(rset, column)) return;
		
		String res = rset.getString(column);
		if(at == null){
			addNonNullBasicTag(tag, res);
		} else {
			addNonNullBasicTag(tag, res, at);
		}
	}

	/**
	 * Tests whether or not the resultset contains a value at the column 
	 * specified by the passed String parameter
	 * 
	 * @param rset
	 * @param column
	 * @return true if column contains a value, false if not
	 * @throws SQLException Exhausted Resultset usually means that your mods aren't coming back with the correct number of links OR they are ordered incorrectly.
	 */
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

	public NodeAttribute[] getDocAttributes() {
		return this.docAttributes;
	}

	public NodeAttribute[] getRowAttributes() {
		return this.rowAttributes;
	}
	
	public Stack<String> getElementStack() {
		return this.elementStack;
	}
}
