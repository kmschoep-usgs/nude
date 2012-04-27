package gov.usgs.cida.nude.out.mapping;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import java.util.ArrayList;
import java.util.List;

public class ColumnToXmlMapping {
	private final String columnName;
	private final String orderByParam;
	private final boolean isOrderDesc;
	private final XmlNodeAttribute[] attributes;
	private final String xmlElementString;

	private final int depth;
	private final int groupBase;
	private final String[] injectXmlAt;
	private final String aliasName;

	public ColumnToXmlMapping(String columnName, String elementName) {
		this(columnName, elementName, false, elementName, null, null, null, null);
	}

	public ColumnToXmlMapping(String columnName, String orderByParam, String elementName){
		this(columnName, orderByParam, false, elementName, null, null, null, null);
	}

	public ColumnToXmlMapping(String columnName, String orderByParam, boolean isOrderDesc, String elementName){
		this(columnName, orderByParam, isOrderDesc, elementName, null, null, null, null);
	}

	public ColumnToXmlMapping(String columnName, String orderByParam, String elementName, XmlNodeAttribute[] attributes){
		this(columnName, orderByParam, false, elementName, attributes, null, null, null);
	}

	public ColumnToXmlMapping(String columnName, String orderByParam, String elementName, XmlNodeAttribute[] attributes, String groupBase){
		this(columnName, orderByParam, false, elementName, attributes, groupBase, null, null);
	}

	public ColumnToXmlMapping(String columnName, String orderByParam, String elementName, XmlNodeAttribute[] attributes, String[] injectXmlAt){
		this(columnName, orderByParam, false, elementName, attributes, null, injectXmlAt, null);
	}

	public ColumnToXmlMapping(String columnName, String orderByParam, String elementName, String groupBase){
		this(columnName, orderByParam, false, elementName, null, groupBase, null, null);
	}

	public ColumnToXmlMapping(String columnName, String orderByParam, String elementName, String groupBase, String[] injectXmlAt){
		this(columnName, orderByParam, false, elementName, null, groupBase, injectXmlAt, null);
	}

	public ColumnToXmlMapping(String columnName, String orderByParam, boolean isOrderDesc, String elementName, XmlNodeAttribute[] attributes, String groupBase, String[] injectXmlAt, String aliasName){
		this.columnName 		= columnName;
		this.orderByParam 		= orderByParam;
		this.isOrderDesc 		= isOrderDesc;
		this.xmlElementString 	= (null == elementName)?"":elementName;
		this.depth 				= 1;
		this.attributes 		= attributes;
		this.aliasName 			= aliasName;
		this.groupBase			= -1;

		this.injectXmlAt = injectXmlAt;

		ArrayList<XmlNodeAttribute> contDefAtts = new ArrayList<XmlNodeAttribute>();
		if (attributes != null) {
			for (int i = 0; i < attributes.length; i++) {
				if (attributes[i].isContentDefinedElement) {
					contDefAtts.add(attributes[i]);
				}
			}
		}

	}

	public String getXmlElement(int depthParam){
		return getXmlElementString();
	}

	public ArrayList<XmlNodeAttribute> getAttribute(int depthParam){
		if(getAttributes() == null) return null;
		ArrayList<XmlNodeAttribute> atts = new ArrayList<XmlNodeAttribute>();
		for(int attributesIndex = 0; attributesIndex < getAttributes().length; attributesIndex++){
			if(getAttributes()[attributesIndex].depth==depthParam) atts.add(getAttributes()[attributesIndex]);
		}
		return atts;
	}

	public XmlNodeAttribute[] getAttributes(){
		return this.attributes;
	}

	public String[] getInjectXmlArray() {
		return this.getInjectXmlAt();
	}

	public String getXmlElementString() {
		return this.xmlElementString;
	}

	public String getColumnName() {
		return this.columnName;
	}

	public String getOrderByParam() {
		return this.orderByParam;
	}

	public boolean isOrderDesc() {
		return this.isOrderDesc;
	}

	public int getDepth() {
		return this.depth;
	}

	public int getGroupBase() {
		return this.groupBase;
	}

	public String[] getInjectXmlAt() {
		return this.injectXmlAt;
	}

	public String getAliasName() {
		return this.aliasName;
	}

	public static int indexOfColumnByOrderByParam(String orderby, ColumnToXmlMapping[] columnMap) {
		for (int columnMapIndex = 0; columnMapIndex < columnMap.length; columnMapIndex++) {
			if(orderby.equals(columnMap[columnMapIndex].getOrderByParam())) {
				return columnMapIndex; 
			} 
		}
		return -1;
	}
	
	public static ColumnToXmlMapping[] getColumnMappings(ColumnGrouping colGroup, boolean mapHiddenColumns) {
		ColumnToXmlMapping[] result = new ColumnToXmlMapping[0];
		
		if (null != colGroup) {
			List<ColumnToXmlMapping> cm = new ArrayList<ColumnToXmlMapping>();
			for (Column col : colGroup) {
				if (col.isDisplayable() || mapHiddenColumns) {
					cm.add(new ColumnToXmlMapping(col.getName(), col.getName()));
				}
			}
			result = cm.toArray(result);
		}
		
		return result;
	}
}
