package gov.usgs.cida.nude.out;

import gov.usgs.cida.spec.formatting.ReturnType;
import gov.usgs.cida.spec.jsl.mapping.ColumnMapping;
import gov.usgs.cida.spec.jsl.mapping.NodeAttribute;
import gov.usgs.cida.spec.out.StreamResponse;
import gov.usgs.webservices.framework.basic.FormatType;
import gov.usgs.webservices.framework.formatter.DataFlatteningFormatter;
import gov.usgs.webservices.framework.formatter.XMLPassThroughFormatter;
import gov.usgs.webservices.framework.transformer.ElementToAttributeTransformer;

import java.io.IOException;
import java.sql.SQLException;

import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dispatcher {
	private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
	
	/**
	 * Returns a reader and a formatter ready to be dispatched.
	 * @param returnType
	 * @param outputType
	 * @param isJsonP
	 * @param rssBaseUrl
	 * @param specResponse
	 * @return non-null StreamResponse
	 * @throws SQLException
	 * @throws XMLStreamException
	 * @throws IOException
	 * @throws InvalidServiceException
	 */
	public static StreamResponse buildFormattedResponse(
			ReturnType returnType,
			FormatType outputType, 
			TableResponse tableResponse) throws SQLException, XMLStreamException, IOException {
		
		StreamResponse sr = new StreamResponse();
		sr.setReader(tableResponse.makeXMLReader());

		String litResp = null;
		
		switch (outputType) {
		case CSV: // fall through
		case EXCEL: // fall through
		case TAB:
//			sr.setCacheable(false); // We don't want to cache files.
//			sr.setFileDownload(true);

			// Empty results check.
			if (!sr.getReader().hasNext()) {
				sr.setReader(tableResponse.makeEmptyXMLReader());
			} else { 
				sr.setReader(tableResponse.makeXMLReaderWithEmptyHeaderRow());
			}

			{ // Configure the formatter
				DataFlatteningFormatter df = new DataFlatteningFormatter(outputType);
				ElementToAttributeTransformer transformer = new ElementToAttributeTransformer();

				df.setRowElementName(tableResponse.getRowTag());
				// use column map to add content-defined elements
				for (ColumnMapping col : tableResponse.getColumns()) {
					NodeAttribute[] attributes = col.getAttributes();
					if (attributes != null)
						for (int attributeIndex = 0; attributeIndex < attributes.length; attributeIndex++) {
							NodeAttribute attribute = attributes[attributeIndex];
							if (attribute.isContentDefinedElement) {
								df.addContentDefinedElement(col
										.getXmlElement(attribute.depth),
										attribute.name);
							}
						}

					String[] extraXmlToInject = col.getInjectXmlArray();
					if (extraXmlToInject != null) {
						for (int extraXmlIndex = 1; extraXmlIndex < extraXmlToInject.length; extraXmlIndex += 3) {
							String xmlElement = col.getXmlElement(0);
							String extraXml = extraXmlToInject[extraXmlIndex];
							transformer.addTargetElement(xmlElement, extraXml);
							df.addContentDefinedElement(xmlElement, extraXml);
						}
					}
				}
				sr.setFormatter(df);
				sr.setReader(transformer.transform(sr.getReader()));
			}

			break;
//		case JSON:
//			sr.setFormatter(Services.getJSONFormatter(specResponse.responseSpec)); // , isJsonP
//			if (!sr.getReader().hasNext()) {
//				litResp = Services.getJSONEmptyResult(specResponse.responseSpec, specResponse.fullRowCount); // , isJsonP
//				log.debug("Writing JSON empty result:" + litResp);
//			}
//			break;
		case XML:
		default:
			sr.setFormatter(new XMLPassThroughFormatter());
			if (!sr.getReader().hasNext()) {
				litResp = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<"+ tableResponse.getDocTag()+" rowCount=\"0\"></"+ tableResponse.getDocTag()+">";
				log.debug("Writing XML based empty result:" + litResp);
			}
			break;
		}
		
		sr.setLiteralResponse(litResp);
		
		return sr;
	}
}
