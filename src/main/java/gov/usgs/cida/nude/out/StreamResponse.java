package gov.usgs.cida.nude.out;

import gov.usgs.webservices.framework.basic.MimeType;
import gov.usgs.webservices.framework.formatter.IFormatter;

import java.io.IOException;
import java.io.Writer;

import javax.xml.stream.XMLStreamReader;

public class StreamResponse {
	public IFormatter formatter;
	public XMLStreamReader reader;
	
	public MimeType outputType;
	public boolean isCacheable = true;
	public boolean isFileDownload = false;
	
	public String literalResponse;

	public StreamResponse() {
		this.formatter = null;
		this.reader = null;
		
		this.outputType = MimeType.XML;
		this.isCacheable = true;
		this.isFileDownload = false;
		
		this.literalResponse = null;
	}
	
	public StreamResponse(IFormatter formatter, XMLStreamReader reader) {
		this.formatter = formatter;
		this.reader = reader;
		
		this.outputType = MimeType.XML;
		this.isCacheable = true;
		this.isFileDownload = false;
		
		this.literalResponse = null;
	}

	public IFormatter getFormatter() {
		return this.formatter;
	}

	public void setFormatter(IFormatter formatter) {
		this.formatter = formatter;
	}

	public XMLStreamReader getReader() {
		return this.reader;
	}

	public void setReader(XMLStreamReader reader) {
		this.reader = reader;
	}
	
	public boolean isCacheable() {
		return this.isCacheable;
	}

	public void setCacheable(boolean isCacheable) {
		this.isCacheable = isCacheable;
	}
	
	public boolean isFileDownload() {
		return this.isFileDownload;
	}

	public void setFileDownload(boolean isFileDownload) {
		this.isFileDownload = isFileDownload;
	}
	
	public MimeType getOutputType() {
		return this.outputType;
	}

	public void setOutputType(MimeType outputType) {
		this.outputType = outputType;
	}
	
	public String getLiteralResponse() {
		return this.literalResponse;
	}
	
	public void setLiteralResponse(String literalResponse) {
		this.literalResponse = literalResponse;
	}
	
	public static void dispatch(StreamResponse in, Writer out) throws IOException {
		if (null != in.getLiteralResponse()) {
			out.write(in.getLiteralResponse());
			out.flush();
		} else if (null != in.getReader() && null != in.getFormatter()) {
			in.getFormatter().dispatch(in.getReader(), new TimeFlushingWriter(out));
		}
	}

}
