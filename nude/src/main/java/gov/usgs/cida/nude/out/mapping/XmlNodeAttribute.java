package gov.usgs.cida.nude.out.mapping;

public class XmlNodeAttribute{
	public String name;
	public String value;
	public int depth;
	public boolean isContentDefinedElement;
	public String dynamicValueColumn;

	public XmlNodeAttribute(String name, String value, int depth, boolean isContentDefinedElement, String dynamicValueColumnName) {
		this.name = name;
		this.value = value;
		this.depth = depth;
		this.isContentDefinedElement = isContentDefinedElement;
		this.dynamicValueColumn = dynamicValueColumnName;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ ");
		sb.append("name:'");
		sb.append(this.name);
		sb.append("', ");
		sb.append("value:'");
		sb.append(this.value);
		sb.append("', depth:'");
		sb.append(this.depth);
		sb.append("'");

		sb.append("}");
		return sb.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (null != obj && obj instanceof XmlNodeAttribute) {
			XmlNodeAttribute att = (XmlNodeAttribute) obj;
			if (this.name.equals(att.name)) {
				if (this.isContentDefinedElement && att.isContentDefinedElement) {
					if (this.value.equals(att.value)) {
						return true;
					}
				} else {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.name.hashCode() + ((this.isContentDefinedElement)?this.value.hashCode():0);
	}
}