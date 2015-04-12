package bcp.conf;

import org.jdom2.Element;

/**
 * @author Yanhong Lee
 * 
 */
public class BcpColumnConfig {

	public String name;

	public String type;

	public int len;

	public boolean isNull;

	public boolean extend;

	public int colIndex = Integer.MIN_VALUE;

	public Element cfg;

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(n=").append(name);
		if (colIndex != Integer.MIN_VALUE) {
			sb.append(", idx=").append(colIndex);
		}
		if (extend) {
			sb.append(", ext");
		}
		sb.append(")");
		return sb.toString();
	}

}
