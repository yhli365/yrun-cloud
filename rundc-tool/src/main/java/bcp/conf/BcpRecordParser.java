package bcp.conf;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yanhong Lee
 * 
 */
public class BcpRecordParser {
	private static final Logger log = LoggerFactory
			.getLogger(BcpRecordParser.class);

	public static final int COLIDX_OFFSET = 0;
	public static final int COLIDX_LEN = 1;

	public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

	protected String bcpName;
	protected Map<String, BcpColumnConfig> bccMap;
	protected List<BcpColumnConfig> bccList;

	protected int maxColIndex;
	protected int[][] vpos;
	protected byte[] vbuf;
	protected Configuration conf;

	private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void init(Configuration conf, String bcpName) throws IOException {
		init(conf, bcpName, true);
	}

	public void init(Configuration conf, String bcpName, boolean init)
			throws IOException {
		this.conf = conf;
		this.bcpName = BcpFmtParser.bcpName(bcpName);
		if (init) {
			init(conf);
		}
	}

	public String getBcpName() {
		return this.bcpName;
	}

	protected void init(Configuration conf) throws IOException {
		BcpFmtParser cfgParser = BcpFmtParser.getParser(conf);
		bccList = cfgParser.getBcpColumnConfig(bcpName, false);
		log.info("Init: bcp=" + bcpName + ", colList(" + bccList.size() + ")="
				+ bccList);
		bccMap = new HashMap<String, BcpColumnConfig>();
		for (BcpColumnConfig bcc : bccList) {
			bccMap.put(bcc.name, bcc);
		}

		String cols = "";
		if (conf != null) {
			cols = conf.get("def." + bcpName);
		}
		if (StringUtils.isNotEmpty(cols)) {
			log.info("def#bcp=" + bcpName + ", cols=" + cols);
			List<BcpColumnConfig> bccListNew = new ArrayList<BcpColumnConfig>();
			HashMap<String, BcpColumnConfig> bccMapNew = new HashMap<String, BcpColumnConfig>();
			String[] arr = cols.split(",");
			for (int i = 0; i < arr.length; i++) {
				String colName = BcpFmtParser.columnName(arr[i]);
				BcpColumnConfig bcc = bccMap.get(colName);
				if (bcc == null) {
					bcc = new BcpColumnConfig();
					bcc.name = colName;
					log.info("newBcpColumn#bcp=" + bcpName + ", colName="
							+ colName);
				}
				bcc.colIndex = i;
				bccMapNew.put(bcc.name, bcc);
				bccListNew.add(bcc);
			}
			bccList = bccListNew;
			bccMap = bccMapNew;
			log.info("ReInit: bcp=" + bcpName + ", colList(" + bccList.size()
					+ ")=" + bccList);
		}

		maxColIndex = bccList.size() - 1;
		vpos = new int[bccList.size()][];
		for (int i = 0; i < vpos.length; i++) {
			vpos[i] = new int[2];
		}
	}

	public BcpColumnConfig getColumnConfig(String colName) throws IOException {
		colName = colName.trim().toLowerCase();
		BcpColumnConfig bcc = bccMap.get(colName);
		if (bcc == null) {
			throw new IOException(" column[" + colName + "] of bcp[" + bcpName
					+ "] is not configured.");
		}
		return bcc;
	}

	public int[][] getPositions() {
		return vpos;
	}

	public int getValueIndex(String colName) throws IOException {
		colName = BcpFmtParser.columnName(colName);
		BcpColumnConfig bcc = bccMap.get(colName);
		if (bcc == null) {
			throw new IOException(" column[" + colName + "] of bcp[" + bcpName
					+ "] is not configured.");
		}
		log.info("getValueIndex# bcp=" + bcpName + ", colName=" + colName
				+ ", colIndex=" + bcc.colIndex);
		return bcc.colIndex;
	}

	public void parseBytes(byte[] vbuf, int start, int end) throws IOException,
			IOException {
		this.vbuf = vbuf;

		int colIndex = 0;
		int pos = start;
		int last = start;
		int[] colpos;
		while (pos < end) {
			if (vbuf[pos] == BcpFmtParser.BCP_SEPERATOR) {
				colpos = vpos[colIndex];
				colpos[COLIDX_OFFSET] = last;
				if (pos > last) {
					// 去除后空格，建议最好由数据产生方处理
					int p = pos - 1;
					while (vbuf[p] == ' ') {
						p--;
					}
					colpos[COLIDX_LEN] = p - last + 1;
				} else {
					colpos[COLIDX_LEN] = 0;
				}

				pos++;
				// 去除前空格，建议最好由数据产生方处理
				while (pos < end && vbuf[pos] == ' ') {
					pos++;
				}
				last = pos;
				colIndex++;
				if (colIndex > maxColIndex) {
					break;
				}
			} else {
				pos++;
			}
		}
		// last column
		if (colIndex == maxColIndex) {
			colpos = vpos[colIndex];
			if (pos > last) {
				// 去除后空格，建议最好由数据产生方处理
				int p = pos - 1;
				while (vbuf[p] == ' ') {
					p--;
				}
				colpos[COLIDX_OFFSET] = last;
				colpos[COLIDX_LEN] = p - last + 1;
			}
		} else if (colIndex > maxColIndex) {
			throw new IOException(
					"Parse bcp error: bcp columns size is invalid: bcp="
							+ bcpName + ", required=" + (maxColIndex + 1)
							+ ", actual=" + (colIndex + 1) + "+");
		} else {
			throw new IOException(
					"Parse bcp error: bcp columns size is invalid: bcp="
							+ bcpName + ", required=" + (maxColIndex + 1)
							+ ", actual=" + (colIndex + 1));
		}
	}

	public String getString(int valueIndex) {
		int[] colpos = vpos[valueIndex];
		if (colpos[COLIDX_LEN] == 0) {
			return null;
		}
		return new String(vbuf, colpos[COLIDX_OFFSET], colpos[COLIDX_LEN],
				CHARSET_UTF8);
	}

	public int getTimeSeconds(int valueIndex) throws IOException {
		int[] colpos = vpos[valueIndex];
		try {
			Date d = dateFormat.parse(new String(vbuf, colpos[COLIDX_OFFSET],
					colpos[COLIDX_LEN], CHARSET_UTF8));
			return (int) (d.getTime() / 1000);
		} catch (ParseException e) {
			throw new IOException("timestamp type format error, info["
					+ bcpName + "," + valueIndex + "]: ", e);
		}
	}

	public long getLong(int valueIndex) throws IOException {
		try {
			int[] colpos = vpos[valueIndex];
			return Long.parseLong(new String(vbuf, colpos[COLIDX_OFFSET],
					colpos[COLIDX_LEN], CHARSET_UTF8));
		} catch (NumberFormatException e) {
			throw new IOException("long type format error, info[" + bcpName
					+ "," + valueIndex + "]: ", e);
		}
	}

	public int getInt(int valueIndex) throws IOException {
		try {
			int[] colpos = vpos[valueIndex];
			return Integer.parseInt(new String(vbuf, colpos[COLIDX_OFFSET],
					colpos[COLIDX_LEN], CHARSET_UTF8));
		} catch (NumberFormatException e) {
			throw new IOException("int type format error, info[" + bcpName
					+ "," + valueIndex + "]: ", e);
		}
	}

	public static BcpRecordParser getParser(Configuration conf, String bcpName)
			throws IOException {
		BcpRecordParser parser = new BcpRecordParser();
		parser.init(conf, bcpName);
		return parser;
	}

}
