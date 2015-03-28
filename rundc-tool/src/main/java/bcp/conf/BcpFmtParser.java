package bcp.conf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yanhong Lee
 * 
 */
public class BcpFmtParser {
	private final static Logger log = LoggerFactory
			.getLogger(BcpFmtParser.class);

	/**
	 * BCP字段分隔符.
	 */
	public static final byte BCP_SEPERATOR = '\t';

	private Map<String, List<BcpColumnConfig>> columnConfigMap = new HashMap<String, List<BcpColumnConfig>>();

	/**
	 * Lazy parse.
	 */
	private List<BcpColumnConfig> headList = new ArrayList<BcpColumnConfig>();
	private List<BcpColumnConfig> tailList = new ArrayList<BcpColumnConfig>();
	private List<BcpColumnConfig> tailpubList = new ArrayList<BcpColumnConfig>();
	private Map<String, Element> bcpCfgMap = new HashMap<String, Element>();

	private URL cfgFile;

	private static BcpFmtParser fmtParser;

	public static BcpFmtParser getParser(Configuration conf) throws IOException {
		if (fmtParser == null) {
			fmtParser = new BcpFmtParser(conf);
		}
		return fmtParser;
	}

	public BcpFmtParser(Configuration conf) throws IOException {
		loadConfigFile(conf);
	}

	public static final String columnName(String colName) {
		return colName.trim().toLowerCase();
	}

	private void loadConfigFile(Configuration conf) throws IOException {
		try {
			String bcpConf = conf.get("cfg.bcpfmt", "conf/bcpfmt.xml");
			ClassLoader classLoader = Thread.currentThread()
					.getContextClassLoader();
			if (classLoader == null) {
				classLoader = getClass().getClassLoader();
			}
			cfgFile = classLoader.getResource(bcpConf);
			if (cfgFile == null) {
				throw new FileNotFoundException("Config file is not found: "
						+ bcpConf);
			}

			SAXBuilder builder = new SAXBuilder();
			Document doc = builder.build(cfgFile);
			Element root = doc.getRootElement();
			List<Element> list = root.getChildren();

			for (Element e : list) {
				// System.out.println(e.getName());
				String name = e.getName();
				if ("head".equalsIgnoreCase(name)) {
					// 公共字段:头部
					parseCols(e, headList);
				} else if ("tail".equalsIgnoreCase(name)) {
					// 公共字段:尾部
					parseCols(e, tailList);
				} else if ("tailpub".equalsIgnoreCase(name)) {
					// 公共字段:尾部
					parseCols(e, tailpubList);
				} else if ("center".equalsIgnoreCase(name)) {
					List<Element> bcpelist = e.getChildren();
					for (Element eb : bcpelist) {
						String key = eb.getAttributeValue("name").toLowerCase();
						key = key.replace("_", "").trim();
						if (bcpCfgMap.containsKey(key)) {
							throw new IOException(cfgFile
									+ "# bcp define is repeated: bcp=" + key
									+ ", file=" + cfgFile);
						}
						bcpCfgMap.put(key, eb);
					}
				}
			}
			log.info("parse OK.");
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException("Load config file failed: " + cfgFile, e);
		}
	}

	private void copyBcpColumnConfigList(List<BcpColumnConfig> srcList,
			List<BcpColumnConfig> dstList) {
		for (BcpColumnConfig bcc : srcList) {
			BcpColumnConfig bcc2 = new BcpColumnConfig();
			bcc2.name = bcc.name;
			bcc2.type = bcc.type;
			bcc2.len = bcc.len;
			bcc2.isNull = bcc.isNull;
			bcc2.extend = bcc.extend;
			bcc2.cfg = bcc.cfg;
			dstList.add(bcc2);
		}
	}

	private void parseCols(Element e, List<BcpColumnConfig> columnList) {
		List<Element> list = e.getChildren();
		for (Element ec : list) {
			String name = ec.getName();
			if ("column".equalsIgnoreCase(name)) {
				BcpColumnConfig bcc = new BcpColumnConfig();
				bcc.cfg = ec;
				bcc.name = ec.getAttributeValue("name").trim().toLowerCase();
				bcc.type = ec.getAttributeValue("type").trim().toLowerCase();
				String s = ec.getAttributeValue("len").trim().toLowerCase();
				if (s != null && s.length() > 0) {
					bcc.len = Integer.parseInt(s);
				}
				bcc.isNull = "true".equalsIgnoreCase(ec
						.getAttributeValue("isnull").trim().toLowerCase());

				if (ec.getAttributeValue("extend") != null) {
					bcc.extend = "true".equalsIgnoreCase(ec
							.getAttributeValue("extend").trim().toLowerCase());
				}
				columnList.add(bcc);
			}
		}
	}

	public int getBcpCode(String name) {
		Element e = bcpCfgMap.get(name);
		String code = e.getAttributeValue("code").trim();
		return Integer.parseInt(code);
	}

	public Set<String> getSupportedBcps() {
		return bcpCfgMap.keySet();
	}

	/**
	 * @return the columnConfigIndexMap
	 * @throws BcpConfigurationException
	 */
	public List<BcpColumnConfig> getBcpColumnConfig(String name)
			throws IOException {
		return this.getBcpColumnConfig(name, true);
	}

	public List<BcpColumnConfig> getBcpColumnConfig(String name, boolean bExtend)
			throws IOException {
		String key = name + "." + String.valueOf(bExtend);
		List<BcpColumnConfig> result = columnConfigMap.get(key);
		if (result == null) {
			Element e = bcpCfgMap.get(name);
			if (e == null) {
				throw new IOException("bcp[" + name + "] is not define: "
						+ cfgFile);
			}
			List<BcpColumnConfig> list = new ArrayList<BcpColumnConfig>();
			if ("true".equalsIgnoreCase(e.getAttributeValue("hashead"))) {
				copyBcpColumnConfigList(headList, list);
			}
			parseCols(e, list);
			if ("true".equalsIgnoreCase(e.getAttributeValue("hastail"))) {
				copyBcpColumnConfigList(tailList, list);
			}
			copyBcpColumnConfigList(tailpubList, list);

			result = new ArrayList<BcpColumnConfig>();
			int index = 0;
			for (BcpColumnConfig bcc : list) {
				if (!bcc.extend) {
					bcc.colIndex = index++;
					result.add(bcc);
				}
			}
			if (bExtend) {
				for (BcpColumnConfig bcc : list) {
					if (bcc.extend) {
						result.add(bcc);
					}
				}
			}
			columnConfigMap.put(key, result);
		}
		return result;
	}

	public static String bcpName(String bcp) {
		bcp = bcp.trim().toLowerCase().replaceAll("_", "");
		return bcp;
	}
}
