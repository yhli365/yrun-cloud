package bcp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据bcpfmt.xml配置，生成Hive表创建语句. <br/>
 * 命令: <br/>
 * 参数: <br/>
 * -Dbcps=im,game -指定要生成的bcp表，以逗号分隔。默认全部协议<br/>
 * -Ddb=bcp -指定数据库名称。默认为空<br/>
 * -DbcpConf=conf/bcpfmt.xml -指定bcp配置文件。默认conf/bcpfmt.xml<br/>
 * -Dcolumn.bcpid=bcpid -指定第一列BCP标识名称。默认为bcpid，为NULL时不创建<br/>
 * -Dstore.dir=/jz -外部表存储路径.<br/>
 * -Dstored.as=sequencefile -外部表存储文件格式.<br/>
 * -Dpartitioned.by=dt -分区字段，为NULL时不创建.<br/>
 * 
 * @author yhli
 * 
 */
public class HiveTableCreateTool extends Configured implements Tool {

	private final static Logger log = LoggerFactory
			.getLogger(HiveTableCreateTool.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new HiveTableCreateTool(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		File destFile = new File("bcphive.sql");
		if (args.length > 0) {
			destFile = new File(args[0]);
		}
		File destParent = destFile.getParentFile();
		if (destParent != null && !destParent.exists()) {
			destParent.mkdirs();
		}

		String bcpConf = conf.get("bcpConf", "conf/bcpfmt.xml");
		URL url;
		File f = new File(bcpConf);
		if (f.exists() && f.isFile()) {
			url = f.toURI().toURL();
		} else {
			ClassLoader classLoader = Thread.currentThread()
					.getContextClassLoader();
			if (classLoader == null) {
				classLoader = getClass().getClassLoader();
			}
			url = classLoader.getResource(bcpConf);
		}
		if (url == null) {
			throw new FileNotFoundException("Config file is not found: "
					+ bcpConf);
		}
		log.info("Load config file: " + url.getPath());

		Map<String, HiveTable> bcpMap = new HashMap<String, HiveTable>();
		try {
			SAXBuilder builder = new SAXBuilder();
			Document doc = builder.build(url);
			Element root = doc.getRootElement();
			List<Element> list = root.getChildren();

			List<HiveColumn> headList = new ArrayList<HiveColumn>();
			List<HiveColumn> tailList = new ArrayList<HiveColumn>();
			for (Element e : list) {
				String name = e.getName();
				if ("head".equalsIgnoreCase(name)) {
					// 公共字段:头部
					parseCols(e, headList);
				} else if ("tail".equalsIgnoreCase(name)) {
					// 公共字段:尾部
					parseCols(e, tailList);
				} else if ("center".equalsIgnoreCase(name)) {
					List<Element> bcpelist = e.getChildren();
					for (Element eb : bcpelist) {
						String key = eb.getAttributeValue("name").toLowerCase();
						HiveTable ht = new HiveTable();
						ht.name = key;
						ht.pe = eb;
						if ("true".equalsIgnoreCase(eb.getAttributeValue(
								"hashead").trim())) {
							ht.headList = headList;
						}
						if ("true".equalsIgnoreCase(eb.getAttributeValue(
								"hastail").trim())) {
							ht.tailList = tailList;
						}
						bcpMap.put(key, ht);
					}
				}
			}
		} catch (Exception e) {
			throw new IOException("Parse config file failed: " + url.getPath(),
					e);
		}

		List<String> bcps = new ArrayList<String>();
		Collection<String> cs = conf.getStringCollection("bcps");
		if (!cs.isEmpty()) {
			bcps.addAll(cs);
		} else {
			bcps.addAll(bcpMap.keySet());
		}
		Collections.sort(bcps);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(destFile), "utf-8"));
		try {
			String db = conf.get("db", "");
			if (StringUtils.isNotEmpty(db)) {
				bw.append("create database if not exists ").append(db)
						.append(";");
				bw.append("use ").append(db).append(";");
				bw.newLine();
			}

			bw.newLine();
			for (String bcp : bcps) {
				bw.append("drop table if exists ").append(bcp).append(";");
				bw.newLine();
			}

			String bcpid = conf.get("column.bcpid", "bcpid");
			for (String bcp : bcps) {
				StringBuilder sb = new StringBuilder();
				sb.append("create external table if not exists ");
				sb.append(db).append(bcp).append(" (\n");
				if (!"NULL".equalsIgnoreCase(bcpid)) {
					sb.append("  ").append(bcpid)
							.append(" string comment 'bcp id',\n");
				}
				HiveTable ht = bcpMap.get(bcp);
				ht.check();

				int seq = 0;
				String seqPrefix;

				if (ht.headList != null) {
					seqPrefix = "H";
					for (HiveColumn hc : ht.headList) {
						sb.append("  ").append(hc.name).append(" ")
								.append(hc.type);
						sb.append(" COMMENT '").append(seqPrefix).append("#")
								.append(String.valueOf(++seq)).append(" ")
								.append(hc.code).append("',\n");
					}
				}

				if (ht.privateList != null) {
					seqPrefix = "P";
					for (HiveColumn hc : ht.privateList) {
						sb.append("  ").append(hc.name).append(" ")
								.append(hc.type);
						sb.append(" COMMENT '").append(seqPrefix).append("#")
								.append(String.valueOf(++seq)).append(" ")
								.append(hc.code).append("',\n");
					}
				}
				if (ht.tailList != null) {
					seqPrefix = "T";
					for (HiveColumn hc : ht.tailList) {
						sb.append("  ").append(hc.name).append(" ")
								.append(hc.type);
						sb.append(" COMMENT '").append(seqPrefix).append("#")
								.append(String.valueOf(++seq)).append(" ")
								.append(hc.code).append("',\n");
					}
				}
				sb.delete(sb.length() - 2, sb.length());
				sb.append(")");

				String str = conf.get("partitioned.by", "dt");
				if (!"NULL".equalsIgnoreCase(str)) {
					sb.append("\npartitioned by (").append(str)
							.append(" string)");
				}

				sb.append("\nrow format delimited fields terminated by '\\t'");

				sb.append("\nstored as ").append(
						conf.get("stored.as", "sequencefile"));

				sb.append("\nlocation '").append(conf.get("store.dir", "/jz"))
						.append("/").append(bcp).append("/';");

				bw.newLine();
				bw.append(sb.toString());
				bw.newLine();
			}

			System.out.println("output file ok: " + destFile.getAbsolutePath());
		} finally {
			bw.close();
		}
		return 0;
	}

	private static void parseCols(Element e, List<HiveColumn> columnList)
			throws IOException {
		String key = e.getAttributeValue("name");
		if (StringUtils.isEmpty(key)) {
			key = e.getName();
		}
		List<Element> list = e.getChildren();
		for (Element ec : list) {
			String name = ec.getName();
			if ("column".equalsIgnoreCase(name)) {
				String attr = ec.getAttributeValue("extend");
				if (attr != null
						&& "true".equalsIgnoreCase(attr.trim().toLowerCase())) {
					continue;
				}
				HiveColumn hcol = new HiveColumn();
				hcol.name = ec.getAttributeValue("name").trim().toLowerCase();

				String type = ec.getAttributeValue("type").trim().toLowerCase();
				if ("varchar2".equalsIgnoreCase(type)) {
					hcol.type = "string";
				} else if ("number".equalsIgnoreCase(type)) {
					String s = ec.getAttributeValue("len");
					if (s != null && Integer.parseInt(s) > 9) {
						hcol.type = "bigint";
					} else {
						hcol.type = "int";
					}
				} else if ("date".equalsIgnoreCase(type)) {
					hcol.type = "bigint";
				} else if ("int".equalsIgnoreCase(type)) { // 非法配置
					String s = ec.getAttributeValue("len");
					if (s != null && Integer.parseInt(s) > 9) {
						hcol.type = "bigint";
					} else {
						hcol.type = "int";
					}
					log.warn("Unsuggested hive column type mapping: " + key
							+ "# " + hcol.name + "/" + type);
				}  else if ("long".equalsIgnoreCase(type)) { // 非法配置
					hcol.type = "bigint";
					log.warn("Unsuggested hive column type mapping: " + key
							+ "# " + hcol.name + "/" + type);
				} else if ("double".equalsIgnoreCase(type)) { // 非法配置
					hcol.type = "double";
					log.warn("Unsuggested hive column type mapping: " + key
							+ "# " + hcol.name + "/" + type);
				} else if ("string".equalsIgnoreCase(type)) { // 非法配置
					hcol.type = "string";
					log.warn("Unsuggested hive column type mapping: " + key
							+ "# " + hcol.name + "/" + type);
				} else {
					throw new IOException(
							"Unsupported hive column type mapping: " + key
									+ "# " + hcol.name + "/" + type);
				}
				String str = ec.getAttributeValue("mps_code");
				if (StringUtils.isNotEmpty(str)) {
					hcol.code = str.trim();
				} else {
					log.warn("Undefined hive column code: " + key + "# "
							+ hcol.name + "/" + type);
				}
				columnList.add(hcol);
			}
		}
	}

	public static class HiveTable {
		public Element pe;

		public String name;

		private List<HiveColumn> headList;
		private List<HiveColumn> tailList;
		private List<HiveColumn> privateList;

		public void check() throws IOException {
			if (privateList == null) {
				privateList = new ArrayList<HiveColumn>();
				parseCols(pe, privateList);
			}

			int seq = 0;
			boolean hasErrors = false;
			Set<String> names = new HashSet<String>();
			if (headList != null) {
				for (HiveColumn c : headList) {
					seq++;
					if (names.contains(c.name.toLowerCase())) {
						hasErrors = true;
						log.error("Repeated hive column name: H#" + seq + " "
								+ name + "# " + c.name + "/" + c.type);
					} else {
						names.add(c.name.toLowerCase());
					}
				}
			}
			if (privateList != null) {
				for (HiveColumn c : privateList) {
					seq++;
					if (names.contains(c.name.toLowerCase())) {
						hasErrors = true;
						log.error("Repeated hive column name: P#" + seq + " "
								+ name + "# " + c.name + "/" + c.type);
					} else {
						names.add(c.name.toLowerCase());
					}
				}
			}
			if (tailList != null) {
				for (HiveColumn c : tailList) {
					seq++;
					if (names.contains(c.name.toLowerCase())) {
						hasErrors = true;
						log.error("Repeated hive column name: T#" + seq + " "
								+ name + "# " + c.name + "/" + c.type);
					} else {
						names.add(c.name.toLowerCase());
					}
				}
			}
			if (hasErrors) {
				throw new IOException("Repeated hive column name found: "
						+ name);
			}
		}
	}

	public static class HiveColumn {

		public String name;

		public String type;

		public String code;

	}

}
