package bcp;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.RunTool;
import bcp.conf.BcpColumnConfig;
import bcp.conf.BcpFmtParser;
import bcp.conf.BcpRecordParser;

/**
 * 将不分协议的BCP数据，按第一列协议名称进行分类输出.<br/>
 * 命令: <br/>
 * 参数: <br/>
 * -Dbcps=im,game -指定要提取的bcp，以逗号分隔。默认全部协议<br/>
 * -Dgroups=20150321 -指定分组日期列表，以逗号分隔。未指定时不按日期分组输出<br/>
 * 
 * @author yhli
 * 
 */
public class BcpExpMR extends RunTool {

	public final static Logger log = LoggerFactory.getLogger(BcpExpMR.class);

	public static void main(String[] args) throws Exception {
		execMain(new BcpExpMR(), args);
	}

	@Override
	public int exec(String[] args) throws Exception {
		Configuration conf = getConf();

		Set<String> bcps = new HashSet<String>();
		Collection<String> strs = conf.getStringCollection("bcps");
		if (!strs.isEmpty()) {
			for (String s : strs) {
				bcps.add(BcpFmtParser.bcpName(s));
			}
		} else {
			bcps.addAll(BcpFmtParser.getParser(conf).getSupportedBcps());
		}
		conf.set("filter.bcps", StringUtils.join(bcps, ","));

		Job job = Job.getInstance(conf);
		job.setJarByClass(BcpExpMR.class);

		// 和Flume生成的HDFS文件格式相同.
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Collection<String> groups = conf.getStringCollection("groups");

		MultipleOutputs.setCountersEnabled(job, true);
		if (groups.isEmpty()) {
			job.setMapperClass(MyExpMapper.class);
			for (String bcp : bcps) {
				MultipleOutputs.addNamedOutput(job,
						getNamedOutput(conf, bcp, null),
						SequenceFileOutputFormat.class, LongWritable.class,
						Text.class);
			}
		} else {
			job.setMapperClass(MyGroupedExpMapper.class);
			for (String bcp : bcps) {
				Map<String, String> groupNames = getGroupNames(conf, bcp);
				for (String ng : groupNames.values()) {
					MultipleOutputs.addNamedOutput(job, ng,
							SequenceFileOutputFormat.class, LongWritable.class,
							Text.class);
				}
			}
		}

		if (waitForCompletion(job, true)) {
			return 0;
		}
		return -1;
	}

	public static Map<String, String> getGroupNames(Configuration conf,
			String bcp) throws IOException {
		Collection<String> groups = conf.getStringCollection("groups");
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
			Map<String, String> groupMap = new HashMap<String, String>();
			for (String str : groups) {
				str = str.trim();
				String[] arr = str.split("-");
				if (arr.length == 2) {
					Date d1 = df.parse(arr[0].trim());
					Date d2 = df.parse(arr[1].trim());
					Calendar cdar = Calendar.getInstance();
					cdar.setTime(d1);
					while (true) {
						String sd = df.format(cdar.getTime());
						groupMap.put(sd, getNamedOutput(conf, bcp, sd));
						cdar.add(Calendar.DATE, 1);
						if (cdar.getTime().after(d2)) {
							break;
						}
					}
				} else {
					groupMap.put(str, getNamedOutput(conf, bcp, str));
				}
			}
			groupMap.put("default", getNamedOutput(conf, bcp, "d"));
			return groupMap;
		} catch (ParseException e) {
			throw new IOException("Parse groups error: " + groups, e);
		}
	}

	public static String getNamedOutput(Configuration conf, String bcp,
			String group) {
		String name;
		if (group == null) {
			name = bcp;
		} else {
			name = bcp + "X" + group;
		}
		String id = conf.get("id");
		if (!StringUtils.isEmpty(id)) {
			name = name + "X" + id;
		}
		return name;
	}

	public static class MyExpMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		private MultipleOutputs<LongWritable, Text> mos;
		private Set<String> bcps = new HashSet<String>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			bcps.addAll(conf.getStringCollection("filter.bcps"));
			mos = new MultipleOutputs<LongWritable, Text>(context);
			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			log.info("cleanup ok.");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int length = value.getLength();
			byte[] vbuf = value.getBytes();

			int pos = 0;
			for (; pos < length; pos++) {
				if (vbuf[pos] == BcpFmtParser.BCP_SEPERATOR) {
					break;
				}
			}

			String bcp = new String(vbuf, 0, pos);
			if (bcps.contains(bcp)) {
				mos.write(key, value, bcp);
			}
		}

	}

	public static class MyGroupedExpMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		private MultipleOutputs<LongWritable, Text> mos;
		private Map<String, GroupedBcpRecordParser> bcps = new HashMap<String, GroupedBcpRecordParser>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			for (String bcp : conf.getStringCollection("filter.bcps")) {
				GroupedBcpRecordParser bp = new GroupedBcpRecordParser();
				bp.setup(conf, bcp, "capture_time");
				bcps.put(bcp, bp);
			}
			mos = new MultipleOutputs<LongWritable, Text>(context);
			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			log.info("cleanup ok.");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int length = value.getLength();
			byte[] vbuf = value.getBytes();

			int pos = 0;
			for (; pos < length; pos++) {
				if (vbuf[pos] == BcpFmtParser.BCP_SEPERATOR) {
					break;
				}
			}

			String bcp = new String(vbuf, 0, pos);
			if (bcps.containsKey(bcp)) {
				try {
					GroupedBcpRecordParser bp = bcps.get(bcp);
					bp.parseBytes(vbuf, pos + 1, length);
					mos.write(key, value, bp.groupValue());
				} catch (IOException e) {
					context.getCounter("BJRUN", "error." + bcp).increment(1);
				}
			}
		}

	}

	public static class GroupedBcpRecordParser extends BcpRecordParser {

		protected int colIdxDate;

		protected SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		protected Map<String, String> groupNames = new HashMap<String, String>();
		protected String groupNameDefault;

		public void setup(Configuration conf, String bcpName, String colName)
				throws IOException {
			init(conf, bcpName);
			BcpColumnConfig bcc = getColumnConfig(colName);
			this.colIdxDate = bcc.colIndex;

			String bcp = getBcpName();
			groupNames = getGroupNames(conf, bcp);
			groupNameDefault = groupNames.get("default");
		}

		public String groupValue() throws IOException {
			int seconds = this.getInt(colIdxDate);
			String date = df.format(new Date(seconds * 1000L));
			String g = groupNames.get(date);
			if (g != null) {
				return g;
			}
			return groupNameDefault;
		}

	}

}
