package bcp;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
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
 * -Dbcp=im -指定统计的bcp<br/>
 * -Dfields=protocol_id,@date -指定统计的bcp字段列表<br/>
 * 
 * @author yhli
 * 
 */
public class BcpCountMR extends RunTool {

	public final static Logger log = LoggerFactory.getLogger(BcpCountMR.class);

	public static void main(String[] args) throws Exception {
		execMain(new BcpCountMR(), args);
	}

	@Override
	public int exec(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		job.setJarByClass(BcpCountMR.class);
		job.setMapperClass(MyCountMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (waitForCompletion(job, true)) {
			return 0;
		}
		return -1;
	}

	public static class MyCountMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		private Text outKey = new Text();
		private LongWritable one = new LongWritable(1);

		private String bcp;
		private BcpRecordParser parser;
		private List<ColumnValueBuilder> bcpCols = new ArrayList<ColumnValueBuilder>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			bcp = conf.get("bcp");
			bcp = BcpFmtParser.bcpName(bcp);
			parser = BcpRecordParser.getParser(conf, bcp);

			String str = conf.get("fields", "protocol_id,@date");
			for (String s : str.split(",")) {
				ColumnValueBuilder cvb;
				if ("@date".equals(s.trim().toLowerCase())) {
					cvb = new DateColumnValueBuilder();
					cvb.setup(parser, "capture_time");
				} else {
					cvb = new ColumnValueBuilder();
					cvb.setup(parser, s);
				}
				bcpCols.add(cvb);
			}
			log.info("bcp=" + bcp + ", fields=" + str);

			log.info("setup ok.");
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
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

			String name = new String(vbuf, 0, pos);
			if (bcp.equals(name)) {
				try {
					parser.parseBytes(vbuf, pos + 1, length);

					StringBuilder sb = new StringBuilder();
					for (ColumnValueBuilder vb : bcpCols) {
						sb.append('\t').append(vb.parse());
					}
					outKey.set(sb.substring(1));
					context.write(outKey, one);
				} catch (IOException e) {
					context.getCounter("BJRUN", "error." + bcp).increment(1);
				}
			}
		}

	}

	public static class ColumnValueBuilder {

		protected BcpRecordParser parser;
		protected int colIndex;

		public void setup(BcpRecordParser parser, String colName)
				throws IOException {
			this.parser = parser;
			BcpColumnConfig bcc = parser.getColumnConfig(colName);
			this.colIndex = bcc.colIndex;
		}

		public String parse() throws IOException {
			return parser.getString(colIndex);
		}
	}

	public static class DateColumnValueBuilder extends ColumnValueBuilder {

		protected SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

		@Override
		public String parse() throws IOException {
			int seconds = parser.getInt(colIndex);
			return df.format(new Date(seconds * 1000L));
		}

	}

}
