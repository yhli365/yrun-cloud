package com.run.ayena.objectpp.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Hadoop工具类.
 * 
 * @author Yanhong Lee
 * 
 */
public abstract class HadoopTool extends Configured implements Tool {
	public static final Logger log = LoggerFactory.getLogger(HadoopTool.class);

	public static final String KEY_TOOLID = "run.toolid";

	private char sep = '\t';
	private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void execMain(HadoopTool tool, String[] args)
			throws Exception {
		String toolid = tool.getClass().getSimpleName();
		log.info("\nexec start: ---------------");
		StringBuilder sb = new StringBuilder(" <cmd> ");
		sb.append(toolid);
		for (int i = 0; i < args.length; i++) {
			sb.append(" ").append(args[i]);
		}
		log.info(sb.toString());
		try {
			Configuration conf = new Configuration();
			conf.addResource("tool/" + toolid + ".xml");
			conf.set(KEY_TOOLID, toolid);
			GenericOptionsParser parser = new GenericOptionsParser(conf, args);

			String val;
			String key = MRJobConfig.JOB_NAME;
			if (conf.get(key) == null) {
				val = toolid
						+ "_"
						+ new SimpleDateFormat("yyyyMMddHHmmss")
								.format(new Date());
				conf.set(key, val);
			}

			tool.setConf(conf);
			String[] toolArgs = parser.getRemainingArgs();
			tool.run(toolArgs);
		} catch (Exception e) {
			log.info("exec failed: " + toolid, e);
		} finally {
			log.info("exec end: ---------------");
		}
	}

	public String getToolId() {
		Configuration conf = getConf();
		return conf.get(KEY_TOOLID);
	}

	/**
	 * 收集Job执行后的相关统计信息.
	 * 
	 * @param job
	 * @param verbose
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public boolean waitForCompletion(Job job, boolean verbose)
			throws ClassNotFoundException, IOException, InterruptedException {
		boolean flag = false;
		long startTime = System.currentTimeMillis();
		if (job.waitForCompletion(verbose)) {
			flag = true;
		}
		long endTime = System.currentTimeMillis();

		File f = new File(job.getConfiguration().get("job.tlog",
				getToolId() + ".tlog"));

		StringBuilder sbt = null;
		if (!f.exists()) {
			sbt = new StringBuilder();
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));

		if (sbt != null) {
			sbt.append(sep).append("Job ID");
			sbt.append(sep).append("Job Name");
			sbt.append(sep).append("Job State");
			sbt.append(sep).append("Start Time");
			sbt.append(sep).append("End Time");
			sbt.append(sep).append("Exec Time(s)");
		}

		StringBuilder sb = new StringBuilder();
		sb.append(sep).append(job.getJobID());
		sb.append(sep).append(job.getJobName());
		sb.append(sep).append(job.getJobState());
		sb.append(sep).append(df.format(new Date(startTime)));
		sb.append(sep).append(df.format(new Date(endTime)));
		long secs = (endTime - startTime) / 1000;
		sb.append(sep).append(secs);

		if (job.getStatus().getState() == JobStatus.State.SUCCEEDED) {

			class CGPair {
				CounterGroup cg;
				Counter c;
			}
			// 输出计数器值
			List<CGPair> cgList = Lists.newArrayList();
			Counters counters = job.getCounters();

			Iterator<CounterGroup> giter = counters.iterator();
			while (giter.hasNext()) {
				CounterGroup cg = giter.next();
				Iterator<Counter> citer = cg.iterator();
				while (citer.hasNext()) {
					Counter c = citer.next();
					CGPair p = new CGPair();
					p.cg = cg;
					p.c = c;
					cgList.add(p);
				}
			}
			Collections.sort(cgList, new Comparator<CGPair>() {

				@Override
				public int compare(CGPair o1, CGPair o2) {
					int c = o1.cg.getDisplayName().compareTo(
							o2.cg.getDisplayName());
					if (c == 0) {
						c = o1.c.getDisplayName().compareTo(
								o2.c.getDisplayName());
					}
					return c;
				}

			});

			for (CGPair p : cgList) {
				sb.append(sep).append(p.c.getValue());
				if (sbt != null) {
					sbt.append(sep).append(p.c.getDisplayName());
				}
			}

		}
		if (sbt != null) {
			bw.append(sbt.substring(1));
			bw.newLine();
		}
		bw.append(sb.substring(1));
		bw.newLine();
		bw.close();
		log.info("write job tlog ok: " + f.getAbsolutePath());

		return flag;
	}

}
