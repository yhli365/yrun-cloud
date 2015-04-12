package bcp;

import java.io.IOException;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bcp.conf.BcpFmtParser;

public class HiveTableDataTool extends Configured implements Tool {

	public final static Logger log = LoggerFactory
			.getLogger(HiveTableDataTool.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new HiveTableDataTool(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String cmd = args[0];
		if ("-loadBcp".equalsIgnoreCase(cmd)) {
			return loadBcp(conf, args);
		} else if ("-loadFlumeBcp".equalsIgnoreCase(cmd)) {
			return loadFlumeBcp(conf, args);
		} else {
			System.out.println("[ERROR] unknown command: " + cmd);
		}
		return 0;
	}

	private int loadFlumeBcp(Configuration conf, String[] args)
			throws IOException {
		Path indir = new Path(args[1]);
		Path outdir = new Path(args[2]);

		String regex = conf.get("file.regex", "bcp_20\\d{8}.*.lzo_deflate");

		int count = 0;
		FileSystem fs = FileSystem.get(conf);
		String bcp = conf.get("bcp", "im");
		String bcpName = BcpFmtParser.bcpName(bcp);
		FileStatus[] farr = fs
				.listStatus(indir, new RegexNamePathFilter(regex));
		for (FileStatus fss : farr) {
			String name = fss.getPath().getName();
			int idx1 = name.indexOf('_');
			int idx2 = name.indexOf('.', idx1);
			String par = name.substring(idx1 + 1, idx2);
			Path dst = new Path(outdir, bcpName + "/" + par + "/" + name);
			Path dstp = dst.getParent();
			if (!fs.exists(dstp)) {
				fs.mkdirs(dstp);
			}
			fs.rename(fss.getPath(), dst);
			count++;
			log.info("Move file ok: #" + count + " {} -> {} ", fss.getPath(),
					dst);
		}

		log.info("loadFlumeBcp ok: {} -> {} , count=" + count, indir, outdir);
		return 0;
	}

	private int loadBcp(Configuration conf, String[] args) throws IOException {
		Path indir = new Path(args[1]);
		Path outdir = new Path(args[2]);

		class BcpPathFilter implements PathFilter {
			private String bcp;

			public BcpPathFilter(String bcp) {
				this.bcp = bcp + "X";
			}

			@Override
			public boolean accept(Path path) {
				String name = path.getName();
				if (name.startsWith(bcp)) {
					return true;
				}
				return false;
			}

		}

		int count = 0;
		FileSystem fs = FileSystem.get(conf);
		Collection<String> strs = conf.getStringCollection("bcps");
		for (String bcp : strs) {
			String bcpName = BcpFmtParser.bcpName(bcp);
			FileStatus[] farr = fs
					.listStatus(indir, new BcpPathFilter(bcpName));
			for (FileStatus fss : farr) {
				String name = fss.getPath().getName();
				int idx1 = name.indexOf("X", 0);
				int idx2 = name.indexOf("X", idx1 + 1);
				String par = name.substring(idx1 + 1, idx2);
				Path dst = new Path(outdir, bcpName + "/" + par + "/" + name);
				Path dstp = dst.getParent();
				if (!fs.exists(dstp)) {
					fs.mkdirs(dstp);
				}
				fs.rename(fss.getPath(), dst);
				count++;
				log.info("Move file ok: #" + count + " {} -> {} ",
						fss.getPath(), dst);
			}
		}

		log.info("loadBcp ok: {} -> {} , count=" + count, indir, outdir);
		return 0;
	}

	public static class RegexNamePathFilter implements PathFilter {
		private Pattern p;

		public RegexNamePathFilter(String regex) {
			this.p = Pattern.compile(regex);
		}

		@Override
		public boolean accept(Path path) {
			String name = path.getName();
			if (p.matcher(name).matches()) {
				return true;
			}
			return false;
		}

	}

}
