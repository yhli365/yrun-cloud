package bcp;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableDataTool extends Configured implements Tool {

	public final static Logger log = LoggerFactory
			.getLogger(HiveTableDataTool.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new HiveTableDataTool(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
