package util;

import org.apache.hadoop.util.ProgramDriver;

import bcp.HiveTableCreateTool;
import bcp.HiveTableDataTool;

/**
 * @author Yanhong Lee
 * 
 */
public class HadoopAppDriver {

	public static int exec(String[] args) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("hive.create", HiveTableCreateTool.class,
					"根据bcpfmt.xml配置生成Hive表创建语句");
			pgd.addClass("hive.data", HiveTableDataTool.class,
					"Hive表数据管理工具");

			pgd.driver(args);

			// Success
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return exitCode;
	}

	public static void main(String[] args) {
		int exitCode = exec(args);
		System.exit(exitCode);
	}

}
