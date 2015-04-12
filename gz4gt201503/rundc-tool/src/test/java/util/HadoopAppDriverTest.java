package util;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopAppDriverTest {

	private static final Logger log = LoggerFactory
			.getLogger(HadoopAppDriverTest.class);

	public static void driver(String cmd) throws IOException {
		log.info("driver cmd: " + cmd);
		System.setProperty("hadoop.home.dir", "D:/ycloud/hadoop-2.5.0-cdh5.2.0");
		String[] args;
		if (StringUtils.isEmpty(cmd)) {
			args = new String[0];
		} else {
			args = cmd.split("\\s+");
		}

		try {
			HadoopAppDriver.exec(args);
		} catch (Exception e) {
			throw new IOException("exec failed: " + cmd, e);
		}
	}

	@Test
	public void stringRegex() {
		String[] inputs = new String[] {//
		"bcp_2015032517.1427516947352.lzo_deflate",//
				"bcp_.1427516947353.lzo_deflate" //
		};
		String regex = "bcp_20\\d{8}.*.lzo_deflate";
		Pattern pat = Pattern.compile(regex);

		for (String input : inputs) {
			System.out.println("\ninput: " + input);
			Matcher m = pat.matcher(input);
			System.out.println("match: " + m.matches());
			if (m.matches()) {
				int idx1 = input.indexOf('_');
				int idx2 = input.indexOf('.', idx1);
				String par = input.substring(idx1 + 1, idx2);
				System.out.println("distill: " + par);
			}
		}

	}

}
