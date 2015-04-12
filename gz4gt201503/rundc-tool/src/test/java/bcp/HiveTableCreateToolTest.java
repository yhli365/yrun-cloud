package bcp;

import java.io.IOException;

import org.junit.Test;

import util.HadoopAppDriverTest;

public class HiveTableCreateToolTest {

	@Test
	public void createTableIM() throws IOException {
		HadoopAppDriverTest.driver("hive.create -Dbcps=im target/im.sql");
	}

	@Test
	public void createTableMulti() throws IOException {
		HadoopAppDriverTest
				.driver("hive.create -Dbcps=im,game target/bcp2.sql");
	}

	@Test
	public void createTableAll() throws IOException {
		HadoopAppDriverTest.driver("hive.create target/bcp.sql");
	}

	@Test
	public void createTableIdentitynetinfo() throws IOException {
		HadoopAppDriverTest.driver("hive.create "//
				+ "-Dbcps=identitynetinfo "//
				+ "-Dcolumn.bcpid=NULL " //
				+ "-Dstored.as=textfile "//
				+ "-Dpartitioned.by=NULL "//
				+ "target/identitynetinfo.sql");
	}
}
