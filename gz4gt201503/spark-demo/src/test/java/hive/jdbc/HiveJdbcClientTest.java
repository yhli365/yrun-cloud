package hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveJdbcClientTest {

	public static final Logger log = LoggerFactory
			.getLogger(HiveJdbcClientTest.class);

	private String driverName = "org.apache.hive.jdbc.HiveDriver";
	private String url = "jdbc:hive2://devs1:10000";
	private String user = "scott";
	private String password = "tiger";

	private Connection conn;
	private String tableName = "testHiveDriverTable";

	@Before
	public void setup() throws ClassNotFoundException, SQLException {
		Class.forName(driverName);
		conn = DriverManager.getConnection(url, user, password);
	}

	@After
	public void cleanup() throws SQLException {
		conn.close();
	}

	@Test
	public void dropTable() throws SQLException {
		Statement stmt = conn.createStatement();
		String sql = "drop table if exists " + tableName;
		boolean flag = stmt.execute(sql);
		stmt.close();
		log.info("dropTable#" + flag);
	}

	@Test
	public void createTable() throws SQLException {
		Statement stmt = conn.createStatement();
		String sql = "create table "
				+ tableName
				+ " (key int, value string) row format delimited fields terminated by '\t'";
		boolean flag = stmt.execute(sql);
		stmt.close();
		log.info("createTable#" + flag);
	}

	@Test
	public void showTables() throws SQLException {
		Statement stmt = conn.createStatement();
		String sql = "show tables";
		ResultSet rs = stmt.executeQuery(sql);
		log.info("showTables");
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
		stmt.close();
	}

	@Test
	public void loadData() throws SQLException {
		// hdfs dfs -put test.txt test.txt
		Statement stmt = conn.createStatement();
		String sql = "load data inpath '/user/hadoop/test.txt' OVERWRITE into table "
				+ tableName;
		boolean flag = stmt.execute(sql);
		stmt.close();
		log.info("loadData#" + flag);
	}

	@Test
	public void queryData() throws SQLException {
		Statement stmt = conn.createStatement();
		String sql = "select * from " + tableName
				+ " where key>=20 order by key desc limit 3";
		long ts1 = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(sql);
		log.info("queryData# " + sql);
		while (rs.next()) {
			System.out.println(rs.getString(1) + " => " + rs.getString(2));
		}
		stmt.close();
		log.info("queryData# OK, times=" + (System.currentTimeMillis() - ts1));
	}

	@Test
	public void count() throws SQLException {
		Statement stmt = conn.createStatement();
		String sql = "select count(1) from " + tableName + " where key>=20";
		long ts1 = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(sql);
		log.info("count# " + sql);
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
		stmt.close();
		log.info("count# OK, times=" + (System.currentTimeMillis() - ts1));
	}

}
