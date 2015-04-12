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

public class IMJdbcTest {

	public static final Logger log = LoggerFactory.getLogger(IMJdbcTest.class);
	private Connection conn;

	@Before
	public void setup() throws ClassNotFoundException, SQLException {
		String driverName = "org.apache.hive.jdbc.HiveDriver";
		String url = "jdbc:hive2://devs1:10000";
		String user = "scott";
		String password = "tiger";

		Class.forName(driverName);
		conn = DriverManager.getConnection(url, user, password);
	}

	@After
	public void cleanup() throws SQLException {
		conn.close();
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
	public void count() throws SQLException {
		Statement stmt = conn.createStatement();
		String sql = "select count(1) from im";
		long ts1 = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(sql);
		System.out.println("EXEC SQL: " + sql);
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
		stmt.close();
		System.out.println("EXEC OK: times="
				+ (System.currentTimeMillis() - ts1));
	}

	@Test
	public void topMSISDN() throws SQLException {
		Statement stmt = conn.createStatement();
		String sql = "select MSISDN,count(1) cnt from im group by MSISDN order by cnt desc limit 30";
		long ts1 = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(sql);
		System.out.println("EXEC SQL: " + sql);

		// 列名称
		int colCount = rs.getMetaData().getColumnCount();
		for (int c = 1; c <= colCount; c++) {
			System.out.print(rs.getMetaData().getColumnName(c) + "\t");
		}
		System.out.println();

		// 数据
		while (rs.next()) {
			System.out.println(rs.getString(1) + " => " + rs.getLong(2));
		}
		stmt.close();
		System.out.println("EXEC OK: times="
				+ (System.currentTimeMillis() - ts1));
	}

	@Test
	public void t001_MSISDN() throws SQLException {
		Statement stmt = conn.createStatement();
		String sql = "select pname,protocol_id,data_id,capture_time,bsid,MSISDN,user_name,from_user_name,to_user_name from im where MSISDN='19377579818' limit 100";
		long ts1 = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(sql);
		System.out.println("EXEC SQL: " + sql);

		// 列名称
		int colCount = rs.getMetaData().getColumnCount();
		for (int c = 1; c <= colCount; c++) {
			System.out.print(rs.getMetaData().getColumnName(c) + "\t");
		}
		System.out.println();

		// 数据
		while (rs.next()) {
			for (int c = 1; c <= colCount; c++) {
				System.out.print(rs.getString(c) + "\t");
			}
			System.out.println();
		}
		stmt.close();
		System.out.println("EXEC OK: times="
				+ (System.currentTimeMillis() - ts1));
	}

}
