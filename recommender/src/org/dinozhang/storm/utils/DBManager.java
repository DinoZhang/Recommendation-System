package org.dinozhang.storm.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DBManager {

	private static final Log log = LogFactory.getLog(DBManager.class);

	private static DataSource dataSource = new ComboPooledDataSource("connection-pool");

	private DBManager() {
	}

	/**
	 * 从数据源中获取数据库连接
	 */
	public static Connection getConnection() {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			// conn.setAutoCommit(false);
			return conn;
		} catch (Exception e) {
			log.error("从数据源获取连接异常", e);
			throw new RuntimeException("从数据源获取连接异常", e);
		}
	}

	/**
	 * 回滚事务
	 */
	public static void rollback(Connection conn) {
		try {
			if (conn != null)
				conn.rollback();
		} catch (SQLException e) {
			log.error("回滚事务异常", e);
			throw new RuntimeException("回滚事务异常", e);
		}
	}

	/**
	 * 提交事务
	 */
	public static void commit(Connection conn) {
		try {
			if (conn != null)
				conn.commit();
		} catch (SQLException e) {
			log.error("事物提交异常", e);
			throw new RuntimeException("事物提交异常", e);
		}
	}

	/**
	 * 关闭数据库连接
	 */
	public static void closeConnection(Connection conn) {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			log.error("关闭数据库连接", e);
			throw new RuntimeException("关闭数据库连接", e);
		}
	}

	public static void main(String[] args) throws Exception {
		Connection conn = DBManager.getConnection();

		// String sql = "INSERT INTO itcast(id, name, age) VALUES (?, ?, ?)";
		// PreparedStatement pstmt = conn.prepareStatement(sql);
		// pstmt.setLong(1, 4L);
		// pstmt.setString(2, "zx");
		// pstmt.setInt(3, 25);
		// int count = pstmt.executeUpdate();
		// conn.commit();
		// System.out.println(count);

		String sql = "SELECT * FROM movies";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		String insertSql = "INSERT INTO movies ";
		String s1 = " (";
		String s2 = " (";
		for (int i = 1; i <= columnCount; i++) {
			String columnName = metaData.getColumnName(i);
			if (i < columnCount) {
				s1 += columnName + ", ";
				s2 += "?, ";
			} else {
				s1 += columnName + ")";
				s2 += "?) ";
			}
		}
		insertSql += (s1 + " VALUES " + s2);
		System.out.println(insertSql);
	}
}
