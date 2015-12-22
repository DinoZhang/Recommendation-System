package org.dinozhang.storm.bolt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dinozhang.storm.utils.DBManager;
import org.dinozhang.storm.utils.PropertyUtil;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 将数据写入到mysq的Bolt
 */
public class RatingsToMysql extends BaseBasicBolt {

    private static final Log  log = LogFactory.getLog(RatingsToMysql.class);

    private String            seprator;

    //private String tableName;

    private Connection        conn;

    private PreparedStatement pstmt;

    private String            insertSql;

    /*public MovieToMysql(String tableName, String filterConfig) {
    	this.tableName = tableName;
    }
    */
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        //seprator = PropertyUtil.getProperty("seprator");
        seprator = PropertyUtil.getProperty("seprator");
        try {
            conn = DBManager.getConnection();
            String selectSql = "SELECT * FROM movie_preferences limit 1";
            insertSql = "INSERT INTO movie_preferences";
            PreparedStatement pstmt = conn.prepareStatement(selectSql);
            ResultSet rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            String fields = " (";
            String vals = " (";
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (i < columnCount) {
                    fields += columnName + ", ";
                    vals += "?, ";
                } else {
                    fields += columnName + ")";
                    vals += "?) ";
                }
            }
            insertSql += (fields + " VALUES " + vals);
        } catch (SQLException e) {
            log.error("获取元数据失败", e);
            throw new RuntimeException("获取元数据失败", e);
        }
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        List<Object> list = input.getValues();

        String[] fields = new String[4];
        fields[0] = (String) list.get(0);
        fields[1] = (String) list.get(1);
        fields[2] = (String) list.get(2);
        fields[3] = (String) list.get(3);
        try {
            pstmt = conn.prepareStatement(insertSql);
            for (int i = 0; i < fields.length; i++) {
                pstmt.setString(i + 1, fields[i]);
            }
            pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("数据插入失败", e);
            throw new RuntimeException("数据插入失败", e);
        }
        //collector.emit(new Values(line));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void cleanup() {
        DBManager.closeConnection(conn);
    }
}
