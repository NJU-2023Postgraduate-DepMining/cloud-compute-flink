package org.example.job;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.Properties;

public class MaxCHSink extends RichSinkFunction<Tuple4<String, String, Long, Integer>> {
    private Properties properties = new Properties();
    private String url = Job.url;

    MaxPO maxPO;

    public MaxCHSink() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        insert();
        super.close();
    }

    @Override
    public void invoke(Tuple4<String, String, Long, Integer> tuple4, Context context) throws Exception {
        MaxPO MaxPO=new MaxPO(tuple4);
        if (maxPO==null) {
            this.maxPO=MaxPO;
        } else {
            if (maxPO.getDependedCount()<MaxPO.getDependedCount()) {
                this.maxPO=MaxPO;
            }
        }
    }

    private void insert() throws SQLException {
        System.out.println("inserting Max");
        ClickHouseDataSource dataSource;
        Connection conn;
        dataSource = new ClickHouseDataSource(url, properties);
        conn = dataSource.getConnection("admin", "password");
        try(PreparedStatement ps = conn.prepareStatement(
                "insert into max select package_id, package_name,version,depended_time_stamp,day,month,year,depended_count" +
                        " from input('package_id String, package_name String, version String,depended_time_stamp Int64," +
                        " day String, month String, year String, depended_count Int32')")) {
                ps.setString(1, maxPO.getPackageId());
                ps.setString(2, maxPO.getPackageName());
                ps.setString(3, maxPO.getVersion());
                ps.setLong(4, maxPO.getDependedTimeStamp());
                ps.setString(5, maxPO.getDay());
                ps.setString(6, maxPO.getMonth());
                ps.setString(7, maxPO.getYear());
                ps.setInt(8, maxPO.getDependedCount());
                ps.addBatch();
            ps.executeBatch();
        }

    }
}
