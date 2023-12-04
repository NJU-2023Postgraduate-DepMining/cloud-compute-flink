package org.example.npm;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.github.GithubPO;
import org.example.job.Job;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NpmCHSink extends RichSinkFunction<Tuple4<String, String, Long, Integer>> {
    private Properties properties = new Properties();
    private String url = Job.url;

    List<NpmPO> list;

    public NpmCHSink() {
        list=new ArrayList<>();
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
        while (list.size()>=1000) {
            insert();
        }
        NpmPO npmPO=new NpmPO(tuple4);
        list.add(npmPO);
    }

    private void insert() throws SQLException{
        System.out.println("inserting Npm");
        ClickHouseDataSource dataSource;
        Connection conn;
        dataSource = new ClickHouseDataSource(url, properties);
        conn = dataSource.getConnection("admin", "password");
        try(PreparedStatement ps = conn.prepareStatement(
                "insert into npm_dependency_stats select package_id, package_name,version,depended_time_stamp,day,month,year,depended_count" +
                        " from input('package_id String, package_name String, version String,depended_time_stamp Int64," +
                        " day String, month String, year String, depended_count Int32')")) {
            for (int i = 0; i < list.size(); i++) {
                NpmPO npmPO = list.get(i);
                ps.setString(1, npmPO.getPackageId());
                ps.setString(2, npmPO.getPackageName());
                ps.setString(3, npmPO.getVersion());
                ps.setLong(4, npmPO.getDependedTimeStamp());
                ps.setString(5, npmPO.getDay());
                ps.setString(6, npmPO.getMonth());
                ps.setString(7, npmPO.getYear());
                ps.setInt(8, npmPO.getDependedCount());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        list.clear();
    }

}
