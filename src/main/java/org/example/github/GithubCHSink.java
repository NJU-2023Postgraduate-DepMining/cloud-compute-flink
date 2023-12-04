package org.example.github;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.job.Job;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class GithubCHSink extends RichSinkFunction<Tuple4<String, String, Long, Integer>> {

    private Properties properties = new Properties();
    private String url = Job.url;
    List<GithubPO> list;

    public GithubCHSink() {
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
        GithubPO githubPO=new GithubPO(tuple4);
        list.add(githubPO);
    }

    private void insert() throws SQLException{
        System.out.println("inserting Github");
        ClickHouseDataSource dataSource;
        Connection conn;
        dataSource = new ClickHouseDataSource(url, properties);
        conn = dataSource.getConnection("admin", "password");
        try(PreparedStatement ps = conn.prepareStatement(
                "insert into github_dependency_stats select package_id, package_name,version,depended_time_stamp,day,month,year,depended_count" +
                        " from input('package_id String, package_name String, version String,depended_time_stamp Int64," +
                        " day String, month String, year String, depended_count Int32')")) {
            for (int i = 0; i < list.size(); i++) {
                GithubPO githubPO = list.get(i);
                ps.setString(1, githubPO.getPackageId());
                ps.setString(2, githubPO.getPackageName());
                ps.setString(3, githubPO.getVersion());
                ps.setLong(4, githubPO.getDependedTimeStamp());
                ps.setString(5, githubPO.getDay());
                ps.setString(6, githubPO.getMonth());
                ps.setString(7, githubPO.getYear());
                ps.setInt(8, githubPO.getDependedCount());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        list.clear();
    }


}
