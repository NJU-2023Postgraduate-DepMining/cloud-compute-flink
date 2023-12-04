package org.example.github;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.tuple.Tuple4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Getter
@Setter
public class GithubPO {
    /**
     * 包的唯一标志，package identity，为报名+版本号的组合
     */
    private String packageId;
    /**
     * 包名
     */
    private String packageName;
    /**
     * 版本号
     */
    private String version;
    /**
     * 时间戳，可以用于排序；对于github，就是updated_at；对于npm，就是版本的发布时间
     */
    private Long dependedTimeStamp;
    /**
     * 日期，精准到天，支持日期聚合查询
     */
    private String day;
    /**
     * 月，精准到月，支持月聚合查询
     */
    private String month;
    /**
     * 年，精准到年，支持年聚合查询
     */
    private String year;
    /**
     * 被依赖计数，今天被依赖的次数
     */
    private Integer dependedCount;

    public GithubPO() {
    }
    public GithubPO(Tuple4<String, String,Long, Integer> tuple4){
        this.packageId = tuple4.f0+":"+ tuple4.f1;
        this.packageName = tuple4.f0;
        this.version = tuple4.f1;
        this.dependedTimeStamp = tuple4.f2;
        this.dependedCount = tuple4.f3;
        Date date= new Date(dependedTimeStamp);
        DateFormat df= new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = df.format(date);
        this.day = dateStr;
        this.month=dateStr.substring(0,7);
        this.year=dateStr.substring(0,4);
    }


}
