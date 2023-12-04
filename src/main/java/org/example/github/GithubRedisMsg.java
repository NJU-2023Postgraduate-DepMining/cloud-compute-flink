package org.example.github;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GithubRedisMsg implements java.io.Serializable {
    private String packageName;
    private String version;
    private Long timestamp;

    private Integer count = 1;

    public GithubRedisMsg(String packageName, String version, Long timestamp) {
        this.packageName = packageName;
        this.version = version;
        this.timestamp = timestamp;
    }
    public GithubRedisMsg() {
    }
    @Override
    public String toString() {
        // json

        return "{\"package\":\"" + packageName + "\",\"version\":\"" + version + "\",\"timestamp\":" + timestamp + ",\"cnt\":" + count + "}";
    }
}
