package org.example.github;

public class GithubRedisMsg implements java.io.Serializable {
    String packageName;
    String version;
    Long timestamp;

    Integer count = 1;

    public GithubRedisMsg(String packageName, String version, Long timestamp) {
        this.packageName = packageName;
        this.version = version;
        this.timestamp = timestamp;
    }


    @Override
    public String toString() {
        // json

        return "{\"package\":\"" + packageName + "\",\"version\":\"" + version + "\",\"timestamp\":" + timestamp + ",\"cnt\":" + count + "}";
    }
}
