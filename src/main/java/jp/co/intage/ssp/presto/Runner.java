package jp.co.intage.ssp.presto;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Runner {
    private static final Log log = LogFactory.getLog(Runner.class);

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 1) {
            throw new RuntimeException("スクリプトをしてください！");
        }

        String path = args[0];
        String script = getScript(path);
        if (script == null) {
            throw new RuntimeException("パスが間違っている：" + path);
        }

        log.info("Script:\n" + script);
        executeScript(script);
    }

    private static void executeScript(String script) throws Exception {
        String url = String.format(
            "jdbc:presto://%s:8889/hive/default",
            InetAddress.getLocalHost().getHostName()
        );

        Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        try (
            Connection conn = DriverManager.getConnection(url, "hadoop", null);
            Statement stmt = conn.createStatement()
        ) {
            for (String s : script.split(";\n")) {
                log.info("Running:\n" + s);
                stmt.execute(s);
            }
        }
    }

    private static String getScript(String path) {
        if (StringUtils.isNullOrEmpty(path)) return null;

        path = path.replaceAll("^(s|S)3://", "");
        int slash = path.indexOf('/');
        if (slash < 1 || slash == path.length()) return null;

        String bucket = path.substring(0, slash);
        String key = path.substring(slash + 1);

        log.info(String.format(
            "Load Script at %s / %s",
            bucket, key
        ));
        try {
            return AmazonS3ClientBuilder.defaultClient().getObjectAsString(bucket, key);
        } catch (Exception e) {
            log.error("Load Script Error:", e);
            return null;
        }
    }
}
