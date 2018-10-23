package jp.co.intage.ssp.presto;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Runner {
    private static final Log log = LogFactory.getLog(Runner.class);
    private static final String CMD = "presto-cli --file %s";

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 1) {
            throw new RuntimeException("スクリプトをしてください！");
        }

        String path = args[0];
        String script = getScript(path);
        if (script == null) {
            throw new RuntimeException("パスが間違っている：" + path);
        }

        executeScript(script);
    }

    private static void executeScript(String script) throws Exception {
        String cmd = String.format(CMD, script);
        log.info("Execute:" + cmd);

        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();

        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            log.info(line);
        }
    }

    private static String getScript(String path) throws Exception {
        if (StringUtils.isNullOrEmpty(path)) return null;

        path = path.replaceAll("^(s|S)3://", "");
        int slash = path.indexOf('/');
        if (slash < 1 || slash == path.length()) return null;

        String bucket = path.substring(0, slash);
        String key = path.substring(slash + 1);
        log.info(String.format("Load Script at %s / %s", bucket, key));

        String script = AmazonS3ClientBuilder.defaultClient().getObjectAsString(bucket, key);
        log.info("Script:\n" + script);
        Files.write(Paths.get(key), script.getBytes());

        return key;
    }
}
