package jp.co.intage.ssp.presto;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

public class Runner {
    private static final Log log = LogFactory.getLog(Runner.class);
    private static final Map<String, Operator> OPERATORS =
        new HashMap<String, Operator>() {{
            put("cli", new CliOperator());
            put("jdbc", new JdbcOperator());
        }};

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            throw new RuntimeException("スクリプトを指定してください！");
        }

        String path = args[0];
        String script = getScript(path);
        if (script == null) {
            throw new RuntimeException("パスが間違っている : " + path);
        }

        log.info("Script:\n" + script);

        String operator = "jdbc";
        if (args.length >= 2) operator = args[1];
        OPERATORS.get(operator).operate(script);
    }

    private static String getScript(String s3Path) throws Exception {
        if (StringUtils.isNullOrEmpty(s3Path)) return null;

        s3Path = s3Path.replaceAll("^(s|S)3://", "");
        int slash = s3Path.indexOf('/');
        if (slash < 1 || slash == s3Path.length()) return null;

        String bucket = s3Path.substring(0, slash);
        String key = s3Path.substring(slash + 1);
        log.info(String.format("Load Script from %s:%s", bucket, key));

        return AmazonS3ClientBuilder.defaultClient().getObjectAsString(bucket, key);
    }
}
