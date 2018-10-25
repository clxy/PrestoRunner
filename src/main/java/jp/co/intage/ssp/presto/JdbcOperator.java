package jp.co.intage.ssp.presto;

import com.amazonaws.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * JDBC経由で実行する
 * 1. pom.xmlには下記が必要
 * <dependency>
 *     <groupId>com.facebook.presto</groupId>
 *     <artifactId>presto-jdbc</artifactId>
 *     <version>0.212</version>
 * </dependency>
 * 2. jarファイルには上記のpresto-jdbcを含める必要
 *  File ⇒ Project Structure ⇒ Artifacts
 */
public class JdbcOperator implements Operator {
    private static final Log log = LogFactory.getLog(JdbcOperator.class);

    @Override
    public void operate(String sql) throws Exception {
        String url = String.format(
            "jdbc:presto://%s:8889/hive/default",
            InetAddress.getLocalHost().getHostName()
        );

        Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        try (
            Connection conn = DriverManager.getConnection(url, "hadoop", null);
            Statement stmt = conn.createStatement()
        ) {
            for (String s : sql.split(";\\R")) {
                if (StringUtils.isNullOrEmpty(s)) continue;

                log.info("Running:\n" + s);
                stmt.execute(s);
            }
        }
    }
}
