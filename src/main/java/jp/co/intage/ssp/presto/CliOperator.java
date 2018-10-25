package jp.co.intage.ssp.presto;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * コマンドライン presto-cli 経由で実行
 */
public class CliOperator implements Operator {
    private static final Log log = LogFactory.getLog(CliOperator.class);
    private static final String CMD = "presto-cli --file %s";

    @Override
    public void operate(String sql) throws Exception {
        String key = UUID.randomUUID().toString().replace("-", "") + ".sql";
        Path file = Paths.get(key).getFileName();
        Files.write(file, sql.getBytes());

        String cmd = String.format(CMD, file.toAbsolutePath().toString());
        log.info(cmd);

        Process process =
            new ProcessBuilder().inheritIO().command(cmd.split(" ")).start();
        process.waitFor();
        int exitValue = process.exitValue();

        log.info("Exit Value : " + exitValue);
        if (exitValue != 0) throw new RuntimeException("Failed : " + exitValue);
    }
}
