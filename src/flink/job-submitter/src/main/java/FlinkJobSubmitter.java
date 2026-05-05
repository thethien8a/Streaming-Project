import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.StatementSet;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FlinkJobSubmitter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Pin session timezone to UTC. CAST(TIMESTAMP_LTZ AS TIMESTAMP) uses the session
        // local time-zone; UTC keeps the conversion deterministic regardless of where the
        // JobManager/TaskManager containers run, so the values written to the sink Kafka
        // topics match the original UTC instants from Postgres/Debezium.
        tableEnv.getConfig().setLocalTimeZone(java.time.ZoneId.of("UTC"));

        // Step 1: Execute DDL from init.sql (CREATE TABLE statements)
        List<String> initStatements = parseSqlResource("/sql/init.sql");
        System.out.println("=== Executing init.sql (" + initStatements.size() + " statements) ===");
        for (String stmt : initStatements) {
            String preview = stmt.replaceAll("\\s+", " ").substring(0, Math.min(80, stmt.length()));
            System.out.println("  -> " + preview + "...");
            tableEnv.executeSql(stmt);
        }

        // Step 2: Submit DML from submit_jobs.sql using StatementSet
        List<String> jobStatements = parseSqlResource("/sql/submit_jobs.sql");
        System.out.println("\n=== Submitting jobs from submit_jobs.sql (" + jobStatements.size() + " statements) ===");

        StatementSet stmtSet = tableEnv.createStatementSet();
        for (String stmt : jobStatements) {
            String preview = stmt.replaceAll("\\s+", " ").substring(0, Math.min(80, stmt.length()));
            System.out.println("  -> " + preview + "...");
            // StatementSet only accepts INSERT; everything else (CREATE VIEW, etc.) must
            // be executed eagerly so it's available for subsequent INSERTs in the set.
            if (stmt.toUpperCase().trim().startsWith("INSERT")) {
                stmtSet.addInsertSql(stmt);
            } else {
                tableEnv.executeSql(stmt);
            }
        }

        System.out.println("\nSubmitting all jobs as a single Flink job...");
        stmtSet.execute();
        System.out.println("All jobs submitted successfully!");
    }

    private static List<String> parseSqlResource(String resourcePath) throws Exception {
        try (InputStream is = FlinkJobSubmitter.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + resourcePath);
            }
            String content = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));

            // Strip SQL comments (line comments + block comments)
            content = content.replaceAll("(?m)--.*$", "").replaceAll("(?s)/\\*.*?\\*/", "");

            // Substitute ${ENV_VAR} placeholders from the JobManager's environment.
            // Used so JDBC connector secrets (POSTGRES_USER / POSTGRES_PASSWORD) are not
            // hard-coded in init.sql. Throws if a referenced var is missing so failures
            // are loud at startup rather than silent NPE/auth failures at lookup time.
            content = substituteEnvVars(content);

            // Split on ';' but ignore semicolons inside single-quoted string literals
            List<String> statements = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean inString = false;
            for (int i = 0; i < content.length(); i++) {
                char ch = content.charAt(i);
                if (ch == '\'') {
                    // Handle escaped '' inside string
                    if (inString && i + 1 < content.length() && content.charAt(i + 1) == '\'') {
                        current.append(ch);
                        current.append(content.charAt(++i));
                        continue;
                    }
                    inString = !inString;
                    current.append(ch);
                } else if (ch == ';' && !inString) {
                    String trimmed = current.toString().trim();
                    if (!trimmed.isEmpty()) {
                        statements.add(trimmed);
                    }
                    current.setLength(0);
                } else {
                    current.append(ch);
                }
            }
            String tail = current.toString().trim();
            if (!tail.isEmpty()) {
                statements.add(tail);
            }
            return statements;
        }
    }

    private static final Pattern ENV_VAR_PATTERN = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}");

    private static String substituteEnvVars(String input) {
        Matcher m = ENV_VAR_PATTERN.matcher(input);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String name = m.group(1);
            String value = System.getenv(name);
            if (value == null) {
                throw new IllegalStateException(
                    "Missing required environment variable referenced in SQL: " + name);
            }
            // Quote replacement to escape regex specials ($, \) inside the value.
            m.appendReplacement(sb, Matcher.quoteReplacement(value));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
