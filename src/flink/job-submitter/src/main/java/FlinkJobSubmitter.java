import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.StatementSet;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FlinkJobSubmitter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
            stmtSet.addInsertSql(stmt);
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
}
