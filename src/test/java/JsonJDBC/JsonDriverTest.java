package JsonJDBC;

import com.google.gson.Gson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.CONCURRENT)
public class JsonDriverTest {
    final String connectionUrl = "jdbc:JsonDriver://src/test/resources/";

    @BeforeAll
    static void setup() throws ClassNotFoundException, SQLException {
        Class.forName("JsonJDBC.JsonDriver");
        DriverManager.registerDriver(new JsonDriver());
    }

    static List<Arguments> validTest1() throws IOException {
        String dirName = JsonDriverTest.class.getResource("/JDBCDriverValidTests").getFile();
        File dir = new File(dirName);
        File[] listOfFiles = dir.listFiles();
        Gson gson = new Gson();

        if (listOfFiles == null) throw new IOException("Error reading directory: " + dir);
        return Arrays.stream(listOfFiles).parallel().map((file) -> {
            try (FileReader reader = new FileReader(file)) {
                Map<?, ?> map = gson.fromJson(reader, Map.class);
                String database = (String) map.get("database");
                String query = (String) map.get("query");
                List<?> data = (List<?>) map.get("data");
                return Arguments.of(query, data, database);
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }).collect(Collectors.toList());
    }

    static List<Arguments> invalidTest1() throws IOException {
        String filename = JsonDriverTest.class.getResource("/JDBCDriverInvalidTests.json").getFile();
        try (FileReader reader = new FileReader(filename)) {
            List<?> tests = new Gson().fromJson(reader, List.class);
            return tests.stream().map((testObject) -> {
                @SuppressWarnings("unchecked") Map<String, String> test = (Map<String, String>) testObject;
                return Arguments.of(test.get("query"), test.get("database"));
            }).collect(Collectors.toList());
        }
    }

    @ParameterizedTest
    @DisplayName("Valid test #1")
    @MethodSource("validTest1")
    public void validTest1(String query, List<?> data, String database) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionUrl + database);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            for (Object rowObject : data) {
                assertFalse(rs.isLast());
                rs.next();
                @SuppressWarnings("unchecked") Map<String, ?> row = (Map<String, ?>) rowObject;
                row.keySet().forEach((String key) -> {
                    try {
                        if (row.get(key) instanceof Double) {
                            assertEquals(row.get(key), rs.getDouble(key));
                        } else if (row.get(key) instanceof String) {
                            assertEquals(row.get(key), rs.getString(key));
                        } else throw new RuntimeException("only doubles and strings are supported");
                    } catch (SQLException e) {
                        throw new RuntimeException(e.getMessage());
                    }
                });
            }
            assertTrue(rs.isLast());
        }
    }

    @ParameterizedTest
    @DisplayName("Invalid test #1")
    @MethodSource("invalidTest1")
    public void invalidTest1(String query, String database) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionUrl + database);
             Statement stmt = conn.createStatement()) {
            Assertions.assertThrows(SQLException.class, () -> stmt.executeQuery(query));
        }
    }
}
