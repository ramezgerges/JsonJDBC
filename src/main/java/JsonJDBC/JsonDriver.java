package JsonJDBC;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A JDBC driver implementation for JSON files.
 */
public class JsonDriver implements Driver {
    private static final Pattern regexPattern = Pattern.compile("^jdbc:JsonDriver://((?:\\w+\\/)+\\w+\\.json)$");

    @Override
    public Connection connect(String s, Properties properties) throws SQLException {
        if (!acceptsURL(s)) return null;

        try {
            final Matcher matcher = regexPattern.matcher(s);
            if (!matcher.matches()) return null;
            return new JsonConnection(matcher.group(1));
        } catch (IOException e) {
            throw new SQLNonTransientConnectionException(e.getMessage());
        }
    }

    @Override
    public boolean acceptsURL(String s) {
        final Matcher matcher = regexPattern.matcher(s);
        if (!matcher.matches()) return false;
        final String filePath = matcher.group(1);
        return Paths.get(filePath).toFile().exists();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
        throw new SQLFeatureNotSupportedException("getPropertyInfo is not implemented.");
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger is not implemented.");
    }
}
