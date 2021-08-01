package JsonJDBC;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class JsonStatement implements Statement {
    private final int MAX_ROWS = 3;
    private final int QUERY_TIMEOUT = 2;

    private final List<String> batch = new ArrayList<>();
    private final Connection conn;
    private final JsonObject obj;
    private boolean isClosed = false;
    private int maxRows = MAX_ROWS;
    private int queryTimeout = QUERY_TIMEOUT;
    private int result = -1;
    private ResultSet resultSet = null;

    public JsonStatement(JsonObject obj, Connection conn) {
        this.obj = obj;
        this.conn = conn;
    }

    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        result = -1;
        Pattern selectPattern =
                Pattern.compile("^\\s*select\\s+(\\*|\\w+(?:\\s*,\\s*\\w+)*)\\s+from\\s+(\\w+)\\s*;\\s*$",
                        Pattern.CASE_INSENSITIVE);
        Matcher matcher = selectPattern.matcher(s);
        if (!matcher.matches()) throw new SQLSyntaxErrorException("Invalid query");

        String[] colNames = matcher.group(1).split("\\s*,\\s*");
        String tabName = matcher.group(2);
        boolean grabAllColumns = matcher.group(1).equals("*");

        if (!obj.has(tabName)) throw new SQLNonTransientException("Database doesn't contain table " + tabName);
        JsonArray rows = obj.getAsJsonArray(tabName);

        List<List<?>> rowList = new ArrayList<>();
        rowList.add(null); //resultSets start at 1
        boolean firstRow = true;
        for (JsonElement row : rows) {
            if (!row.isJsonObject()) throw new SQLDataException("Rows in database aren't objects");
            JsonObject rowObject = row.getAsJsonObject();
            if (grabAllColumns && firstRow) {
                firstRow = false;
                List<String> newColNames = new ArrayList<>();
                for (Map.Entry<String, JsonElement> entry : rowObject.entrySet())
                    newColNames.add(entry.getKey());
                colNames = newColNames.toArray(new String[0]);
            }
            List<Object> r = new ArrayList<>();
            for (String col : colNames) {
                if (!rowObject.has(col)) throw new SQLDataException("Column " + col + " not found in some rows");
                JsonElement e = rowObject.get(col);
                if (!e.isJsonPrimitive()) throw new SQLDataException("Only primitives are allowed in the database");
                JsonPrimitive p = e.getAsJsonPrimitive();
                if (p.isString()) {
                    r.add(p.getAsString());
                } else if (p.isBoolean()) {
                    r.add(p.getAsBoolean());
                } else if (p.isNumber()) {
                    r.add(p.getAsDouble());
                }
            }
            rowList.add(r);
        }

        resultSet = new JsonResultSet(rowList, Arrays.asList(colNames), this);
        return resultSet;
    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int i) {

    }

    @Override
    public int getMaxRows() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to get max rows after closing the statement");
        return maxRows;
    }

    @Override
    public void setMaxRows(int i) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to set max rows after closing the statement");
        if (i < 0) throw new SQLNonTransientException("Argument cannot be less than zero");
        maxRows = i;
    }

    @Override
    public void setEscapeProcessing(boolean b) {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to get query timeout after closing the statement");
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int i) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to set query timeout after closing the statement");
        if (i < 0) throw new SQLNonTransientException("Argument cannot be less than zero");
        queryTimeout = i;
    }

    @Override
    public void cancel() {

    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {

    }

    @Override
    public void setCursorName(String s) {

    }

    @Override
    public boolean execute(String s) {
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to get ResultSet after statement is closed.");
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to get update count after closing the statement");
        return result == 0 ? -1 : result;
    }

    @Override
    public boolean getMoreResults() {
        return false;
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int i) {

    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public void setFetchSize(int i) {

    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String s) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to add to batch after closing the statement");
        batch.add(s);
    }

    @Override
    public void clearBatch() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to clear batch after closing the statement");
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to execute a batch after closing the statement");
        int[] results = new int[batch.size()];
        for (int i = 0; i <= batch.size(); i++) results[i] = executeUpdate(batch.get(i));
        return results;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to get connection after closing the statement");
        return conn;
    }

    @Override
    public boolean getMoreResults(int i) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() {
        return null;
    }

    @Override
    public int executeUpdate(String s, int i) {
        return 0;
    }

    @Override
    public int executeUpdate(String s, int[] ints) {
        return 0;
    }

    @Override
    public int executeUpdate(String s, String[] strings) {
        return 0;
    }

    @Override
    public boolean execute(String s, int i) {
        return false;
    }

    @Override
    public boolean execute(String s, int[] ints) {
        return false;
    }

    @Override
    public boolean execute(String s, String[] strings) {
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void setPoolable(boolean b) {

    }

    @Override
    public void closeOnCompletion() {

    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) {
        return false;
    }
}
