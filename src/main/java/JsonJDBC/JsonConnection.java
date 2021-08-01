package JsonJDBC;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

class JsonConnection implements Connection {
    private final String fileString;
    private boolean isReadOnly = true;
    private boolean isClosed = false;

    public JsonConnection(String s) throws IOException {
        FileReader reader = new FileReader(s);
        fileString = Files.readString(Paths.get(s));
        reader.close();
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Statements cannot be created after closing the connection");
        return new JsonStatement(new Gson().fromJson(fileString, JsonObject.class), this);
    }

    @Override
    public PreparedStatement prepareStatement(String s) {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String s) {
        return null;
    }

    @Override
    public String nativeSQL(String s) {
        return null;
    }

    @Override
    public boolean getAutoCommit() {
        return true;
    }

    @Override
    public void setAutoCommit(boolean b) throws SQLException {
        throw new SQLFeatureNotSupportedException("auto-commit is always on in this implementation");
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("auto-commit is always on in this implementation");
    }

    @Override
    public void rollback() {

    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return isReadOnly;
    }

    @Override
    public void setReadOnly(boolean b) {
        isReadOnly = b;
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setCatalog(String s) {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public void setTransactionIsolation(int i) throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int i, int i1) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public void setHoldability(int i) throws SQLException {

    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String s) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int i, int i1, int i2) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int i) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(String s, String s1) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String s) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        return null;
    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String s) throws SQLException {

    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int i) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;
    }
}
