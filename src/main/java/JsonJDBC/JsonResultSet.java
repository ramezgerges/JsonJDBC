package JsonJDBC;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

class JsonResultSet implements ResultSet {
    private final List<List<?>> results;
    private final List<String> names;
    private final Statement statement;
    private int index = 0;
    private boolean isClosed = false;

    public JsonResultSet(List<List<?>> results, List<String> names, Statement statement) {
        this.results = results;
        this.names = names;
        this.statement = statement;
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call next() after closing the set");
        index++;
        return index > 0 && index < results.size();
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call close() after closing the set");

        isClosed = true;
    }

    @Override
    public boolean wasNull() {
        return false;
    }

    @Override
    public String getString(int i) {
        return null;
    }

    @Override
    public boolean getBoolean(int i) {
        return false;
    }

    @Override
    public byte getByte(int i) {
        return 0;
    }

    @Override
    public short getShort(int i) {
        return 0;
    }

    @Override
    public int getInt(int i) {
        return 0;
    }

    @Override
    public long getLong(int i) {
        return 0;
    }

    @Override
    public float getFloat(int i) {
        return 0;
    }

    @Override
    public double getDouble(int i) {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int i, int i1) {
        return null;
    }

    @Override
    public byte[] getBytes(int i) {
        return new byte[0];
    }

    @Override
    public Date getDate(int i) {
        return null;
    }

    @Override
    public Time getTime(int i) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int i) {
        return null;
    }

    @Override
    public InputStream getAsciiStream(int i) {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int i) {
        return null;
    }

    @Override
    public InputStream getBinaryStream(int i) {
        return null;
    }

    @Override
    public String getString(String s) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call getString() after closing the set");
        int i = names.indexOf(s);
        if (i == -1) throw new SQLNonTransientException("Column name " + s + " doesn't exist in result set");
        Object o = results.get(index).get(i);
        if (!(o instanceof String)) throw new SQLNonTransientException("Column " + s + " is not of type String");
        return (String) o;
    }

    @Override
    public boolean getBoolean(String s) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call getBoolean() after closing the set");
        int i = names.indexOf(s);
        if (i == -1) throw new SQLNonTransientException("Column name " + s + " doesn't exist in result set");
        Object o = results.get(index).get(i);
        if (!(o instanceof Boolean)) throw new SQLNonTransientException("Column " + s + " is not of type Boolean");
        return (Boolean) o;
    }

    @Override
    public byte getByte(String s) {
        return 0;
    }

    @Override
    public short getShort(String s) {
        return 0;
    }

    @Override
    public int getInt(String s) {
        return 0;
    }

    @Override
    public long getLong(String s) {
        return 0;
    }

    @Override
    public float getFloat(String s) {
        return 0;
    }

    @Override
    public double getDouble(String s) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call getDouble() after closing the set");
        int i = names.indexOf(s);
        if (i == -1) throw new SQLNonTransientException("Column name " + s + " doesn't exist in result set");
        Object o = results.get(index).get(i);
        if (!(o instanceof Double)) throw new SQLNonTransientException("Column " + s + " is not of type Double");
        return (Double) o;
    }

    @Override
    public BigDecimal getBigDecimal(String s, int i) {
        return null;
    }

    @Override
    public byte[] getBytes(String s) {
        return new byte[0];
    }

    @Override
    public Date getDate(String s) {
        return null;
    }

    @Override
    public Time getTime(String s) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String s) {
        return null;
    }

    @Override
    public InputStream getAsciiStream(String s) {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String s) {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String s) {
        return null;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {

    }

    @Override
    public String getCursorName() {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return null;
    }

    @Override
    public Object getObject(int i) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call getObject() after closing the set");
        i--;
        if (i >= names.size()) throw new SQLNonTransientException("Index out of range");
        return results.get(index).get(i);
    }

    @Override
    public Object getObject(String s) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call getObject() after closing the set");
        int i = names.indexOf(s);
        if (i == -1) throw new SQLNonTransientException("Column name " + s + " doesn't exist in result set");
        return results.get(index).get(i);
    }

    @Override
    public int findColumn(String s) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call findColumn() after closing the set");
        return names.indexOf(s) + 1; //columns are 1-indexed
    }

    @Override
    public Reader getCharacterStream(int i) {
        return null;
    }

    @Override
    public Reader getCharacterStream(String s) {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int i) {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String s) {
        return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call isBeforeFirst() after closing the set");
        return index == 0 && !results.isEmpty();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call isAfterLast() after closing the set");
        return results.size() == index && !results.isEmpty();
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call isFirst() after closing the set");
        return index == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call isLast() after closing the set");
        return index == results.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call beforeFirst() after closing the set");

        index = 0;
    }

    @Override
    public void afterLast() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call afterLast() after closing the set");

        index = results.size();
    }

    @Override
    public boolean first() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call first() after closing the set");
        index = 1;
        return !results.isEmpty();
    }

    @Override
    public boolean last() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call last() after closing the set");
        index = results.size() - 1;
        return !results.isEmpty();
    }

    @Override
    public int getRow() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call getRow() after closing the set");
        return index;
    }

    @Override
    public boolean absolute(int i) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call absolute() after closing the set");

        if (i >= 0) index = i;
        else index = results.size() - i;

        if (index < 0) {
            index = 0;
            return false;
        } else if (index > results.size()) {
            index = results.size();
            return false;
        }

        return true;
    }

    @Override
    public boolean relative(int i) throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call relative() after closing the set");
        index += i;
        return index > 0 && index < results.size();
    }

    @Override
    public boolean previous() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call absolute() after closing the set");
        index--;
        return index > 0 && index < results.size();
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
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public void updateNull(int i) {

    }

    @Override
    public void updateBoolean(int i, boolean b) {

    }

    @Override
    public void updateByte(int i, byte b) {

    }

    @Override
    public void updateShort(int i, short i1) {

    }

    @Override
    public void updateInt(int i, int i1) {

    }

    @Override
    public void updateLong(int i, long l) {

    }

    @Override
    public void updateFloat(int i, float v) {

    }

    @Override
    public void updateDouble(int i, double v) {

    }

    @Override
    public void updateBigDecimal(int i, BigDecimal bigDecimal) {

    }

    @Override
    public void updateString(int i, String s) {

    }

    @Override
    public void updateBytes(int i, byte[] bytes) {

    }

    @Override
    public void updateDate(int i, Date date) {

    }

    @Override
    public void updateTime(int i, Time time) {

    }

    @Override
    public void updateTimestamp(int i, Timestamp timestamp) {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, int i1) {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, int i1) {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader, int i1) {

    }

    @Override
    public void updateObject(int i, Object o, int i1) throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public void updateObject(int i, Object o) throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public void updateNull(String s) {

    }

    @Override
    public void updateBoolean(String s, boolean b) {

    }

    @Override
    public void updateByte(String s, byte b) {

    }

    @Override
    public void updateShort(String s, short i) {

    }

    @Override
    public void updateInt(String s, int i) {

    }

    @Override
    public void updateLong(String s, long l) {

    }

    @Override
    public void updateFloat(String s, float v) {

    }

    @Override
    public void updateDouble(String s, double v) {

    }

    @Override
    public void updateBigDecimal(String s, BigDecimal bigDecimal) {

    }

    @Override
    public void updateString(String s, String s1) {

    }

    @Override
    public void updateBytes(String s, byte[] bytes) {

    }

    @Override
    public void updateDate(String s, Date date) {

    }

    @Override
    public void updateTime(String s, Time time) {

    }

    @Override
    public void updateTimestamp(String s, Timestamp timestamp) {

    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, int i) {

    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, int i) {

    }

    @Override
    public void updateCharacterStream(String s, Reader reader, int i) {

    }

    @Override
    public void updateObject(String s, Object o, int i) throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public void updateObject(String s, Object o) throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public void updateRow() {

    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLNonTransientException("Not implemented");
    }

    @Override
    public void refreshRow() {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLNonTransientException("Not implemented");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLNonTransientException("Not implemented");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLNonTransientException("Not implemented");
    }

    @Override
    public Statement getStatement() throws SQLException {
        if (isClosed()) throw new SQLNonTransientException("Attempt to call getStatement() after closing the set");
        return statement;
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        throw new SQLNonTransientException("not implemented");
    }

    @Override
    public Ref getRef(int i) {
        return null;
    }

    @Override
    public Blob getBlob(int i) {
        return null;
    }

    @Override
    public Clob getClob(int i) {
        return null;
    }

    @Override
    public Array getArray(int i) {
        return null;
    }

    @Override
    public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public Ref getRef(String s) {
        return null;
    }

    @Override
    public Blob getBlob(String s) {
        return null;
    }

    @Override
    public Clob getClob(String s) {
        return null;
    }

    @Override
    public Array getArray(String s) {
        return null;
    }

    @Override
    public Date getDate(int i, Calendar calendar) {
        return null;
    }

    @Override
    public Date getDate(String s, Calendar calendar) {
        return null;
    }

    @Override
    public Time getTime(int i, Calendar calendar) {
        return null;
    }

    @Override
    public Time getTime(String s, Calendar calendar) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int i, Calendar calendar) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String s, Calendar calendar) {
        return null;
    }

    @Override
    public URL getURL(int i) {
        return null;
    }

    @Override
    public URL getURL(String s) {
        return null;
    }

    @Override
    public void updateRef(int i, Ref ref) {

    }

    @Override
    public void updateRef(String s, Ref ref) {

    }

    @Override
    public void updateBlob(int i, Blob blob) {

    }

    @Override
    public void updateBlob(String s, Blob blob) {

    }

    @Override
    public void updateClob(int i, Clob clob) {

    }

    @Override
    public void updateClob(String s, Clob clob) {

    }

    @Override
    public void updateArray(int i, Array array) {

    }

    @Override
    public void updateArray(String s, Array array) {

    }

    @Override
    public RowId getRowId(int i) {
        return null;
    }

    @Override
    public RowId getRowId(String s) {
        return null;
    }

    @Override
    public void updateRowId(int i, RowId rowId) {

    }

    @Override
    public void updateRowId(String s, RowId rowId) {

    }

    @Override
    public int getHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void updateNString(int i, String s) {

    }

    @Override
    public void updateNString(String s, String s1) {

    }

    @Override
    public void updateNClob(int i, NClob nClob) {

    }

    @Override
    public void updateNClob(String s, NClob nClob) {

    }

    @Override
    public NClob getNClob(int i) {
        return null;
    }

    @Override
    public NClob getNClob(String s) {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int i) {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String s) {
        return null;
    }

    @Override
    public void updateSQLXML(int i, SQLXML sqlxml) {

    }

    @Override
    public void updateSQLXML(String s, SQLXML sqlxml) {

    }

    @Override
    public String getNString(int i) {
        return null;
    }

    @Override
    public String getNString(String s) {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int i) {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String s) {
        return null;
    }

    @Override
    public void updateNCharacterStream(int i, Reader reader, long l) {

    }

    @Override
    public void updateNCharacterStream(String s, Reader reader, long l) {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, long l) {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, long l) {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader, long l) {

    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, long l) {

    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, long l) {

    }

    @Override
    public void updateCharacterStream(String s, Reader reader, long l) {

    }

    @Override
    public void updateBlob(int i, InputStream inputStream, long l) {

    }

    @Override
    public void updateBlob(String s, InputStream inputStream, long l) {

    }

    @Override
    public void updateClob(int i, Reader reader, long l) {

    }

    @Override
    public void updateClob(String s, Reader reader, long l) {

    }

    @Override
    public void updateNClob(int i, Reader reader, long l) {

    }

    @Override
    public void updateNClob(String s, Reader reader, long l) {

    }

    @Override
    public void updateNCharacterStream(int i, Reader reader) {

    }

    @Override
    public void updateNCharacterStream(String s, Reader reader) {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream) {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream) {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader) {

    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream) {

    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream) {

    }

    @Override
    public void updateCharacterStream(String s, Reader reader) {

    }

    @Override
    public void updateBlob(int i, InputStream inputStream) {

    }

    @Override
    public void updateBlob(String s, InputStream inputStream) {

    }

    @Override
    public void updateClob(int i, Reader reader) {

    }

    @Override
    public void updateClob(String s, Reader reader) {

    }

    @Override
    public void updateNClob(int i, Reader reader) {

    }

    @Override
    public void updateNClob(String s, Reader reader) {

    }

    @Override
    public <T> T getObject(int i, Class<T> aClass) throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
    }

    @Override
    public <T> T getObject(String s, Class<T> aClass) throws SQLException {
        throw new SQLNonTransientException("Not Implemented");
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
