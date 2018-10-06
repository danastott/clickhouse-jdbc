package ru.yandex.clickhouse;

import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.clickhouse.response.ClickHouseResponse;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.ByteArray;
import ru.yandex.clickhouse.util.ByteBufferOutputStream;
import ru.yandex.clickhouse.util.ClickHouseArrayUtil;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ClickHousePreparedStatementImpl extends ClickHouseStatementImpl implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);
    private static final Pattern VALUES = Pattern.compile("(?i)VALUES[\\s]*\\(");
    private static final ByteArray declaredNullBytes = new ByteArray("\\N".getBytes());
    private static final ByteArray nullBytes = new ByteArray("null".getBytes());
    private static final ByteArray zeroBytes = new ByteArray("0".getBytes());
    private static final ByteArray oneBytes = new ByteArray("1".getBytes());
    private static final ByteArray tabBytes = new ByteArray("\t".getBytes());
    private static final ByteArray newLineBytes = new ByteArray("\n".getBytes());
    private static final ByteArray singleQuoteBytes = new ByteArray("'".getBytes());

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final ByteBufferOutputStream byteBuffer = new ByteBufferOutputStream(ByteBuffer.allocate(1024), true);

    private final String sql;
    private final List<byte[]> sqlParts;
    private ByteArray[] binds;
    private boolean[] valuesQuote;
    private List<byte[]> batchRows = new ArrayList<byte[]>();

    public ClickHousePreparedStatementImpl(CloseableHttpClient client, ClickHouseConnection connection,
                                           ClickHouseProperties properties, String sql, TimeZone timezone) throws SQLException {
        super(client, connection, properties);
        this.sql = sql;
        this.sqlParts = parseSql(sql);
        createBinds();
        initTimeZone(timezone);
    }

    private void createBinds() {
        this.binds = new ByteArray[this.sqlParts.size() - 1];
        for (int i = 0 ; i < this.sqlParts.size() - 1 ; i++) {
            binds[i] = new ByteArray();
        }
        this.valuesQuote = new boolean[this.sqlParts.size() - 1];
    }

    private void initTimeZone(TimeZone timeZone) {
        dateTimeFormat.setTimeZone(timeZone);
        if (properties.isUseServerTimeZoneForDates()) {
            dateFormat.setTimeZone(timeZone);
        }
    }

    @Override
    public void clearParameters() {
        for (ByteArray bind : binds) {
            bind.reset();
        }
        Arrays.fill(valuesQuote, false);
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse() throws SQLException {
        return super.executeQueryClickhouseResponse(buildSql());
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return super.executeQueryClickhouseResponse(buildSql(), additionalDBParams);
    }

    protected static List<byte[]> parseSql(String sql) throws SQLException {
        if (sql == null) {
            throw new SQLException("sql statement can't be null");
        }

        List<byte[]> parts = new ArrayList<byte[]>();

        boolean afterBackSlash = false, inQuotes = false, inBackQuotes = false;
        boolean inSingleLineComment = false;
        boolean inMultiLineComment = false;
        int partStart = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inSingleLineComment) {
                if (c == '\n') {
                    inSingleLineComment = false;
                }
            } else if (inMultiLineComment) {
                if (c == '*' && sql.length() > i + 1 && sql.charAt(i + 1) == '/') {
                    inMultiLineComment = false;
                    i++;
                }
            } else if (afterBackSlash) {
                afterBackSlash = false;
            } else if (c == '\\') {
                afterBackSlash = true;
            } else if (c == '\'') {
                inQuotes = !inQuotes;
            } else if (c == '`') {
                inBackQuotes = !inBackQuotes;
            } else if (!inQuotes && !inBackQuotes) {
                if (c == '?') {
                    parts.add(sql.substring(partStart, i).getBytes());
                    partStart = i + 1;
                } else if (c == '-' && sql.length() > i + 1 && sql.charAt(i + 1) == '-') {
                    inSingleLineComment = true;
                    i++;
                } else if (c == '/' && sql.length() > i + 1 && sql.charAt(i + 1) == '*') {
                    inMultiLineComment = true;
                    i++;
                }
            }
        }
        parts.add(sql.substring(partStart).getBytes(StreamUtils.UTF_8));

        return parts;
    }

    protected String buildSql() throws SQLException {
        if (sqlParts.size() == 1) {
            return new String(sqlParts.get(0));
        }
        checkBinded(binds);
        try {
            byteBuffer.reset();
            byteBuffer.write(sqlParts.get(0));
            for (int i = 1; i < sqlParts.size(); i++) {
                appendBoundValue(i - 1);
                byteBuffer.write(sqlParts.get(i));
            }

            return new String(byteBuffer.toByteArray(), StreamUtils.UTF_8);
        } catch (IOException ioe) {
            throw new SQLException("Unable to process batch", ioe);
        }
    }

    private void appendBoundValue(int num) throws IOException {
        ByteArray ba = binds[num];
        if (ba.length == declaredNullBytes.length &&
                ba.bytes[0] == declaredNullBytes.bytes[0] &&
                ba.bytes[1] == declaredNullBytes.bytes[1]) {
            byteBuffer.write(nullBytes.bytes);
        } else if (valuesQuote[num]) {
            byteBuffer.write(singleQuoteBytes.bytes);
            ba.writeTo(byteBuffer);
            byteBuffer.write(singleQuoteBytes.bytes);
        } else {
            ba.writeTo(byteBuffer);
        }
        valuesQuote[num] = false;
    }

    private static void checkBinded(ByteArray[] binds) throws SQLException {
        int i = 0;
        for (ByteArray b : binds) {
            ++i;
            if (b == null || b.bytes == null) {
                throw new SQLException("Not all parameters binded (placeholder " + i + " is undefined)");
            }
        }
    }

    private byte[] buildBinds() throws SQLException {
        checkBinded(binds);
        byteBuffer.reset();
        for (int i = 0; i < binds.length; i++) {
            binds[i].writeTo(byteBuffer);
            byteBuffer.write((i < binds.length - 1 ? tabBytes.bytes : newLineBytes.bytes));
            binds[i].reset();
        }
        return byteBuffer.toByteArray();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(buildSql());
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return super.executeQuery(buildSql(), additionalDBParams);
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams, List<ClickHouseExternalData> externalData) throws SQLException {
        return super.executeQuery(buildSql(), additionalDBParams, externalData);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(buildSql());
    }

    private void setBind(int parameterIndex, byte[] x) {
        setBind(parameterIndex, x, false);
    }

    private void setBind(int parameterIndex, byte[] x, boolean quote) {
        binds[parameterIndex - 1].write(x);
        valuesQuote[parameterIndex - 1] = quote;
    }

    private void setBind(int parameterIndex, byte[] x, int off, int length, boolean quote) {
        binds[parameterIndex - 1].write(x, off, length);
        valuesQuote[parameterIndex - 1] = quote;
    }

    private void setBind(int parameterIndex, ByteArray bind) {
        binds[parameterIndex - 1].write(bind);
        valuesQuote[parameterIndex - 1] = false;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setBind(parameterIndex, declaredNullBytes);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setBind(parameterIndex, x ? oneBytes : zeroBytes);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setBind(parameterIndex, new byte[]{x});
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setBind(parameterIndex, String.valueOf(x).getBytes());
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setBind(parameterIndex, String.valueOf(x).getBytes());
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setBind(parameterIndex, String.valueOf(x).getBytes());
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setBind(parameterIndex, String.valueOf(x).getBytes());
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setBind(parameterIndex, String.valueOf(x).getBytes());
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setBind(parameterIndex, x == null ? declaredNullBytes.bytes : x.toPlainString().getBytes());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setBind(parameterIndex, x == null ? declaredNullBytes.bytes : ClickHouseUtil.escape(x).getBytes(StreamUtils.UTF_8), x != null);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setBind(parameterIndex, x == null ? declaredNullBytes.bytes : x, true);
    }

    public void setBytes(int parameterIndex, byte[] x, int off, int length, int sqlType) throws SQLException {
        if (x == null || (length == 0 && Types.VARCHAR != sqlType)) {
            setBind(parameterIndex, declaredNullBytes);
        } else {
            setBind(parameterIndex, x, off, length, Types.VARCHAR == sqlType || Types.DATE == sqlType || Types.TIMESTAMP == sqlType);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setBind(parameterIndex, x == null ? declaredNullBytes.bytes : dateFormat.format(x).getBytes(StreamUtils.UTF_8), true);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
        //        setBind(parameterIndex, "toDateTime('" + dateTimeFormat.format(x) + "')");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setBind(parameterIndex, x == null ? declaredNullBytes.bytes : dateTimeFormat.format(x).getBytes(StreamUtils.UTF_8), true);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x == null ? declaredNullBytes.bytes : x);
    }

    @Override
    public void setArray(int parameterIndex, Collection collection) throws SQLException {
        setBind(parameterIndex, ClickHouseArrayUtil.toString(collection).getBytes(StreamUtils.UTF_8));
    }

    @Override
    public void setArray(int parameterIndex, Object[] array) throws SQLException {
        setBind(parameterIndex, ClickHouseArrayUtil.toString(array).getBytes(StreamUtils.UTF_8));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.OTHER);
        } else {
            if (x instanceof Byte) {
                setInt(parameterIndex, ((Byte) x).intValue());
            } else if (x instanceof String) {
                setString(parameterIndex, (String) x);
            } else if (x instanceof BigDecimal) {
                setBigDecimal(parameterIndex, (BigDecimal) x);
            } else if (x instanceof Short) {
                setShort(parameterIndex, ((Short) x).shortValue());
            } else if (x instanceof Integer) {
                setInt(parameterIndex, ((Integer) x).intValue());
            } else if (x instanceof Long) {
                setLong(parameterIndex, ((Long) x).longValue());
            } else if (x instanceof Float) {
                setFloat(parameterIndex, ((Float) x).floatValue());
            } else if (x instanceof Double) {
                setDouble(parameterIndex, ((Double) x).doubleValue());
            } else if (x instanceof byte[]) {
                setBytes(parameterIndex, (byte[]) x);
            } else if (x instanceof Date) {
                setDate(parameterIndex, (Date) x);
            } else if (x instanceof Time) {
                setTime(parameterIndex, (Time) x);
            } else if (x instanceof Timestamp) {
                setTimestamp(parameterIndex, (Timestamp) x);
            } else if (x instanceof Boolean) {
                setBoolean(parameterIndex, ((Boolean) x).booleanValue());
            } else if (x instanceof InputStream) {
                setBinaryStream(parameterIndex, (InputStream) x, -1);
            } else if (x instanceof Blob) {
                setBlob(parameterIndex, (Blob) x);
            } else if (x instanceof Clob) {
                setClob(parameterIndex, (Clob) x);
            } else if (x instanceof BigInteger) {
                setBind(parameterIndex, x.toString().getBytes(StreamUtils.UTF_8));
            } else if (x instanceof UUID) {
                setString(parameterIndex, x.toString());
            } else if (x instanceof Collection) {
                setArray(parameterIndex, (Collection) x);
            } else if (x.getClass().isArray()) {
                setArray(parameterIndex, (Object[]) x);
            } else {
                throw new SQLDataException("Can't bind object of class " + x.getClass().getCanonicalName());
            }
        }
    }


    @Override
    public boolean execute() throws SQLException {
        return super.execute(buildSql());
    }

    @Override
    public void addBatch() throws SQLException {
        byte[] binds = buildBinds();
        batchRows.add(binds);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return executeBatch(null);
    }

    @Override
    public int[] executeBatch(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        Matcher matcher = VALUES.matcher(sql);
        if (!matcher.find()) {
            throw new SQLSyntaxErrorException(
                    "Query must be like 'INSERT INTO [db.]table [(c1, c2, c3)] VALUES (?, ?, ?)'. " +
                            "Got: " + sql
            );
        }
        int valuePosition = matcher.start();

        String insertSql = sql.substring(0, valuePosition);
        BatchHttpEntity entity = new BatchHttpEntity(batchRows);
        sendStream(entity, insertSql, additionalDBParams);

        int[] result = new int[batchRows.size()];
        Arrays.fill(result, 1);
        batchRows.clear();
        return result;
    }

    private static class BatchHttpEntity extends AbstractHttpEntity {
        private final List<byte[]> rows;

        public BatchHttpEntity(List<byte[]> rows) {
            this.rows = rows;
        }

        @Override
        public boolean isRepeatable() {
            return true;
        }

        @Override
        public long getContentLength() {
            return -1;
        }

        @Override
        public InputStream getContent() throws IOException, IllegalStateException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(OutputStream outputStream) throws IOException {
            for (byte[] row : rows) {
                outputStream.write(row);
            }
        }

        @Override
        public boolean isStreaming() {
            return false;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setBind(parameterIndex, ClickHouseArrayUtil.arrayToString(x.getArray()).getBytes(StreamUtils.UTF_8));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
