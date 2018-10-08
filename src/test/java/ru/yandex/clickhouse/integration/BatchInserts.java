package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.ClickHousePreparedStatementImpl;
import ru.yandex.clickhouse.LocalSettings;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.*;
import java.util.Collections;

public class BatchInserts {
    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://" + LocalSettings.getHost() + ":" + LocalSettings.getPort(), properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test
    public void batchInsert() throws Exception {

        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.batch_insert (i Int32, s String) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.batch_insert (s, i) VALUES (?, ?)");

        statement.setString(1, "string1");
        statement.setInt(2, 21);
        statement.addBatch();

        statement.setString(1, "string2");
        statement.setInt(2, 32);
        statement.addBatch();

        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from test.batch_insert");
        rs.next();

        Assert.assertEquals(rs.getInt("cnt"), 2);

        Assert.assertFalse(rs.next());

    }

    @Test
    public void batchInsert2() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert2");
        connection.createStatement().execute(
                "CREATE TABLE test.batch_insert2 (" +
                        "date Date," +
                        "date_time DateTime," +
                        "string String," +
                        "int32 Int32," +
                        "float64 Float64" +
                        ") ENGINE = MergeTree(date, (date), 8192)"
        );

        Date date = new Date(602110800000L); //1989-01-30
        Timestamp dateTime = new Timestamp(1471008092000L); //2016-08-12 16:21:32
        String string = "testString";
        int int32 = Integer.MAX_VALUE;
        double float64 = 42.21;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO test.batch_insert2 (date, date_time, string, int32, float64) VALUES (?, ?, ?, ?, ?)"
        );

        statement.setDate(1, date);
        statement.setTimestamp(2, dateTime);
        statement.setString(3, string);
        statement.setInt(4, int32);
        statement.setDouble(5, float64);
        statement.addBatch();
        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.batch_insert2");
        Assert.assertTrue(rs.next());

        Assert.assertEquals(rs.getDate("date"), date);
        Assert.assertEquals(rs.getTimestamp("date_time"), dateTime);
        Assert.assertEquals(rs.getString("string"), string);
        Assert.assertEquals(rs.getInt("int32"), int32);
        Assert.assertEquals(rs.getDouble("float64"), float64);

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSimpleInsert() throws Exception{
        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert");
        connection.createStatement().execute(
                "CREATE TABLE test.insert (" +
                        "date Date," +
                        "date_time DateTime," +
                        "string String," +
                        "int32 Int32," +
                        "float64 Float64" +
                        ") ENGINE = MergeTree(date, (date), 8192)"
        );

        Date date = new Date(602110800000L); //1989-01-30
        Timestamp dateTime = new Timestamp(1471008092000L); //2016-08-12 16:21:32
        String string = "testString";
        int int32 = Integer.MAX_VALUE;
        double float64 = 42.21;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO test.insert (date, date_time, string, int32, float64) VALUES (?, ?, ?, ?, ?)"
        );

        statement.setDate(1, date);
        statement.setTimestamp(2, dateTime);
        statement.setString(3, string);
        statement.setInt(4, int32);
        statement.setDouble(5, float64);

        statement.execute();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.insert");
        Assert.assertTrue(rs.next());

        Assert.assertEquals(rs.getDate("date"), date);
        Assert.assertEquals(rs.getTimestamp("date_time"), dateTime);
        Assert.assertEquals(rs.getString("string"), string);
        Assert.assertEquals(rs.getInt("int32"), int32);
        Assert.assertEquals(rs.getDouble("float64"), float64);

        Assert.assertFalse(rs.next());
    }


    @Test
    public void batchInsertNulls() throws Exception {
        batchInsertObjectNulls(null);
    }

    @Test
    public void batchInsertDeclaredNulls() throws Exception {
        batchInsertObjectNulls("\\N");
    }

    @Test
    public void batchInsertEmpties() throws Exception {
        batchInsertObjectNulls("");
    }

    public void batchInsertObjectNulls(String nullValue) throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert_nulls");
        connection.createStatement().execute(
                        "CREATE TABLE test.batch_insert_nulls (" +
                                        "date Date," +
                                        "date_time Nullable(DateTime)," +
                                        "string Nullable(String)," +
                                        "int32 Nullable(Int32)," +
                                        "float64 Nullable(Float64)" +
                                        ") ENGINE = MergeTree(date, (date), 8192)"
                        );

        ClickHousePreparedStatement statement = (ClickHousePreparedStatement) connection.prepareStatement(
                "INSERT INTO test.batch_insert_nulls (date, date_time, string, int32, float64) VALUES (?, ?, ?, ?, ?)"
                );

        Date date = new Date(602110800000L); //1989-01-30
        statement.setDate(1, date);
        statement.setObject(2, nullValue, Types.TIMESTAMP);
        statement.setObject(3, nullValue, Types.VARCHAR);
        statement.setObject(4, nullValue, Types.INTEGER);
        statement.setObject(5, nullValue, Types.DOUBLE);
        statement.addBatch();
        statement.executeBatch(Collections.singletonMap(ClickHouseQueryParam.CONNECT_TIMEOUT, "1000"));

        ResultSet rs = connection.createStatement().executeQuery("SELECT date, date_time, string, int32, float64 from test.batch_insert_nulls");
        Assert.assertTrue(rs.next());

        Assert.assertEquals(rs.getDate("date"), date);
        Assert.assertNull(rs.getTimestamp("date_time"));
        Assert.assertNull(rs.getString("string"));
        Assert.assertEquals(rs.getInt("int32"), 0);
        Assert.assertNull(rs.getObject("int32"));
        Assert.assertEquals(rs.getDouble("float64"), 0.0);
        Assert.assertNull(rs.getObject("float64"));

        Assert.assertFalse(rs.next());
        connection.createStatement().execute("DROP TABLE test.batch_insert_nulls");
    }

    @Test
    public void batchInsertEmptyBytes() throws Exception {
        batchInsertBytes("");
    }

    @Test
    public void batchInsertDeclaredEmptyBytes() throws Exception {
        batchInsertBytes("\\N");
    }

    public void batchInsertBytes(String value) throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert_nulls");
        connection.createStatement().execute(
                        "CREATE TABLE test.batch_insert_nulls (" +
                                        "date Date," +
                                        "date_time Nullable(DateTime)," +
                                        "string Nullable(String)," +
                                        "int32 Nullable(Int32)," +
                                        "float64 Nullable(Float64)" +
                                        ") ENGINE = MergeTree(date, (date), 8192)"
                        );

        ClickHousePreparedStatementImpl statement = (ClickHousePreparedStatementImpl) connection.prepareStatement(
                "INSERT INTO test.batch_insert_nulls (date, date_time, string, int32, float64) VALUES (?, ?, ?, ?, ?)"
                );
        Date date = new Date(602110800000L); //1989-01-30
        statement.setDate(1, date);
        statement.setBytes(2, value.getBytes(), 0, 0, Types.TIMESTAMP);
        statement.setBytes(3, value.getBytes(), 0, 0, Types.VARCHAR);
        statement.setBytes(4, value.getBytes(), 0, 0, Types.INTEGER);
        statement.setBytes(5, value.getBytes(), 0, 0, Types.DOUBLE);
        statement.addBatch();
        statement.executeBatch(Collections.singletonMap(ClickHouseQueryParam.CONNECT_TIMEOUT, "1000"));

        ResultSet rs = connection.createStatement().executeQuery("SELECT date, date_time, string, int32, float64 from test.batch_insert_nulls");
        Assert.assertTrue(rs.next());

        Assert.assertNull(rs.getTimestamp("date_time"));
        Assert.assertNull(rs.getString("string"));
        Assert.assertEquals(rs.getInt("int32"), 0);
        Assert.assertNull(rs.getObject("int32"));
        Assert.assertEquals(rs.getDouble("float64"), 0.0);
        Assert.assertNull(rs.getObject("float64"));

        Assert.assertFalse(rs.next());
        connection.createStatement().execute("DROP TABLE test.batch_insert_nulls");
    }

    @Test
    public void testBatchValuesColumn() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_single_test");
        connection.createStatement().execute(
                "CREATE TABLE test.batch_single_test(date Date, values String) ENGINE = StripeLog"
        );

        PreparedStatement st = connection.prepareStatement("INSERT INTO test.batch_single_test (date, values) VALUES (?, ?)");
        st.setDate(1, new Date(System.currentTimeMillis()));
        st.setString(2, "test");

        st.addBatch();
        st.executeBatch();
    }

    @Test
    public void batchDeleteSimple() throws Exception {

        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.batch_insert (i Int32, s String) ENGINE = MergeTree partition by i order by i"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.batch_insert (s, i) VALUES (?, ?)");

        statement.setString(1, "string1");
        statement.setInt(2, 21);
        statement.addBatch();

        statement.setString(1, "string2");
        statement.setInt(2, 32);
        statement.addBatch();

        statement.executeBatch();

        PreparedStatement st = connection.prepareStatement("ALTER TABLE test.batch_insert DELETE WHERE i = ?");
        st.setInt(1, 21);
        st.addBatch();
        st.setInt(1, 32);
        st.addBatch();

        st.executeBatch();

        int count = 1;
        for (int i = 0 ; i < 50 ; i++) {
            st = connection.prepareStatement("select count(1) from test.batch_insert");
            ResultSet rs = st.executeQuery();
            Assert.assertTrue(rs.next());
            if (rs.getInt(1) == 0) {
                count = 0;
                break;
            }
            // eventually consistent so we'll wait for a while
            Thread.sleep(100);
        }
        Assert.assertEquals(count, 0);

    }

    @Test
    public void batchDeleteMultiplePredicates() throws Exception {

        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.batch_insert (i Int32, s String) ENGINE = MergeTree partition by i order by i"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.batch_insert (s, i) VALUES (?, ?)");

        statement.setString(1, "string1");
        statement.setInt(2, 21);
        statement.addBatch();

        statement.setString(1, "string2");
        statement.setInt(2, 32);
        statement.addBatch();

        statement.executeBatch();

        PreparedStatement st = connection.prepareStatement("ALTER TABLE test.batch_insert DELETE WHERE i = ? and s = ?");
        st.setInt(1, 21);
        st.setString(2, "string1");
        st.addBatch();
        st.setInt(1, 32);
        st.setString(2, "string2");
        st.addBatch();

        st.executeBatch();

        int count = 1;
        for (int i = 0 ; i < 50 ; i++) {
            st = connection.prepareStatement("select count(1) from test.batch_insert");
            ResultSet rs = st.executeQuery();
            Assert.assertTrue(rs.next());
            if (rs.getInt(1) == 0) {
                count = 0;
                break;
            }
            // eventually consistent so we'll wait for a while
            Thread.sleep(100);
        }
        Assert.assertEquals(count, 0);

    }
}
