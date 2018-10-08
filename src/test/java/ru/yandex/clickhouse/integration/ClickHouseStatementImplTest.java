package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseExternalData;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.LocalSettings;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;

import static org.testng.Assert.assertEquals;

public class ClickHouseStatementImplTest {
    private ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://" + LocalSettings.getHost() + ":" + LocalSettings.getPort(), properties);
        connection = (ClickHouseConnection) dataSource.getConnection();
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testUpdateCountForSelect() throws Exception {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT dummy FROM system.one");
        Assert.assertEquals(stmt.getUpdateCount(), -1);
        rs.next();
        Assert.assertEquals(stmt.getUpdateCount(), -1);
        rs.close();
        stmt.close();
    }

    @Test
    public void testSingleColumnResultSet() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select c from (\n" +
                "    select 'a' as c, 1 as rn\n" +
                "    UNION ALL select 'b' as c, 2 as rn\n" +
                "    UNION ALL select '' as c, 3 as rn\n" +
                "    UNION ALL select 'd' as c, 4 as rn\n" +
                " ) order by rn");
        StringBuffer sb = new StringBuffer();
        while (rs.next()) {
            sb.append(rs.getString("c")).append("\n");
        }
        Assert.assertEquals(sb.toString(), "a\nb\n\nd\n");
    }

    @Test
    public void readsPastLastAreSafe() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select c from (\n" +
                "    select 'a' as c, 1 as rn\n" +
                "    UNION ALL select 'b' as c, 2 as rn\n" +
                "    UNION ALL select '' as c, 3 as rn\n" +
                "    UNION ALL select 'd' as c, 4 as rn\n" +
                " ) order by rn");
        StringBuffer sb = new StringBuffer();
        while (rs.next()) {
            sb.append(rs.getString("c")).append("\n");
        }
        Assert.assertFalse(rs.next());
        Assert.assertFalse(rs.next());
        Assert.assertFalse(rs.next());
        Assert.assertEquals(sb.toString(), "a\nb\n\nd\n");
    }

    @Test
    public void testSelectUInt32() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select toUInt32(10), toUInt32(-10)");
        rs.next();
        Object smallUInt32 = rs.getObject(1);
        Assert.assertTrue(smallUInt32 instanceof Long);
        Assert.assertEquals(((Long)smallUInt32).longValue(), 10);
        Object bigUInt32 = rs.getObject(2);
        Assert.assertTrue(bigUInt32 instanceof Long);
        Assert.assertEquals(((Long)bigUInt32).longValue(), 4294967286L);
    }

    @Test
    public void testSelectUInt64() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select toUInt64(10), toUInt64(-10)");
        rs.next();
        Object smallUInt64 = rs.getObject(1);
        Assert.assertTrue(smallUInt64 instanceof BigInteger);
        Assert.assertEquals(((BigInteger)smallUInt64).intValue(), 10);
        Object bigUInt64 = rs.getObject(2);
        Assert.assertTrue(bigUInt64 instanceof BigInteger);
        Assert.assertEquals(bigUInt64, new BigInteger("18446744073709551606"));
    }

    @Test
    public void testExternalData() throws SQLException, UnsupportedEncodingException {
        ClickHouseStatement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(
                "select UserName, GroupName " +
                        "from (select 'User' as UserName, 1 as GroupId) " +
                        "any left join groups using GroupId",
                null,
                Collections.singletonList(new ClickHouseExternalData(
                        "groups",
                        new ByteArrayInputStream("1\tGroup".getBytes())
                ).withStructure("GroupId UInt8, GroupName String"))
        );

        rs.next();

        String userName = rs.getString("UserName");
        String groupName = rs.getString("GroupName");

        Assert.assertEquals(userName, "User");
        Assert.assertEquals(groupName, "Group");
    }

    @Test
    public void testResultSetWithExtremes() throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setExtremes(true);
        ClickHouseDataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://" + LocalSettings.getHost() + ":" + LocalSettings.getPort(), properties);
        Connection connection = dataSource.getConnection();

        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select 1");
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1)).append("\n");
            }

            Assert.assertEquals(sb.toString(), "1\n");
        } finally {
            connection.close();
        }
    }

    @Test
    public void testSelectManyRows() throws SQLException {
        Statement stmt = connection.createStatement();
        int limit = 10000;
        ResultSet rs = stmt.executeQuery("select concat('test', toString(number)) as str from system.numbers limit " + limit);
        int i = 0;
        while (rs.next()) {
            String s = rs.getString("str");
            Assert.assertEquals(s, "test" + i);
            i++;
        }
        Assert.assertEquals(i, limit);
    }

    @Test
    public void testMoreResultsWithResultSet() throws SQLException {
        Statement stmt = connection.createStatement();

        Assert.assertTrue(stmt.execute("select 1"));
        Assert.assertNotNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);

        Assert.assertFalse(stmt.getMoreResults());
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);
    }

    @Test
    public void testMoreResultsWithUpdateCount() throws SQLException {
        Statement stmt = connection.createStatement();

        Assert.assertFalse(stmt.execute("create database if not exists default"));
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), 0);

        Assert.assertFalse(stmt.getMoreResults());
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);
    }

    @Test
    public void testSelectQueryStartingWithWith() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("WITH 2 AS two SELECT two * two;");

        Assert.assertNotNull(rs);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt(1), 4);

        rs.close();
        stmt.close();
    }

    @Test
    public void testSelectQueryWithComment() throws SQLException {
        Statement stmt = connection.createStatement();

        Assert.assertTrue(stmt.execute("/* comment here */ select /* another comment */ 1/*and another*/"));
        ResultSet rs = stmt.getResultSet();
        Assert.assertNotNull(rs);
        rs.next();
        Assert.assertEquals(rs.getInt(1), 1);
        Assert.assertFalse(stmt.getMoreResults());
    }

    @Test
    public void testSelectQueryMetadataType() throws Exception {
        Connection connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");

        connection.createStatement().execute("DROP TABLE IF EXISTS test.type_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.type_test (i Int32, l Int64, s String, f Float64, bd Nullable(Decimal128(13)), d Date, dt DateTime, b String) ENGINE = TinyLog"
        );
        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.type_test (i, l, s, f, bd, d, dt, b) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        final BigDecimal bd = new BigDecimal("1234567890.123456789");
        final Date date = new Date(1483330102000L); //2017-01-01 12:21:42 GMT
        final Timestamp ts = new Timestamp(date.getTime());
        statement.setInt(1, 42);
        statement.setLong(2, 4242L);
        statement.setString(3, "asd");
        statement.setDouble(4, 1.0D);
        statement.setBigDecimal(5, bd);
        statement.setDate(6, date);
        statement.setTimestamp(7, ts);
        statement.setBytes(8, "abc".getBytes());
        statement.execute();


        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.type_test");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(md.getColumnType(1), Types.INTEGER);
        assertEquals(md.getColumnType(2), Types.BIGINT);
        assertEquals(md.getColumnType(3), Types.VARCHAR);
        assertEquals(md.getColumnType(4), Types.DOUBLE);
        assertEquals(md.getColumnType(5), Types.DECIMAL);
        assertEquals(md.getColumnType(6), Types.DATE);
        assertEquals(md.getColumnType(7), Types.TIMESTAMP);
        assertEquals(md.getColumnType(8), Types.VARCHAR);
        assertEquals(md.getScale(5), 13);
        rs.next();

        assertEquals(rs.getString("s"), "asd");
        assertEquals(rs.getInt("i"), 42);
        assertEquals(rs.getLong("l"), 4242);
        assertEquals(rs.getDouble("f"), 1.0D);
        assertEquals(rs.getBigDecimal("bd"), bd);
        assertEquals(rs.getDate("d"), date);
        assertEquals(rs.getTimestamp("dt"), ts);
        assertEquals(rs.getString("b"), "abc");

        rs = connection.createStatement().executeQuery("SELECT toDateTime(addHours(toDateTime(dt), 7, 'UTC')) from test.type_test");
        md = rs.getMetaData();
        assertEquals(md.getColumnType(1), Types.TIMESTAMP);

    }

}
