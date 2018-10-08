package ru.yandex.clickhouse;


import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;


public class ClickHousePreparedStatementTest {

    @Test
    public void testParseSql() throws Exception {
        assertEquals(new ArrayList<String>() {{
            add("SELECT * FROM tbl");
        }}, convertToStringList(ClickHousePreparedStatementImpl.parseSql("SELECT * FROM tbl")));

        assertEquals(new ArrayList<String>(){{
            add("SELECT * FROM tbl WHERE t = ");
            add("");
        }}, convertToStringList(ClickHousePreparedStatementImpl.parseSql("SELECT * FROM tbl WHERE t = ?")));

        assertEquals(new ArrayList<String>(){{
            add("SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ");
            add(" AND r = ");
            add(" ORDER BY 1");
        }}, convertToStringList(ClickHousePreparedStatementImpl.parseSql("SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ? AND r = ? ORDER BY 1")));
    }

    @Test
    public void testParseWithComment() throws SQLException {
        assertEquals(new ArrayList<String>(){{
            add("select a --what is it?\nfrom t where a = ");
            add(" and b = 1");
        }}, convertToStringList(ClickHousePreparedStatementImpl.parseSql("select a --what is it?\nfrom t where a = ? and b = 1")));

        assertEquals(new ArrayList<String>(){{
            add("select a /*what is it?*/ from t where a = ");
            add(" and b = 1");
        }}, convertToStringList(ClickHousePreparedStatementImpl.parseSql("select a /*what is it?*/ from t where a = ? and b = 1")));
    }

    private List<String> convertToStringList(List<byte[]> result) {
        List<String> list = new ArrayList<String>();
        for (byte[] bytes : result) {
            list.add(new String(bytes));
        }
        return list;
    }
}