package ru.yandex.clickhouse;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class LocalSettings {
    private static final String SETTINGS_FILE = "settings.properties";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final Properties properties = new Properties();

    static {
        ClassLoader classLoader = LocalSettings.class.getClassLoader();
        File file = new File(classLoader.getResource(SETTINGS_FILE).getFile());

        try {
            InputStream in = new FileInputStream(file);
            try {
                properties.load(in);
            } finally {
                in.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getHost() {
        return getProperty(HOST, "localhost");
    }

    public static String getPort() {
        return getProperty(PORT, "8123");
    }

    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}
