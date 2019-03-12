package io.jaegertracing.analytics.config;

import java.util.Enumeration;
import java.util.Properties;

public class Utils {

    /**
     * @param input  Properties file
     * @param prefix the prefix with
     * @return the filtered properties file
     */
    public static Properties filterPrefix(Properties input, String prefix) {
        Properties output = new Properties();
        Enumeration<?> propertyNames = input.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String key = (String) propertyNames.nextElement();

            if (key.startsWith(prefix)) {
                if (key.equals(prefix)) {
                    throw new IllegalArgumentException(String.format("Key %s only contains prefix", key));
                }
                String value = input.getProperty(key);
                output.setProperty(key.substring(prefix.length() + 1), value);
            }
        }
        return output;
    }
}
