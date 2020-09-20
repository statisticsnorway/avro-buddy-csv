package no.ssb.avro.convert.csv;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvParserSettings {
    public static final String DELIMITERS = "delimiters";
    public static final String HEADERS = "headers";
    public static final String COLUMN_NAME_OVERRIDES = "columnNameOverrides";
    public static final Map<String, String> columnNameOverrides = new HashMap<>();

    private final com.univocity.parsers.csv.CsvParserSettings settings;

    public CsvParserSettings() {
        settings = new com.univocity.parsers.csv.CsvParserSettings();

        // default settings:
        settings.detectFormatAutomatically();
        settings.setHeaderExtractionEnabled(true);
        settings.setCommentProcessingEnabled(false);
    }

    @Override
    public String toString() {
        return settings.toString();
    }

    /**
     * String with valid csv delimiter characters. This setting is optional, but not specifying this could lead to
     * unexpected csv parsing errors, since without this, we have to guess the delimters.
     */
    public CsvParserSettings delimiters(String delimiters) {
        settings.setDelimiterDetectionEnabled(true, delimiters.toCharArray());
        return this;
    }

    public CsvParserSettings headers(List<String> headers) {
        settings.setHeaders(headers.toArray(new String[0]));
        return this;
    }

    /**
     * Map of column name overrides that should be used in the converted results. If not specified, the converter will
     * assume that the target avro schema field name is the same as the source csv column name.
     *
     * Sometimes a CSV file will have column names that are not valid field names in Avro. In order to prevent errors
     * during record assembly, you can specify mapping overrides using this setting.
     */
    public Map<String, String> getColumnNameOverrides() {
        return this.columnNameOverrides;
    }

    public CsvParserSettings columnNameOverrides(Map<String, String> columnNameOverrides) {
        this.columnNameOverrides.putAll(columnNameOverrides);
        return this;
    }

    public CsvParserSettings configure(Map<String, Object> configMap) {
        if (configMap == null) {
            return this;
        }
        if (configMap.containsKey(DELIMITERS)) {
            delimiters((String) configMap.get(DELIMITERS));
        }
        if (configMap.containsKey(HEADERS)) {
            headers((List<String>) configMap.get(HEADERS));
        }
        if (configMap.containsKey(COLUMN_NAME_OVERRIDES)) {
            columnNameOverrides((Map<String, String>) configMap.get(COLUMN_NAME_OVERRIDES));
        }
        return this;
    }

    com.univocity.parsers.csv.CsvParserSettings getInternal() {
        return settings;
    }

}
