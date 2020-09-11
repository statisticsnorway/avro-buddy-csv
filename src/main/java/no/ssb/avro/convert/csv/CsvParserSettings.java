package no.ssb.avro.convert.csv;

import java.util.Map;

public class CsvParserSettings {
    public static final String DELIMITERS = "delimiters";

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

    public CsvParserSettings delimiters(String delimiters) {
        settings.setDelimiterDetectionEnabled(true, delimiters.toCharArray());
        return this;
    }

    public CsvParserSettings configure(Map<String, Object> configMap) {
        if (configMap == null) {
            return this;
        }
        if (configMap.containsKey(DELIMITERS)) {
            delimiters((String) configMap.get(DELIMITERS));
        }
        return this;
    }

    com.univocity.parsers.csv.CsvParserSettings getInternal() {
        return settings;
    }

}
