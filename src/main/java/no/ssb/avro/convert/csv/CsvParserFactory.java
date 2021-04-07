package no.ssb.avro.convert.csv;

import no.ssb.avro.convert.core.ValueInterceptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CsvParserFactory {
    final CsvParserSettings parserSettings;
    final com.univocity.parsers.csv.CsvParser internalCsvParser;
    final ValueInterceptor valueInterceptor;

    private CsvParserFactory(CsvParserSettings parserSettings, com.univocity.parsers.csv.CsvParser internalCsvParser, ValueInterceptor valueInterceptor) {
        Objects.requireNonNull(parserSettings);
        Objects.requireNonNull(internalCsvParser);
        this.parserSettings = parserSettings;
        this.internalCsvParser = internalCsvParser;
        this.valueInterceptor = valueInterceptor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private CsvParserSettings parserSettings = new CsvParserSettings();
        private com.univocity.parsers.csv.CsvParser internalCsvParser;
        private ValueInterceptor valueInterceptor;

        public Builder withSettings(CsvParserSettings settings) {
            if (settings == null) {
                this.parserSettings = new CsvParserSettings();
            } else {
                this.parserSettings = settings;
            }
            return this;
        }

        public Builder withSettings(Map<String, Object> settings) {
            parserSettings.configure(settings);
            return this;
        }

        public Builder withValueInterceptor(ValueInterceptor valueInterceptor) {
            this.valueInterceptor = valueInterceptor;
            return this;
        }

        public CsvParserFactory build() {
            internalCsvParser = new com.univocity.parsers.csv.CsvParser(parserSettings.getInternal());
            return new CsvParserFactory(parserSettings, internalCsvParser, valueInterceptor);
        }
    }

    public CsvParserSettings getSettings() {
        return this.parserSettings;
    }

    public List<String> getHeaders() {
        return Arrays.asList(this.internalCsvParser.getRecordMetadata().headers());
    }

    public String getDelimiter() {
        return this.internalCsvParser.getDetectedFormat().getDelimiterString();
    }

    public CsvParser parserFor(byte[] csvData) {
        return new CsvParser(parserSettings, internalCsvParser, valueInterceptor, new ByteArrayInputStream(csvData));
    }

    public CsvParser parserFor(InputStream inputStream) {
        return new CsvParser(parserSettings, internalCsvParser, valueInterceptor, inputStream);
    }
}
