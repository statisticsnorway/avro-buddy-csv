package no.ssb.avro.convert.csv;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.ResultIterator;
import com.univocity.parsers.common.record.Record;
import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.ValueInterceptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class CsvParser implements AutoCloseable, Iterable<DataElement> {
    private InputStream inputStream;
    private CsvParserSettings parserSettings = new CsvParserSettings();
    private com.univocity.parsers.csv.CsvParser internalCsvParser;
    private ValueInterceptor valueInterceptor;

    public static class Builder {
        private CsvParser csvParser = new CsvParser();

        public Builder withSettings(CsvParserSettings settings) {
            if (settings != null) {
                csvParser.parserSettings = settings;
            }
            return this;
        }

        public Builder withSettings(Map<String, Object> settings) {
            csvParser.parserSettings.configure(settings);
            return this;
        }

        public Builder withValueInterceptor(ValueInterceptor valueInterceptor) {
            csvParser.valueInterceptor = valueInterceptor;
            return this;
        }

        public CsvParser buildFor(byte[] csvData) {
            csvParser.inputStream = new ByteArrayInputStream(csvData);
            return build();
        }

        public CsvParser buildFor(InputStream inputStream) {
            csvParser.inputStream = inputStream;
            return build();
        }

        private CsvParser build() {
            requireNonNull(csvParser.inputStream, "CSV InputStream is not set");
            csvParser.internalCsvParser = new com.univocity.parsers.csv.CsvParser(csvParser.parserSettings.getInternal());
            csvParser.internalCsvParser.beginParsing(csvParser.inputStream);
            return csvParser;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private CsvParser() { }

    public CsvParser withValueInterceptor(ValueInterceptor valueInterceptor) {
        this.valueInterceptor = valueInterceptor;
        return this;
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

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public Iterator<DataElement> iterator() {

        final ResultIterator<Record, ParsingContext> recordIterator = internalCsvParser.iterateRecords(inputStream).iterator();
        final String[] headers = internalCsvParser.getRecordMetadata().headers();
        final Map<String, String> columnNameOverrides = parserSettings.getColumnNameOverrides();

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return recordIterator.hasNext();
            }

            @Override
            public DataElement next() {
                Record record = recordIterator.next();
                DataElement root = new DataElement("root");
                record.toFieldMap(headers).forEach((key, value) -> {
                    String elementName = columnNameOverrides.getOrDefault(key, key);

                    // TODO: Make this more robust
                    if (elementName.startsWith("#")) {
                        elementName = elementName.substring(1);
                    }
                    // TODO: Update avro-buddy-core to handle new DataElement(key, value) with valueInterceptor. That code is buggy
                    DataElement e = new DataElement(elementName).withValueInterceptor(valueInterceptor);
                    e.setValue(value);
                    root.addChild(e);
                  }
                );
                return root;
            }
        };
    }

}
