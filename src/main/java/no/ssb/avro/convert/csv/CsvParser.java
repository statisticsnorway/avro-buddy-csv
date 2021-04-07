package no.ssb.avro.convert.csv;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.ResultIterator;
import com.univocity.parsers.common.record.Record;
import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.ValueInterceptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

public class CsvParser implements AutoCloseable, Iterable<DataElement> {
    final CsvParserSettings parserSettings;
    final com.univocity.parsers.csv.CsvParser internalCsvParser;
    final ValueInterceptor valueInterceptor;
    final InputStream inputStream;

    CsvParser(CsvParserSettings parserSettings, com.univocity.parsers.csv.CsvParser internalCsvParser, ValueInterceptor valueInterceptor, InputStream inputStream) {
        this.parserSettings = parserSettings;
        this.internalCsvParser = internalCsvParser;
        this.valueInterceptor = valueInterceptor;
        this.inputStream = inputStream;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public Iterator<DataElement> iterator() {

        final ResultIterator<Record, ParsingContext> recordIterator = internalCsvParser.iterateRecords(inputStream).iterator();
        final Map<String, String> columnNameOverrides = parserSettings.getColumnNameOverrides();

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return recordIterator.hasNext();
            }

            @Override
            public DataElement next() {
                Record record = recordIterator.next();
                String[] headers = recordIterator.getContext().recordMetaData().headers();
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
