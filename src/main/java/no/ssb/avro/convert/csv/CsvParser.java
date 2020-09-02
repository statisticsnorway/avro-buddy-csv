package no.ssb.avro.convert.csv;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.ResultIterator;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParserSettings;
import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.ValueInterceptor;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class CsvParser implements AutoCloseable, Iterable<DataElement> {
    private final InputStream inputStream;
    private final com.univocity.parsers.csv.CsvParser csvParser;
    private ValueInterceptor valueInterceptor;

    public CsvParser(String fileName) throws IOException {
        this(new FileInputStream(fileName));
    }

    public CsvParser(byte[] csvData) throws IOException {
        this(new ByteArrayInputStream(csvData));
    }

    public CsvParser(InputStream is) throws IOException {
        this.inputStream = is;

        CsvParserSettings settings = new CsvParserSettings();
        settings.detectFormatAutomatically();
        settings.setHeaderExtractionEnabled(true);
        settings.setCommentProcessingEnabled(false);

        csvParser = new com.univocity.parsers.csv.CsvParser(settings);
        csvParser.beginParsing(inputStream);
    }

    public CsvParser withValueInterceptor(ValueInterceptor valueInterceptor) {
        this.valueInterceptor = valueInterceptor;
        return this;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public Iterator<DataElement> iterator() {
        final ResultIterator<Record, ParsingContext> recordIterator = csvParser.iterateRecords(inputStream).iterator();

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return recordIterator.hasNext();
            }

            @Override
            public DataElement next() {
                Record record = recordIterator.next();
                DataElement root = new DataElement("root");
                record.toFieldMap().forEach((key, value) -> {
                    // TODO: Make this more robust
                    if (key.startsWith("#")) {
                        key = key.substring(1);
                    }
                    // TODO: Update avro-buddy-core to handle new DataElement(key, value) with valueInterceptor. That code is buggy
                    DataElement e = new DataElement(key).withValueInterceptor(valueInterceptor);
                    e.setValue(value);
                    root.addChild(e);
                  }
                );
                return root;
            }
        };
    }
}
