package no.ssb.avro.convert.csv;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.convert.core.ValueInterceptor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

public class CsvToRecords implements AutoCloseable, Iterable<GenericRecord> {

    private final CsvParser csvParser;
    private final SchemaBuddy schemaBuddy;
    private Callback callBack;

    public CsvToRecords(InputStream inputStream, Schema schema) throws IOException {
        this(inputStream, schema, (CsvParserSettings) null);
    }

    public CsvToRecords(InputStream inputStream, Schema schema, Map<String, Object> settings) throws IOException {
        this.csvParser = CsvParser.builder().withSettings(settings).buildFor(inputStream);
        this.schemaBuddy = SchemaBuddy.parse(schema);
    }

    public CsvToRecords(InputStream inputStream, Schema schema, CsvParserSettings settings) throws IOException {
        this.csvParser = CsvParser.builder().withSettings(settings).buildFor(inputStream);
        this.schemaBuddy = SchemaBuddy.parse(schema);
    }

    public CsvToRecords withValueInterceptor(ValueInterceptor valueInterceptor) {
        csvParser.withValueInterceptor(valueInterceptor);
        return this;
    }

    public CsvToRecords withCallBack(Callback callBack) {
        this.callBack = callBack;
        return this;
    }

    @Override
    public void close() throws XMLStreamException, IOException {
        csvParser.close();
    }

    @Override
    public Iterator<GenericRecord> iterator() {
        Iterator<DataElement> dataElementIterator = csvParser.iterator();

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return dataElementIterator.hasNext();
            }

            @Override
            public GenericRecord next() {
                DataElement dataElement = dataElementIterator.next();
                if(callBack != null) {
                    callBack.onElement(dataElement);
                }
                return SchemaAwareElement.toRecord(dataElement, schemaBuddy);
            }
        };
    }

    public interface Callback {
        void onElement(DataElement dataElement);
    }
}
