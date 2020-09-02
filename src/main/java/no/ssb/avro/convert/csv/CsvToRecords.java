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

public class CsvToRecords implements AutoCloseable, Iterable<GenericRecord> {

    private final CsvParser csvParser;
    private final SchemaBuddy schemaBuddy;
    private Callback callBack;

    public CsvToRecords(String fileName, Schema schema) throws IOException {
        this(new FileInputStream(fileName), schema);
    }

    public CsvToRecords(String fileName, Schema schema, ValueInterceptor valueInterceptor) throws IOException {
        this(new FileInputStream(fileName), schema, valueInterceptor);
    }

    public CsvToRecords(byte[] data, Schema schema) throws IOException {
        this(new ByteArrayInputStream(data), schema);
    }

    public CsvToRecords(byte[] data, Schema schema, ValueInterceptor valueInterceptor) throws IOException {
        this(new ByteArrayInputStream(data), schema, valueInterceptor);
    }

    public CsvToRecords(InputStream inputStream, Schema schema) throws IOException {
        this(inputStream, schema, null);
    }

    public CsvToRecords(InputStream inputStream, Schema schema, ValueInterceptor valueInterceptor) throws IOException {
        this.csvParser = new CsvParser(inputStream).withValueInterceptor(valueInterceptor);
        this.schemaBuddy = SchemaBuddy.parse(schema);
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
