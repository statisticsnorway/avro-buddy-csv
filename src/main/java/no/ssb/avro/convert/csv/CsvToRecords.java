package no.ssb.avro.convert.csv;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class CsvToRecords implements AutoCloseable, Iterable<GenericRecord> {

    private final CsvParser csvParser;
    private final SchemaBuddy schemaBuddy;
    private Callback callBack;

    public CsvToRecords(CsvParserFactory csvParserFactory, InputStream inputStream, SchemaBuddy schemaBuddy) {
        this.csvParser = csvParserFactory.parserFor(inputStream);
        this.schemaBuddy = schemaBuddy;
    }

    public CsvToRecords withCallBack(Callback callBack) {
        this.callBack = callBack;
        return this;
    }

    @Override
    public void close() throws IOException {
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
