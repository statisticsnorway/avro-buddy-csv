package no.ssb.avro.convert.csv;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CsvToRecordsTest {

    private Schema schema(String schemaFileName) throws IOException {
        return new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream(schemaFileName));
    }

    private InputStream data(String fileName) throws IOException {
        return getClass().getClassLoader().getResourceAsStream(fileName);
    }

    @ParameterizedTest
    @ValueSource(strings = {
      "simple",
      "types-example"
    })
    public void csv_convertToGenericRecords(String scenario) throws Exception {
        InputStream csvInputStream = data(scenario + ".csv");
        Schema schema = schema(scenario + ".avsc");
        List<GenericRecord> records = new ArrayList<>();
        try (CsvToRecords csvToRecords = new CsvToRecords(csvInputStream, schema)) {
            csvToRecords.forEach(records::add);
        }

        assertThat(records.size()).isEqualTo(2);
    }

    @Test
    public void csv_convertToGenericRecords_withValueInterceptor() throws Exception {
        InputStream csvInputStream = data("simple.csv");
        Schema schema = schema("simple.avsc");
        List<GenericRecord> records = new ArrayList<>();
        try (CsvToRecords csvToRecords = new CsvToRecords(csvInputStream, schema, (field, value) -> {
            if ("someString".equals(field.getName()) && "Captain Joe".equals(value)) {
                return "substituted value";
            }
            return value;
        })) {
            csvToRecords.forEach(records::add);
        }

        assertThat(records.get(0).get("someString")).isEqualTo("Hey ho");
        assertThat(records.get(1).get("someString")).isEqualTo("substituted value");
    }

}