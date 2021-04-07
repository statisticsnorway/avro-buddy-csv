package no.ssb.avro.convert.csv;

import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        CsvParserFactory factory = CsvParserFactory.builder()
                .build();
        try (CsvToRecords csvToRecords = new CsvToRecords(factory, csvInputStream, SchemaBuddy.parse(schema))) {
            csvToRecords.forEach(records::add);
        }

        assertThat(records.size()).isEqualTo(2);
    }

    @Test
    public void csv_convertToGenericRecords_withValueInterceptor() throws Exception {
        InputStream csvInputStream = data("simple.csv");
        Schema schema = schema("simple.avsc");
        List<GenericRecord> records = new ArrayList<>();

        CsvParserFactory factory = CsvParserFactory.builder()
                .withValueInterceptor((field, value) -> {
                    if ("someString".equals(field.getName()) && "Captain Joe".equals(value)) {
                        return "substituted value";
                    }
                    return value;
                })
                .build();
        try (CsvToRecords csvToRecords = new CsvToRecords(factory, csvInputStream, SchemaBuddy.parse(schema))) {
            csvToRecords.forEach(records::add);
        }

        assertThat(records.get(0).get("someString")).isEqualTo("Hey ho");
        assertThat(records.get(1).get("someString")).isEqualTo("substituted value");
    }

    @Test
    public void csvWithFunkyColumnNames_convertToGenericRecords_withRenamedColumns() throws Exception {
        String scenario = "with-column-renames";
        InputStream csvInputStream = data(scenario + ".csv");
        Schema schema = schema(scenario + ".avsc");
        List<GenericRecord> records = new ArrayList<>();
        CsvParserSettings csvParserSettings = new CsvParserSettings().columnNameOverrides(Map.of(
          "a column with spaces", "renamedCol1Name",
          "a_column-with-$pecicialName!", "renamedCol2Name"
        ));

        CsvParserFactory factory = CsvParserFactory.builder()
                .build();
        try (CsvToRecords csvToRecords = new CsvToRecords(factory, csvInputStream, SchemaBuddy.parse(schema))) {
            csvToRecords.forEach(records::add);
        }

        assertThat(records.size()).isEqualTo(2);
        assertThat(records.get(0).get("renamedCol1Name")).isEqualTo("Hey ho");
        assertThat(records.get(0).get("renamedCol2Name")).isEqualTo("42");
        assertThat(records.get(0).get("renamedCol3Name")).isEqualTo("foo");
    }

    @Test
    public void csvWithFunkyColumnNames_convertToGenericRecords_withExplicitlyNamedColumns() throws Exception {
        String scenario = "with-column-renames";
        InputStream csvInputStream = data(scenario + ".csv");
        Schema schema = schema(scenario + ".avsc");
        List<GenericRecord> records = new ArrayList<>();
        CsvParserSettings csvParserSettings = new CsvParserSettings().headers(List.of(
          "renamedCol1Name",
          "renamedCol2Name",
          "renamedCol3Name"));

        CsvParserFactory factory = CsvParserFactory.builder()
          .withSettings(csvParserSettings)
          .build();
        try (CsvToRecords csvToRecords = new CsvToRecords(factory, csvInputStream, SchemaBuddy.parse(schema))) {
            csvToRecords.forEach(records::add);
        }

        assertThat(records.size()).isEqualTo(2);
        assertThat(records.get(0).get("renamedCol1Name")).isEqualTo("Hey ho");
        assertThat(records.get(0).get("renamedCol2Name")).isEqualTo("42");
        assertThat(records.get(0).get("renamedCol3Name")).isEqualTo("foo");
    }
}