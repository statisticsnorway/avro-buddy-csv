package no.ssb.avro.convert.csv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CsvParserTest {

    private InputStream inputStreamOf(String filename) {
        return getClass().getClassLoader().getResourceAsStream(filename);
    }

    /**
     * Holds settings specific for a test scenario.
     * If nothing specified, then no setting is applied
     */
    private static final Map<String, CsvParserSettings> SCENARIO_SETTINGS = Map.of(
        "pipe-separated", new CsvParserSettings().delimiters("|")
    );

    CsvParser csvParserForScenario(String scenario) {
        CsvParserSettings settings = SCENARIO_SETTINGS.get(scenario);
        return CsvParser.builder()
          .withSettings(SCENARIO_SETTINGS.get(scenario))
          .buildFor(inputStreamOf(scenario + ".csv"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
      "simple",
      "types-example",
      "adresses",
      "pipe-separated",
    })
    void scenarios_shouldParse(String scenario) {
        CsvParser csvParser = csvParserForScenario(scenario);
    }

    @Test
    void pipeSeparatedCsv_shouldParse() {
        // Auto detection of delimiters fails for this scenario
        CsvParser csvParser = CsvParser.builder()
          .buildFor(inputStreamOf("pipe-separated.csv"));
        assertThat(csvParser.getDelimiter()).isEqualTo("."); // suprisingly!
        assertThat(csvParser.getHeaders().size()).isEqualTo(1);

        // so we explicitly set the delimiter
        csvParser = CsvParser.builder()
          .withSettings(Map.of(
            CsvParserSettings.DELIMITERS, "|"
          ))
          .buildFor(inputStreamOf("pipe-separated.csv"));
        assertThat(csvParser.getDelimiter()).isEqualTo("|");
        assertThat(csvParser.getHeaders().size()).isEqualTo(17);
    }

    @Test
    void csvWithColumnMismatch_shouldThrowInconsistentDataException() {

        String csvWithColumnMismatch = "COL1;COL2;COL3\n" +
          "somestring;13;blah\n" +
          "somestring2;42;;blah2";

        assertThatThrownBy(() -> {
            CsvParser.builder()
              .buildFor(csvWithColumnMismatch.getBytes())
              .forEach(dataElement -> {});
        }).isInstanceOf(InconsistentCsvDataException.class)
          .hasMessageContaining("Expected 3 columns, but encountered 4");

        // Should not fail:
        String csvWithoutData = "COL1;COL2;COL3\n\n\n\n \n\n";
        CsvParser.builder()
          .buildFor(csvWithoutData.getBytes())
          .forEach(dataElement -> {});
    }
}