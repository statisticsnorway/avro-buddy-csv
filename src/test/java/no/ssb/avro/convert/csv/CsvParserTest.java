package no.ssb.avro.convert.csv;

import no.ssb.avro.convert.core.DataElement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
        return CsvParserFactory.builder()
          .withSettings(SCENARIO_SETTINGS.get(scenario))
          .build()
          .parserFor(inputStreamOf(scenario + ".csv"));
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
        {
            // Auto detection of delimiters fails for this scenario
            CsvParserFactory factory = CsvParserFactory.builder().build();
            factory.parserFor(inputStreamOf("pipe-separated.csv")).iterator().next(); // parse first data record in order to detect delimiter
            assertThat(factory.getDelimiter()).isEqualTo("."); // suprisingly!
            assertThat(factory.getHeaders().size()).isEqualTo(1);
        }
        {
            // so we explicitly set the delimiter
            CsvParserFactory factory = CsvParserFactory.builder()
                    .withSettings(Map.of(
                            CsvParserSettings.DELIMITERS, "|"
                    ))
                    .build();
            factory.parserFor(inputStreamOf("pipe-separated.csv")).iterator().next(); // parse first data record
            assertThat(factory.getDelimiter()).isEqualTo("|");
            assertThat(factory.getHeaders().size()).isEqualTo(17);
        }
    }

    @Test
    void csvWithColumnMismatch_shouldParseSuccessfully() {

        String csvWithColumnMismatch = "COL1;COL2;COL3\n" +
          "somestring;13;blah\n" +
          "somestring2;42;;blah2";

        CsvParserFactory.builder()
          .build()
          .parserFor(csvWithColumnMismatch.getBytes())
          .forEach(dataElement -> {});
    }

    @Test
    void csvWithoutData_shouldParseSuccessfully() {
        // Should not fail:
        String csvWithoutData = "COL1;COL2;COL3\n \n\n";
        CsvParserFactory.builder()
          .build()
          .parserFor(csvWithoutData.getBytes())
          .forEach(dataElement -> {});
    }


}