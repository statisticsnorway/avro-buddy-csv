package no.ssb.avro.convert.csv;

/**
 * Base for all exceptions thrown explicitly by avro-buddy-csv
 */
public class CsvParseException extends RuntimeException {
    public CsvParseException(String message) {
        super(message);
    }

    public CsvParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
