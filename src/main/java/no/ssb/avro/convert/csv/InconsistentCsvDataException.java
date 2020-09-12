package no.ssb.avro.convert.csv;

/**
 * Base for exceptions caused by inconsistencies in parsed data where there are no clear
 * ways to fix the problem. These types of exceptions must be dealt with either by explicitlty failing and stopping
 * the conversion process, or simply mark (e.g. log) the record as "unconvertable data" and attempt to continue.
 */
public class InconsistentCsvDataException extends CsvParseException{
    public InconsistentCsvDataException(String message) {
        super(message);
    }

    public InconsistentCsvDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
