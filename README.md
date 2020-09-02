[![Build Status](https://dev.azure.com/statisticsnorway/Dapla/_apis/build/status/statisticsnorway.avro-buddy-csv?repoName=statisticsnorway%2Favro-buddy-csv&branchName=master)](https://dev.azure.com/statisticsnorway/Dapla/_build/latest?definitionId=90&repoName=statisticsnorway%2Favro-buddy-csv&branchName=master)

# avro-buddy-csv

CSV -> GenericRecord

This project depends on [avro-buddy-core](https://github.com/statisticsnorway/avro-buddy-core)

## Maven coordinates

```xml
<dependency>
    <groupId>no.ssb.avro.convert.csv</groupId>
    <artifactId>avro-buddy-csv</artifactId>
    <version>x.x.x</version>
</dependency>
```

## Usage

```java
        InputStream csvInputStream = getCsvFile("something.csv");
        Schema schema = getSchema("something.avsc");
        List<GenericRecord> records = new ArrayList<>();
        try (CsvToRecords csvToRecords = new CsvToRecords(csvInputStream, schema)) {
            csvToRecords.forEach(records::add);
        }
```

For more examples, have a look at the [tests](https://github.com/statisticsnorway/avro-buddy-csv/tree/master/src/test/java/no/ssb/avro/convert/csv).


## Development

From the CLI, run `make help` to see common development commands.

```
build-mvn                      Build the project and install to you local maven repo
release-dryrun                 Simulate a release in order to detect any issues
release                        Release a new version. Update POMs and tag the new version in git.
```
