# Metrics Trimmer

Provides the ability to trim Neo4j metric files based on a start date, to only consider the files with data after a specific date.

## Usage

1. Download the latest release
2. Execute the downloaded .jar as follows (it requires Java 11):

```
java -jar metrics-trimmer-<VERSION>.jar --sourcePath=<SOURCE_PATH> --destinationPath=<DESTINATION_PATH> --startDate=<START_DATE>
```

3. A description of each parameter is available by using the help command:

```
java -jar metrics-trimmer-<VERSION>.jar --help
```
