# Metrics Trimmer

Provides the ability to trim Neo4j metric files, using a start and an end date, to only consider .csv files within a specific date range.

## Usage

1. Download the latest release
2. Execute the downloaded .jar as follows (it requires Java 11):

```
java -jar metrics-trimmer-<VERSION>.jar --sourcePath=<SOURCE_PATH> --destinationPath=<DESTINATION_PATH> --startDate=<START_DATE> --endDate=<END_DATE>
```

3. A description of each parameter is available by using the help command:

```
java -jar metrics-trimmer-<VERSION>.jar --help
```

## Example

Let's say you need to work with a set of Neo4j metric files (.csv), but the whole dataset contains data starting from a long ago (making it very heavy in terms of disk size) while in reality you only need to consider the last 7 days, for example to speed-up Synlig ingestion.
The following command will read all the .csv metric files from <SOURCE_PATH> and generate the trimmed .csv metric files on the <DESTINATION_PATH>, within the timeframe specified by <START_DATE> and <END_DATE>. 

```
java -jar metrics-trimmer-<VERSION>.jar --sourcePath=<SOURCE_PATH> --destinationPath=<DESTINATION_PATH> --startDate=<START_DATE> --endDate=<END_DATE>
```
