import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.input.ReversedLinesFileReader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Command(name = "metrics-trimmer", mixinStandardHelpOptions = true,
        description = "Trims down metrics .csv files based on a start date")
class MetricsTrimmer implements Callable<Integer> {

    @Option(names = { "--sourcePath" }, required = true, description = "Source path of metric files")
    private String sourcePath;

    @Option(names = { "--destinationPath" }, required = true, description = "Destination path of metric files")
    private String destinationPath;

    @Option(names = { "--startDate" }, required = true, description = "Initial valid date for the metrics data, every data prior to this date will be ignored (format dd-mm-yyyy)")
    private String startDate;

    @Option(names = { "--endDate" }, required = false, description = "End date for the metrics data, every date after this date will be ignored (format dd-mm-yyyy)")
    private String endDate;

    @Option(names = { "--debug" }, description = "Print debug logs during execution")
    private boolean debug;

    @Option(names = { "--threadCount" }, required = false, defaultValue = "1", description = "Thread count number for parallel processing")
    private Integer threadCount;

    SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
    Date parsedStartDate;
    Date parsedEndDate;

    @Override
    public Integer call() {
        System.out.println("Source path: " + sourcePath);
        System.out.println("Destination path: " + destinationPath);
        System.out.println("Start date: " + startDate);

        if (endDate != null) {
            System.out.println("End date: " + endDate);
        }

        System.out.println("Thread count: " + threadCount);

        if(sourcePath != null && !sourcePath.trim().isEmpty() && startDate != null && !startDate.trim().isEmpty()){
            Path fullSourcePath = Paths.get(sourcePath);
            Path fullDestinationPath = Paths.get(destinationPath);
            if(!Files.exists(fullSourcePath)){
                System.out.println("Source path provided does not exists, make sure the directory already exists! " + sourcePath);
                return 0;
            }

            if(!Files.exists(fullDestinationPath)){
                System.out.println("Destination path provided does not exists, make sure the directory already exists! " + destinationPath);
                return 0;
            }

            try{
                parsedStartDate = dateFormat.parse(startDate);

                System.out.println("Parsed start date: " + parsedStartDate);
                System.out.println("Start date millis: " + parsedStartDate.getTime());

                if (endDate != null) {
                    parsedEndDate = dateFormat.parse(endDate);
                    System.out.println("Parsed end date: " + parsedEndDate);
                    System.out.println("End date millis: " + parsedEndDate.getTime());

                    if (parsedStartDate.compareTo(parsedEndDate) > 0) {
                        System.out.println("Start date: " + startDate + " cannot be after End date: " + endDate);
                        System.exit(-1);
                    }
                }

                System.out.println("Process started");
                long startTime = System.currentTimeMillis();

                ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(sourcePath))) {
                    for (Path entry : stream) {
                        File file = entry.toFile();
                        BasicFileAttributes basicFileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
                        if (basicFileAttributes.isRegularFile()) {
                            executorService.submit(() -> trimMetricFile(entry));
                        }
                    }
                }

                stopExecutorService(executorService);
                long endTime = System.currentTimeMillis();
                System.out.println("Process completed in " + (endTime - startTime)/1000 + " s!");
            }catch (Exception e){
                e.printStackTrace();
                return 0;
            }
        }
        return 0;
    }

    private void stopExecutorService(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.HOURS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
        File destFile = new File(destinationDir, zipEntry.getName().replaceAll("/.zip", ".csv"));
        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }

    private void trimMetricFile(Path path) {
        if (debug) {
            System.out.println("Filename: " + path.getFileName());
        }

        String fileName = path.getFileName().toString();
        if (path.getFileName().toString().toLowerCase().endsWith(".zip")) {
            unzipCsv(path);
            return;
        }

        if (path.getFileName().toString().toLowerCase().endsWith(".csv") || path.getFileName().toString().toLowerCase().contains(".csv.")) {
            String absolutePathFile = sourcePath + "/" + fileName;

            if (isLastDateInFileMoreThenStartDate(absolutePathFile) && (parsedEndDate != null ? isFirstDateInFileLessThenEndDate(absolutePathFile) : true)) {
                String updatedMetricsFileName = destinationPath + "/" + fileName;

                CsvWriter writeCsv = null;
                try {
                    writeCsv = CsvWriter.builder().build(Path.of(updatedMetricsFileName));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                int idx = 0;
                try (CsvReader<CsvRecord> csv = CsvReader.builder().ofCsvRecord(Path.of(absolutePathFile))) {
                    for (CsvRecord csvRecord : csv) {
                        if (idx == 0) {
                            writeCsv.writeRecord(csvRecord.getFields());
                        }
                        else{
                            try {
                                long rowDate = Long.parseLong(csvRecord.getField(0) + "000");
                                if (rowDate >= parsedStartDate.getTime() && (parsedEndDate != null ? rowDate <= parsedEndDate.getTime() : true)) {
                                    writeCsv.writeRecord(csvRecord.getFields());
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        idx ++;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }


                try {
                    writeCsv.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                if (debug) {
                    System.out.println("CSV completed");
                }

            } else {
                if (debug) {
                    System.out.println("File " + fileName + " contains old data, skip");
                }
            }
        }
    }

    private boolean isLastDateInFileMoreThenStartDate(String absolutePathFile){
        try (ReversedLinesFileReader reverseReader = new ReversedLinesFileReader(new File(absolutePathFile), StandardCharsets.UTF_8)) {
            String line = reverseReader.readLine();
            if (line == null) {
                throw new Exception("Last line of CSV file " + absolutePathFile + " does not contain any data!");
            }

            String[] lineSplit = line.split(",");

            long rowDate = Long.parseLong(lineSplit[0] + "000");
            if (rowDate >= parsedStartDate.getTime()) {
                return true;
            }
        } catch (Exception e){
            System.out.println(absolutePathFile);
            e.printStackTrace();
        }
        return false;
    }

    private boolean isFirstDateInFileLessThenEndDate(String absolutePathFile){
        try (LineIterator lineIterator = FileUtils.lineIterator(new File(absolutePathFile), StandardCharsets.UTF_8.name())){
            lineIterator.nextLine(); // skip headers
            String line = lineIterator.nextLine();
            if (line == null) {
                throw new Exception("Last line of CSV file " + absolutePathFile + " does not contain any data!");
            }

            String[] lineSplit = line.split(",");

            long rowDate = Long.parseLong(lineSplit[0] + "000");
            if(rowDate <= parsedEndDate.getTime()){
                return true;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private String unzipCsv(Path path) {
        String destinationFileName = null;
        try{
            String fileZip = sourcePath + "/" + path.getFileName();
            File destDir = new File(sourcePath);
            byte[] buffer = new byte[1024];
            ZipInputStream zis = new ZipInputStream(new FileInputStream(fileZip));
            ZipEntry zipEntry = zis.getNextEntry();

            while (zipEntry != null) {
                File newFile = newFile(destDir, zipEntry);
                destinationFileName = newFile.getName();
                if(debug) {
                    System.out.println("Updated filename: " + destinationFileName);
                }
                if (zipEntry.isDirectory()) {
                    if (!newFile.isDirectory() && !newFile.mkdirs()) {
                        throw new IOException("Failed to create directory " + newFile);
                    }
                } else {
                    // fix for Windows-created archives
                    File parent = newFile.getParentFile();
                    if (!parent.isDirectory() && !parent.mkdirs()) {
                        throw new IOException("Failed to create directory " + parent);
                    }

                    // write file content
                    FileOutputStream fos = new FileOutputStream(newFile);
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                    fos.close();
                }
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
            zis.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return destinationFileName;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new MetricsTrimmer()).execute(args);
        System.exit(exitCode);
    }
}