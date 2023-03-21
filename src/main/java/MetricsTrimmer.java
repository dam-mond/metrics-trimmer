import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.input.ReversedLinesFileReader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
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

        if(sourcePath != null && sourcePath.trim().length() > 0 && startDate != null && startDate.trim().length() > 0){
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
                Files.walk(Paths.get(sourcePath))
                        .filter(Files::isRegularFile)
                        .forEach(this::execute);
                System.out.println("Process completed!");
            }catch (Exception e){
                e.printStackTrace();
                return 0;
            }
        }
        return 0;
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

    private void execute(Path path) {
        if (debug) {
            System.out.println("Filename: " + path.getFileName());
        }

        String fileName = path.getFileName().toString();
        if (path.getFileName().toString().toLowerCase().endsWith(".zip")) {
            fileName = unzipCsv(path);
        }

        if (path.getFileName().toString().toLowerCase().endsWith(".csv") || path.getFileName().toString().toLowerCase().contains(".csv.")) {
            String absolutePathFile = sourcePath + "/" + fileName;

            List<String> headersList = new ArrayList<>();
            HashMap<String, List<String>> newCsvMap = new LinkedHashMap<>();

            if (isLastDateInFileMoreThenStartDate(absolutePathFile) && (parsedEndDate != null ? isFirstDateInFileLessThenEndDate(absolutePathFile) : true)) {
                try (CSVReader reader = new CSVReader(new FileReader(absolutePathFile))) {
                    List<String[]> r = reader.readAll();

                    int idx = 0;
                    for (String[] strings : r) {
                        if (idx == 0) {
                            headersList.addAll(Arrays.asList(strings));
                        }
                        try {
                            long rowDate = Long.parseLong(strings[0] + "000");
                            if (rowDate >= parsedStartDate.getTime() && (parsedEndDate != null ? rowDate <= parsedEndDate.getTime() : true)) {
                                List<String> values = new ArrayList<>(Arrays.asList(strings).subList(1, strings.length));
                                newCsvMap.put(strings[0], values);
                            }
                        } catch (Exception ignored) {
                        }
                        idx++;
                    }
                } catch (IOException | CsvException e) {
                    e.printStackTrace();
                }

                if (!newCsvMap.isEmpty()) {
                    String updatedMetricsFileName = destinationPath + "/" + fileName;
                    try (PrintWriter writer = new PrintWriter(updatedMetricsFileName)) {
                        StringBuilder sb = new StringBuilder();
                        if (!headersList.isEmpty()) {
                            for (String s : headersList) {
                                if (sb.toString().trim().length() > 0) {
                                    sb.append(',');
                                }
                                sb.append(s);
                            }
                            sb.append('\n');
                        }
                        for (Map.Entry<String, List<String>> entrySet : newCsvMap.entrySet()) {
                            sb.append(entrySet.getKey());
                            for (String val : entrySet.getValue()) {
                                sb.append(',');
                                sb.append(val);
                            }
                            sb.append('\n');
                        }
                        writer.write(sb.toString());
                        if (debug) {
                            System.out.println("CSV completed");
                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
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
            CSVParser parser = new CSVParser();
            String[] fields = parser.parseLine(line);

            long rowDate = Long.parseLong(fields[0] + "000");
            if(rowDate >= parsedStartDate.getTime()){
                return true;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    private boolean isFirstDateInFileLessThenEndDate(String absolutePathFile){
        try (LineIterator lineIterator = FileUtils.lineIterator(new File(absolutePathFile), "UTF-8")){
            lineIterator.nextLine(); // skip headers
            String line = lineIterator.nextLine();
            if (line == null) {
                throw new Exception("Last line of CSV file " + absolutePathFile + " does not contain any data!");
            }

            CSVParser parser = new CSVParser();
            String[] fields = parser.parseLine(line);

            long rowDate = Long.parseLong(fields[0] + "000");
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