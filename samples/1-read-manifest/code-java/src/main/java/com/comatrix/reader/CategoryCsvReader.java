package com.comatrix.reader;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CategoryCsvReader {

    public static void main(String[] args) {
        CategoryCsvReader categoryCsvReader = new CategoryCsvReader();
        try {
            List<String[]> categories = categoryCsvReader.readAllCategories();
            System.out.println(""+categories.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // https://www.baeldung.com/opencsv

    public List<String[]> readAllCategories() throws Exception {
        Reader reader = Files.newBufferedReader(Paths.get(
                ClassLoader.getSystemResource("categories.csv").toURI()));
        return readAll(reader);
    }
    public List<String[]> readAll(Reader reader) throws Exception {

        CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .withQuoteChar('"')
                // .withIgnoreQuotations(true)
                .build();

        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withSkipLines(0)
                .withCSVParser(parser)
                .build();

        // CSVReader csvReader = new CSVReader(reader);
        List<String[]> list = new ArrayList<>();
        list = csvReader.readAll();
        reader.close();
        csvReader.close();
        return list;
    }
}
