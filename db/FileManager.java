package db;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by CoryM on 2/28/2017.
 */

public class FileManager {

    public String getCurrDir() {
        String path = Paths.get(".").toAbsolutePath().normalize().toString();
        return path;
    }

    public static void storeTable(Table table) {
        PrintWriter fileWriter = null;
        try {
            fileWriter = new PrintWriter(table.getName() + ".tbl");
        } catch (FileNotFoundException error) {
            error.printStackTrace();
        }
        fileWriter.println(table.toString());
        fileWriter.close();

    }


    public static Table readTable(String fileName) throws IOException, Exception {
        //Read file using Files class
        System.setProperty("file.encoding", "UTF-8");
        Path path = Paths.get(fileName);
        List<String> tableString;

        tableString = Files.readAllLines(path);
        //Construct new table

        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<DataTypes> types = new ArrayList<>();
        String[] strArr = tableString.get(0).split(",");
        String[] strArr2;
        for (String string : strArr) {
            strArr2 = string.trim().split("\\s+");
            columnNames.add(strArr2[0]);
            types.add(DataTypes.valueOf(strArr2[1] + "Type"));
        }
        Table returnTable = new Table(fileName.substring(0, fileName.length() - 4), columnNames, types);
        for (int i = 1; i < tableString.size(); i++) {
            returnTable.addRow(Row.fromString(tableString.get(i)));
        }
        return returnTable;


    }

}