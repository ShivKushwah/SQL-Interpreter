package db;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Shiv on 2/25/17.
 */
public class CoryTestClass {
    public static void main(String[] args) {
        Table table = makeExampleTable();


        table.printTable();
        FileManager.storeTable(table);
        try {
            Table testTable = FileManager.readTable("db/table.txt");
            testTable.printTable();
        } catch (Exception error) {
            error.printStackTrace();
        }

    }

    public static Table makeExampleTable() {
        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.add("Name");
        columnNames.add("Age");
        columnNames.add("IDs");


        ArrayList<DataTypes> columnTypes = new ArrayList<>();
        columnTypes.add(DataTypes.stringType);
        columnTypes.add(DataTypes.intType);
        columnTypes.add(DataTypes.intType);

        Table table = new Table("table", columnNames,columnTypes);

        ArrayList<Item> row1 = new ArrayList<>();
        ArrayList<Item> row2 = new ArrayList<>();
        ArrayList<Item> row3 = new ArrayList<>();



        Item x = new Item<String>("'bob'");
        Item y = new Item<Integer>(new Integer(19));
        Item z = new Item<Integer>(new Integer(01));
        Item x1 = new Item<String>("'joe'");
        Item y1 = new Item<Integer>(new Integer(27));
        Item z1 = new Item<Integer>(new Integer(02));
        Item x2 = new Item<String>("'john'");
        Item y2 = new Item<Integer>(new Integer(36));
        Item z2 = new Item<Integer>(new Integer(03));

        row1.add(x);
        row1.add(y);
        row1.add(z);

        row2.add(x1);
        row2.add(y1);
        row2.add(z1);

        row3.add(x2);
        row3.add(y2);
        row3.add(z2);

        table.addRow(row1);
        table.addRow(row2);
        table.addRow(row3);

        return table;
    }

}
