package db;

import java.util.ArrayList;

/**
 * Created by CoryM on 3/1/2017.
 */
public class ShivTestClass {
    public static void main(String[] args) {

        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.add("Name");
        columnNames.add("Age");
        columnNames.add("IDs");


        ArrayList<DataTypes> columnTypes = new ArrayList<>();
        columnTypes.add(DataTypes.stringType);
        columnTypes.add(DataTypes.intType);
        columnTypes.add(DataTypes.intType);

        Table table = new Table("testDatabase",columnNames,columnTypes);

        ArrayList<Item> row1 = new ArrayList<>();
        ArrayList<Item> row2 = new ArrayList<>();
        ArrayList<Item> row3 = new ArrayList<>();



        Item x = new Item<String>("bob");
        Item y = new Item<Integer>(new Integer(19));
        Item z = new Item<Integer>(new Integer(01));
        Item x1 = new Item<String>("joe");
        Item y1 = new Item<Integer>(new Integer(27));
        Item z1 = new Item<Integer>(new Integer(02));
        Item x2 = new Item<String>("john");
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

        table.printTable();

        ArrayList<String> selectColumns = new ArrayList<>();
        selectColumns.add("Age");
        selectColumns.add("IDs");

        Table table2 = table.selectFromOneDatabase(selectColumns);
        table2.printTable();

        //This is a second table to be merged


        ArrayList<String> columnNames3 = new ArrayList<>();
        columnNames3.add("Name");
        columnNames3.add("Y");
        columnNames3.add("IDs");


        ArrayList<DataTypes> columnTypes3 = new ArrayList<>();
        columnTypes3.add(DataTypes.stringType);
        columnTypes3.add(DataTypes.intType);
        columnTypes3.add(DataTypes.intType);

        Table table3 = new Table("testDatabase", columnNames3,columnTypes3);

        ArrayList<Item> row13 = new ArrayList<>();
        ArrayList<Item> row23 = new ArrayList<>();
        ArrayList<Item> row33 = new ArrayList<>();



        Item x3 = new Item<String>("bob");
        Item y3 = new Item<Integer>(new Integer(19));
        Item z3 = new Item<Integer>(new Integer(03));
        Item x13 = new Item<String>("joe");
        Item y13 = new Item<Integer>(new Integer(32));
        Item z13 = new Item<Integer>(new Integer(02));
        Item x23 = new Item<String>("john");
        Item y23 = new Item<Integer>(new Integer(3006));
        Item z23 = new Item<Integer>(new Integer(03));

        row13.add(x3);
        row13.add(y3);
        row13.add(z3);

        row23.add(x13);
        row23.add(y13);
        row23.add(z13);

        row33.add(x23);
        row33.add(y23);
        row33.add(z23);

        table3.addRow(row13);
        table3.addRow(row23);
        table3.addRow(row33);

        table3.printTable();

        Table.join(table,table3).printTable();




    }
}
