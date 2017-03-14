package db;

import java.util.*;

/**
 * Created by Shiv on 2/21/17.
 */
public class Table implements DatabaseInterface {

    ArrayList<Row> rows;
    HashMap<String, Integer> columnNamesToIndex;
    LinkedHashMap<String, DataTypes> columnNamesToTypes;
    String tableName;
    ArrayList<String> columnNames;
    ArrayList<DataTypes> types;

    public Table(String name, ArrayList<String> columnNames, ArrayList<DataTypes> types) {
        this.columnNames = columnNames;
        this.types = types;
        tableName = name;
        rows = new ArrayList<>();
        columnNamesToIndex = new HashMap<>();
        columnNamesToTypes = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columnNamesToIndex.put(columnNames.get(i) , i);
            columnNamesToTypes.put(columnNames.get(i) , types.get(i));
        }
    }


    public void initializeDatabase(ArrayList<String> columnNames, ArrayList<DataTypes> types) {
        this.columnNames = columnNames;
        this.types = types;

        rows = new ArrayList<>();
        columnNamesToIndex = new HashMap<>();
        columnNamesToTypes = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {

            columnNamesToIndex.put(columnNames.get(i) , i);
            columnNamesToTypes.put(columnNames.get(i) , types.get(i));
        }

    }

    public String transact(String query) {
        //call initialize database with the query
        //return the string from print table

        return null;
    }

    public static Table joinMultiple(ArrayList<Table> tablesArray) {
        Table x = join(tablesArray.get(0),tablesArray.get(1));

        for (int i = 2; i < tablesArray.size(); i++) {
            x = join(x, tablesArray.get(i));
        }
        return x;

        /*
        ArrayList<String> newColumns = new ArrayList<>();
        ArrayList<DataTypes> newTypes = new ArrayList<>();
        ArrayList<String> commonColumns = new ArrayList<>();
        ArrayList<String> columns1 = new ArrayList<>(db1.columnNames);
        ArrayList<String> columns2 = new ArrayList<>(db2.columnNames);

        //Get common columns
        for (int i = 0; i < columns1.size(); i++) {
            for (int j = 0; j < columns2.size(); j++) {
                if (columns1.get(i).equals(columns2.get(j))) {
                    newColumns.add(columns1.get(i));
                    commonColumns.add(columns1.get(i));
                    columns1.remove(i);
                    columns2.remove(j);
                    break;
                }
            }
        }

        //if no commoncolumns, then do cartesian join
        if (commonColumns.size() == 0) {

            //Get columns from left
            for (int i = 0; i < columns1.size(); i++) {
                newColumns.add(columns1.get(i));
            }
            //Get columns from right
            for (int i = 0; i < columns2.size(); i++) {
                newColumns.add(columns2.get(i));
            }

            //initialize types array
            for (String s: newColumns) {
                if (db1.columnNamesToTypes.get(s) != null) {
                    newTypes.add(db1.columnNamesToTypes.get(s));

                } else if (db2.columnNamesToTypes.get(s) != null) {
                    newTypes.add(db2.columnNamesToTypes.get(s));
                } else {
                    throw new RuntimeException();
                }

            }

            Table db3 = new Table("selected",newColumns, newTypes);
            for (int i = 0; i < db1.getNumRows(); i++) {
                for (int j = 0; j < db2.getNumRows(); j++) {
                    Row x = db1.getRow(i).copyRow();
                    Row y = db2.getRow(j).copyRow();
                    ArrayList<Item> newitems = new ArrayList<>();
                    for (int z = 0; z < x.getLength(); z++) {
                        newitems.add(x.getItem(z));
                    }
                    for (int k = 0; k < y.getLength(); k++) {
                        newitems.add(y.getItem(k));
                    }
                    db3.addRow(newitems);
                }
            }

            return  db3;
        }

        //Get columns from left
        for (int i = 0; i < columns1.size(); i++) {
            newColumns.add(columns1.get(i));
        }
        //Get columns from right
        for (int i = 0; i < columns2.size(); i++) {
            newColumns.add(columns2.get(i));
        }

        //initialize types array
        for (String s: newColumns) {
            if (db1.columnNamesToTypes.get(s) != null) {
                newTypes.add(db1.columnNamesToTypes.get(s));

            } else if (db2.columnNamesToTypes.get(s) != null) {
                newTypes.add(db2.columnNamesToTypes.get(s));
            } else {
                throw new RuntimeException();
            }

        }
        Table db3 = new Table("selected",newColumns, newTypes);

        ArrayList<Row> commonRows1 = new ArrayList<>();
        ArrayList<Row> commonRows2 = new ArrayList<>();
        //Check which rows match using common columns
        for (int i =0; i < db1.getNumRows(); i++) {

            for (int j = 0; j < db2.getNumRows(); j++) {
                boolean correctRow = true;
                for (String s: commonColumns) {
                    //if the items aren't the same
                    //make below in helper function
                    Item z = db1.getRow(i).getItem( db1.columnNamesToIndex.get(s) );
                    Item y = db2.getRow(j).getItem( db2.columnNamesToIndex.get(s) );
                    if (!z.openItem().equals(y.openItem())) {
                        correctRow = false;
                    }
                }

                if (correctRow) {
                    commonRows1.add(db1.getRow(i));
                    commonRows2.add(db2.getRow(j));
                    //maybe don't need break
                    break;
                }
            }

        }

        ArrayList<Row> commmonRowsFinal = new ArrayList<>();
        //filter the rows to make a single rows arraylist
        for (int i = 0; i < commonRows1.size(); i++) {
            //only add unique elements to the rows
            Row r = commonRows1.get(i);
            Row r2 = commonRows2.get(i);
            ArrayList<Item> items = new ArrayList<>();
            //copy elements that are in commonColumns
            for (String c: newColumns) { //before commonColumnns
                if (db1.columnNamesToIndex.get(c) != null) {
                    Item x = Item.copyItem( r.getItem(db1.columnNamesToIndex.get(c)) );
                    items.add(x);
                } else if (db2.columnNamesToIndex.get(c) != null) {
                    Item x = Item.copyItem( r2.getItem(db2.columnNamesToIndex.get(c)) );
                    items.add(x);
                } else {
                    throw new RuntimeException();
                }
            }
            //copy other elements

            commmonRowsFinal.add(new Row(items));

        }

        for (int i = 0; i < commmonRowsFinal.size(); i++) {
            db3.addRow(commmonRowsFinal.get(i).items);
        }

        return db3;
        */
    }

    public Table selectFromOneDatabase(ArrayList<String> columns) {
        //First, make sure the columns asked for should be valid

        //Then, pull the indices you need
        ArrayList<Integer> indices = getRowIndicesFromColumns(columns); //What if the columns aren't passed in order and the row indicies are out of order?
        //iterate over the rows, and make a new database with these rows
        //Get the types of the new requested columns

        ArrayList<DataTypes> typesarray = new ArrayList<>();
        for (String s: columns) {
            typesarray.add(columnNamesToTypes.get(s));
        }
        Table db = new Table("selected",columns,typesarray);


        for (int i = 0; i < getNumRows(); i++) {
            ArrayList<Item> items = new ArrayList<>();
            for (Integer x: indices) {
                items.add(getRow(i).getItem(x));
            }
            db.addRow(items);
        }

        return db;
    }

    public Table selectFromOneDatabaseColumnExpressions(ArrayList<String> columns, ArrayList<String> newColumnFromExpressions, ArrayList<DataTypes> dataTypesOfExpressionColumns, ArrayList<ArrayList<String>> columnOperation) {
        //columnOperation array has arrays of format [first column, second column, operation]

        ArrayList<Integer> indices = getRowIndicesFromColumns(columns); //Note, null elements whereever the column doesn' exsist
        //iterate over the rows, and make a new database with these rows
        //Get the types of the new requested columns

        ArrayList<DataTypes> typesarray = new ArrayList<>();
        ArrayList<DataTypes> typesarrayCopy = new ArrayList<>();
        for (String s: columns) {
            if (columnNamesToTypes.get(s) == null) {
//                try {
//                    if (dataTypesOfExpressionColumns.get(0) == DataTypes.floatType || dataTypesOfExpressionColumns.get(1) == DataTypes.floatType) {
//                        typesarray.add(DataTypes.floatType);
//                        typesarrayCopy.add(DataTypes.floatType);
//                    }
//                } catch (Exception e) {
                    typesarray.add(dataTypesOfExpressionColumns.get(0));
                    typesarrayCopy.add(dataTypesOfExpressionColumns.get(0));
  //              }



                dataTypesOfExpressionColumns.remove(0);
            }
            else {
                typesarray.add(columnNamesToTypes.get(s));
                typesarrayCopy.add(columnNamesToTypes.get(s));
            }
        }




        ArrayList<ArrayList<String>> columnOperationCopy = new ArrayList<>(columnOperation);
        Table db = new Table("selected",columns,typesarray);
        for (int i = 0; i < getNumRows(); i++) {
            //reinitlize columnOperations everytime so that you can performs operations again on each row
            columnOperation = new ArrayList<>(columnOperationCopy);
            //ArrayList<ArrayList<String>> columnOperationCopy = new ArrayList<ArrayList<String>>();
            ArrayList<Item> items = new ArrayList<>();
            int k = 0;
            for (Integer x: indices) {
                if (x != null && columnOperation.size() == 0) {
                    items.add(getRow(i).getItem(x));
                }
                else if (x != null && Integer.parseInt(columnOperation.get(0).get(3)) != k) {
                    items.add(getRow(i).getItem(x));
                }
//                if (x != null) {
//                    items.add(getRow(i).getItem(x));
//                }
                else {
                    //perform specialized operation here
                    String col1 = columnOperation.get(0).get(0);
                    String col2 = columnOperation.get(0).get(1);
                    Operation op = OurParse.getOperator(columnOperation.get(0).get(2));
                    int indexOfElement1 = columnNamesToIndex.get(col1);
                    int indexOfElement2 = columnNamesToIndex.get(col2);
                    Item item1 = getRow(i).getItem(indexOfElement1);
                    Item item2 = getRow(i).getItem(indexOfElement2);
                    items.add(OperationClass.operate(item1, item2, op));
                    columnOperation.remove(0);
                }
                k++;
            }
            db.addRow(items);

        }

        return db;

    }

    public static Table selectMultipleDatabases(Table db1, Table db2, ArrayList<String> columns) {
        Table db3 = join(db1, db2);

        return db3.selectFromOneDatabase(columns);
    }
    public static Table selectMultipleDatabasesMultipleTables(ArrayList<Table> tablesArray, ArrayList<String> columns) {
        Table db3 = joinMultiple(tablesArray);

        return db3.selectFromOneDatabase(columns);
    }
    public static Table selectMultipleDatabasesMultipleTablesStar(ArrayList<Table> tablesArray) {
        Table db3 = joinMultiple(tablesArray);

        return db3;
    }

    public static Table filterTableWithConds(Table table, ArrayList<ArrayList<String>> literalConditions, ArrayList<ArrayList<String>> columnConditions, Database db) {
        //go to a row
        //if all conditions are met, add row
        //Columnconditions and literalconditions [column 1, conditional, literal/column2]
        ArrayList<ArrayList<String>> literalConditionsCopy = new ArrayList<ArrayList<String>>(literalConditions);
        ArrayList<ArrayList<String>> columnConditionsCopy = new ArrayList<ArrayList<String>>(columnConditions);



        //get copy types array for filterdtable
        ArrayList<DataTypes> types = new ArrayList<>();
        for (String s: table.getColumnNames()) {
            types.add (table.columnNamesToTypes.get(s));
        }
        //get copy column names array
        ArrayList<String> columnNames = new ArrayList<>();
        for (String s: table.getColumnNames()) {
            columnNames.add(s);
        }


        Table filteredTable = new Table("filtered", columnNames, types);

        for (int i = 0; i < table.getNumRows(); i++) {

            boolean correctRow = true;
            Row currentRow = table.getRow(i);

            for (ArrayList<String> lcondition: literalConditions) {
                //parse the conditions and then if bad, set correct row to false
                String literal = lcondition.get(2).trim();
                Item literalItem;
                try {
                     literalItem = new Item(Integer.parseInt(literal));
                } catch (Exception e) {
                     literalItem = new Item(literal);
                }

                String columnName = lcondition.get(0).trim();
                Item columnItem = table.getRow(i).getItem(table.columnNamesToIndex.get(columnName));

                String comparison = lcondition.get(1);
                ComparisonOperators comparisonOperator = OurParse.getComparisonOperator(comparison.trim());

                if (!ComparisonOperatorsClass.compareValid(columnItem, literalItem, comparisonOperator)) {
                    correctRow = false;
                }

            }
            for (ArrayList<String> ccondition: columnConditions) {

                String column1Name = ccondition.get(0).trim();
                Item columnItem1 = table.getRow(i).getItem(table.columnNamesToIndex.get(column1Name));

                String column2Name = ccondition.get(2).trim();
                Item columnItem2 = table.getRow(i).getItem(table.columnNamesToIndex.get(column2Name));

                String comparison = ccondition.get(1);
                ComparisonOperators comparisonOperator = OurParse.getComparisonOperator(comparison.trim());


                if (!ComparisonOperatorsClass.compareValid(columnItem1, columnItem2, comparisonOperator)) {
                    correctRow = false;
                }

            }

            if (correctRow) {
                //copy items from current row and put them in new items database
                //TODO: Potential bug, you may want to copy the curentRow.items instead of just passing it in
                filteredTable.addRow(currentRow.items);
            }
        }
        return filteredTable;
    }

    public static Table join(Table db1, Table db2) {

        //Find the common columns
        ArrayList<String> newColumns = new ArrayList<>();
        ArrayList<DataTypes> newTypes = new ArrayList<>();
        ArrayList<String> commonColumns = new ArrayList<>();
        ArrayList<String> columns1 = new ArrayList<>(db1.columnNames);
        ArrayList<String> columns2 = new ArrayList<>(db2.columnNames);

        //Get common columns
        for (int i = 0; i < columns1.size(); i++) {
            for (int j = 0; j < columns2.size(); j++) {
                if (columns1.get(i).equals(columns2.get(j))) {
                    if (!newColumns.contains(columns1.get(i))) {
                        newColumns.add(columns1.get(i));
                        commonColumns.add(columns1.get(i));
                    }
                    //columns1.remove(i);
                    //columns2.remove(j);
                    //break;
                }
            }
        }
        //remove these common columns from columns1 and columns2
        for (String s : commonColumns) {
            columns1.remove(s);
            columns2.remove(s);
        }

        //if no commoncolumns, then do cartesian join
        if (commonColumns.size() == 0) {

            //Get columns from left
            for (int i = 0; i < columns1.size(); i++) {
                newColumns.add(columns1.get(i));
            }
            //Get columns from right
            for (int i = 0; i < columns2.size(); i++) {
                newColumns.add(columns2.get(i));
            }

            //initialize types array
            for (String s: newColumns) {
                if (db1.columnNamesToTypes.get(s) != null) {
                    newTypes.add(db1.columnNamesToTypes.get(s));

                } else if (db2.columnNamesToTypes.get(s) != null) {
                    newTypes.add(db2.columnNamesToTypes.get(s));
                } else {
                    throw new RuntimeException();
                }

            }

            Table db3 = new Table("selected",newColumns, newTypes);
            for (int i = 0; i < db1.getNumRows(); i++) {
                for (int j = 0; j < db2.getNumRows(); j++) {
                    Row x = db1.getRow(i).copyRow();
                    Row y = db2.getRow(j).copyRow();
                    ArrayList<Item> newitems = new ArrayList<>();
                    for (int z = 0; z < x.getLength(); z++) {
                        newitems.add(x.getItem(z));
                    }
                    for (int k = 0; k < y.getLength(); k++) {
                        newitems.add(y.getItem(k));
                    }
                    db3.addRow(newitems);
                }
            }

            return  db3;
        }

        //Get columns from left
        for (int i = 0; i < columns1.size(); i++) {
            newColumns.add(columns1.get(i));
        }
        //Get columns from right
        for (int i = 0; i < columns2.size(); i++) {
            newColumns.add(columns2.get(i));
        }

        //initialize types array
        for (String s: newColumns) {
            if (db1.columnNamesToTypes.get(s) != null) {
                newTypes.add(db1.columnNamesToTypes.get(s));

            } else if (db2.columnNamesToTypes.get(s) != null) {
                newTypes.add(db2.columnNamesToTypes.get(s));
            } else {
                throw new RuntimeException();
            }

        }
        Table db3 = new Table("selected",newColumns, newTypes);

        ArrayList<Row> commonRows1 = new ArrayList<>();
        ArrayList<Row> commonRows2 = new ArrayList<>();
        //Check which rows match using common columns
        for (int i =0; i < db1.getNumRows(); i++) {

            for (int j = 0; j < db2.getNumRows(); j++) {
                boolean correctRow = true;
                for (String s: commonColumns) {
                    //if the items aren't the same
                    //make below in helper function
                    Item z = db1.getRow(i).getItem( db1.columnNamesToIndex.get(s) );
                    Item y = db2.getRow(j).getItem( db2.columnNamesToIndex.get(s) );
                    if (!z.openItem().equals(y.openItem())) {
                        correctRow = false;
                    }
                }

                if (correctRow) {
                    commonRows1.add(db1.getRow(i));
                    commonRows2.add(db2.getRow(j));
                    //maybe don't need break
                    //break;
                }
            }

        }

        ArrayList<Row> commmonRowsFinal = new ArrayList<>();
        //filter the rows to make a single rows arraylist
        for (int i = 0; i < commonRows1.size(); i++) {
            //only add unique elements to the rows
            Row r = commonRows1.get(i);
            Row r2 = commonRows2.get(i);
            ArrayList<Item> items = new ArrayList<>();
            //copy elements that are in commonColumns
            for (String c: newColumns) { //before commonColumnns
                if (db1.columnNamesToIndex.get(c) != null) {
                    Item x = Item.copyItem( r.getItem(db1.columnNamesToIndex.get(c)) );
                    items.add(x);
                } else if (db2.columnNamesToIndex.get(c) != null) {
                    Item x = Item.copyItem( r2.getItem(db2.columnNamesToIndex.get(c)) );
                    items.add(x);
                } else {
                    throw new RuntimeException();
                }
            }
            //copy other elements

            commmonRowsFinal.add(new Row(items));

        }

        for (int i = 0; i < commmonRowsFinal.size(); i++) {
            db3.addRow(commmonRowsFinal.get(i).items);
        }

        return db3;
    }

    public static Table join(Table db1, Table db2, String name) {
        Table tb3 = Table.join(db1, db2);
        tb3.tableName = name;
        return  tb3;

    }

    public static boolean commonRows(Row r1, Row r2, ArrayList<String> commonColumns) {
        return false;

    }

    public ArrayList<Integer> getRowIndicesFromColumns(ArrayList<String> columns){


        //What if the columns aren't passed in order?
        ArrayList<Integer> indices = new ArrayList<>();
        for (int i = 0; i < columns.size();i++) {
            if (columnNamesToIndex.get(columns.get(i)) != null) {
                indices.add(columnNamesToIndex.get(columns.get(i)));
            }
            else {
                indices.add(null);
            }
        }
        return indices;
    }

    public void addRow(ArrayList<Item> row) {
        Row r = new Row(row);
        if (checkRow(r)) {
            rows.add(r);
        } else {
            throw new RuntimeException();
        }
    }

    public boolean checkRow(Row r) {
        Iterator it = columnNamesToTypes.entrySet().iterator();
        int itemIndex = 0;
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            if (!checkType(r.getItem(itemIndex), (DataTypes) pair.getValue())) {
                DataTypes itemTyp = r.getItem(itemIndex).getType();
                if (itemTyp == DataTypes.NOVALUEType || itemTyp == DataTypes.NaNType) {
                    r.getItem(itemIndex).convertFromSpecialType((DataTypes) pair.getValue());
                } else {
                    return false;
                }
            }
            itemIndex++;
        }
        if (itemIndex < r.size()) {
            return false;
        }
        return true;
    }

    public boolean checkType(Item obj, DataTypes type) {
        if (obj.getType() == type) {
            return true;
        } else {
            return false;
        }
    }

    public Row getRow(int i) {
        return rows.get(i);
    }

    public int getNumRows(){
        return rows.size();
    }

    @Override
    public String toString() {

        //Turns column names to strings
        StringBuffer table = new StringBuffer();
        Iterator it = columnNamesToTypes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            if (it.hasNext()) {
                table.append(pair.getKey() + " " + pair.getValue().toString().substring(0, pair.getValue().toString().length() - 4) + ",");
            } else {
                table.append(pair.getKey() + " " + pair.getValue().toString().substring(0, pair.getValue().toString().length() - 4));
            }
            // avoids a ConcurrentModificationException
        }
        if (getNumRows() > 0) {
            table.append("\n");
        }

        //print rows
        for (int i =0; i < getNumRows() - 1; i++) {
            table.append(getRow(i).toString() + "\n");
        }
        if (getNumRows() > 0) {
            table.append(getRow(getNumRows() - 1).toString());
        }
        return table.toString();

        /*

        //Turns column names to strings
        String table = "";
        Iterator it = columnNamesToTypes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            if (it.hasNext()) {
                table += pair.getKey() + " " + pair.getValue().toString().substring(0, pair.getValue().toString().length() - 4) + ",";
            } else {
                table += pair.getKey() + " " + pair.getValue().toString().substring(0, pair.getValue().toString().length() - 4);
            }
            // avoids a ConcurrentModificationException
        }
        if (getNumRows() > 0) {
            table += "\n";
        }

        //print rows
        for (int i =0; i < getNumRows() - 1; i++) {
            table += getRow(i).toString() + "\n";
        }
        if (getNumRows() > 0) {
            table += getRow(getNumRows() - 1).toString();
        }
        return table;
         */

    }

    public void printTable() {
        String t = this.toString();
        System.out.print(t);
    }



    public String getName() {
        return tableName;
    }

    public ArrayList<String> getColumnNames() {
        return columnNames;

    }


}




