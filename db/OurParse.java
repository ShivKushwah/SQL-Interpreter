package db;


import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.StringJoiner;

public class OurParse {
    // Various common constructs, simplifies parsing.
    private static final String REST  = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*",
            AND   = "\\s+and\\s+";

    // Stage 1 syntax, contains the command name.
    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
            LOAD_CMD   = Pattern.compile("load " + REST),
            STORE_CMD  = Pattern.compile("store " + REST),
            DROP_CMD   = Pattern.compile("drop table " + REST),
            INSERT_CMD = Pattern.compile("insert into " + REST),
            PRINT_CMD  = Pattern.compile("print " + REST),
            SELECT_CMD = Pattern.compile("select " + REST);

    // Stage 2 syntax, contains the clauses of commands.
    private static final Pattern CREATE_NEW  = Pattern.compile("(\\S+)\\s+\\((\\S+\\s+\\S+\\s*" +
            "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS  = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+" +
                    "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+" +
                    "([\\w\\s+\\-*/'<>=!.]+?(?:\\s+and\\s+" +
                    "[\\w\\s+\\-*/'<>=!.]+?)*))?"),
            CREATE_SEL  = Pattern.compile("(\\S+)\\s+as select\\s+" +
                    SELECT_CLS.pattern()),
            INSERT_CLS  = Pattern.compile("(\\S+)\\s+values\\s+(.+?" +
                    "\\s*(?:,\\s*.+?\\s*)*)");

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Expected a single query argument");
            return;
        }

       // eval(args[0]);
    }

    public static String eval(String query, Database db) {
        try {


            Matcher m;
            if ((m = CREATE_CMD.matcher(query)).matches()) {
                return createTable(m.group(1), db);
            } else if ((m = LOAD_CMD.matcher(query)).matches()) {
                return loadTable(m.group(1), db);
            } else if ((m = STORE_CMD.matcher(query)).matches()) {
                return storeTable(m.group(1), db);
            } else if ((m = DROP_CMD.matcher(query)).matches()) {
                return dropTable(m.group(1), db);
            } else if ((m = INSERT_CMD.matcher(query)).matches()) {
                return insertRow(m.group(1), db);
            } else if ((m = PRINT_CMD.matcher(query)).matches()) {
                return printTable(m.group(1), db);
            } else if ((m = SELECT_CMD.matcher(query)).matches()) {
                return select(m.group(1), db);
            } else {
                return "ERROR: Malformed query";//System.err.printf("Malformed query: %s\n", query);
            }

        }catch (Exception e) {
            return "ERROR: Misc input error";
            }

    }

    private static String createTable(String expr, Database db) {
        Matcher m;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            return createNewTable(m.group(1), m.group(2).split(COMMA), db);
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            return createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4), db);
        } else {

            return "ERROR: Malformed create";
        }
    }

    private static String createNewTable(String name, String[] cols, Database db) {
        StringJoiner joiner = new StringJoiner(", "); //", "
        for (int i = 0; i < cols.length-1; i++) {
            joiner.add(cols[i]);
            //System.out.println(cols[i]);
        }
        //for (int i = 0; i< cols.length; i++) {
            //System.out.println(cols[i]);
        //}
        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<DataTypes> types = new ArrayList<>();
        String[] temp;// = new String[2];
        for (int i =0; i < cols.length; i++ ) {
            temp = cols[i].trim().split("\\s+");
            columnNames.add(temp[0]);
            try {
                types.add(DataTypes.valueOf(temp[1] + "Type"));
            } catch (Exception e) {
                return "ERROR: invalid type inserted";
            }
        }
        db.loadTable(new Table(name,columnNames,types));

        //cols -> all but last column


        String colSentence = joiner.toString() + " and " + cols[cols.length-1];
        System.out.printf("You are trying to create a table named %s with the columns %s\n", name, colSentence);
        return "";
    }

    private static String createSelectedTable(String name, String exprs, String tables, String conds, Database db) {
        System.out.printf("You are trying to create a table named %s by selecting these expressions:" +
                " '%s' from the join of these tables: '%s', filtered by these conditions: '%s'\n", name, exprs, tables, conds);

        if (conds == null) {

            if (tables.split(",").length == 2) {

                if (exprs.trim().equals("*")) {
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    String table2 = strArr[1].trim();
                    Table tb1 = db.getTable(table1);
                    Table tb2 = db.getTable(table2);
                    Table tb3 = Table.join(tb1, tb2, name);
                    tb3.tableName = name;
                    db.loadTable(tb3);
                    return "";
                } else {
                    try {

                        Table newtable = evaluateColumnExpressionTwoTables(exprs, tables, conds, db);
                        newtable.tableName = name;
                        db.loadTable(newtable);
                        return "";
                    } catch (Exception e) {
                        return "ERROR: Bad input1";
                    }
                /*

                //parse columns
                //call my special method
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }

                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                Table tb3 = Table.selectMultipleDatabases(tb1, tb2, columnNames);
                tb3.tableName = name;
                db.loadTable(tb3);
                return "";
                */


                }
            } else if (tables.split(",").length == 1) {
                if (exprs.trim().equals("*")) {
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    Table tb1 = db.getTable(table1);
                    Table newtable = tb1.selectFromOneDatabase(tb1.getColumnNames());
                    newtable.tableName = name;
                    db.loadTable(newtable);
                    return "";
                }

                //parse column names
                if (exprs.trim().equals("*")) { //if all columns with star
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    Table tb1 = db.getTable(table1);
                    Table newtable = tb1.selectFromOneDatabase(tb1.getColumnNames());
                    newtable.tableName = name;
                    db.loadTable(newtable);
                    return "";
                } else { //if select columns

                    //write code that goes through each expression and evaulates
                    try {

                        Table tb = evaluateColumnExpression(exprs, tables, conds, db);
                        tb.tableName = name;
                        db.loadTable(tb);
                        return "";
                    } catch (Exception e) {
                        return "ERROR: Bad input2";
                    }
                }

                /*
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }

                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                Table tb1 = db.getTable(table1);
                Table newtable = tb1.selectFromOneDatabase(columnNames);
                newtable.tableName = name;
                db.loadTable(newtable);
                return "";
                */


            } else {

                if (exprs.trim().equals("*")) {
                /*
                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                Table tb3 = Table.join(tb1, tb2, name);
                tb3.tableName = name;
                db.loadTable(tb3);
                return "";
                */

                    String[] strArr = tables.split(",");

                    //String[] columnNameArr = exprs.split(",");
                    ArrayList<String> columnNames = new ArrayList<>();
                /*
                for (int i = 0; i < strArr.length; i++) {
                    strArr[i] = strArr[i].trim();
                    Table x = db.getTable(strArr[i]);
                    columnNames.addAll(x.columnNames);//  add(columnNameArr[i]);
                }
                */

                    ArrayList<Table> tblist = new ArrayList<>();
                    for (int i = 0; i < strArr.length; i++) {
                        tblist.add(db.getTable(strArr[i].trim()));
                    }

                    Table newTable = Table.selectMultipleDatabasesMultipleTablesStar(tblist);
                    newTable.tableName = name;
                    db.loadTable(newTable);
                    return ""; //dont use join, but rather the other function i made


                } else {

                    try {

                        Table x = evaluateColumnExpressionMultipleTables(exprs, tables, conds, db);
                        x.tableName = name;
                        db.loadTable(x);
                        return "";
                    } catch (Exception e) {
                        return "ERROR: Bad input3";
                    }

                /*


                //parse columns
                //call my special method
                String[] strArr = tables.split(",");

                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }

                ArrayList<Table> tblist = new ArrayList<>();
                for (int i = 0; i < strArr.length; i++) {
                    tblist.add(db.getTable(strArr[i].trim()));
                }

                Table newTable = Table.selectMultipleDatabasesMultipleTables(tblist, columnNames);
                newTable.tableName = name;
                db.loadTable(newTable);
                return ""; //dont use join, but rather the other function i made
                */
                }
            }

        /*
           if (tables.split(",").length == 2) {

            if (exprs.trim().equals("*")) {
                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                return Table.join(tb1,tb2).toString(); //dont use join, but rather the other function i made
            }
            else {
                //parse columns
                //call my special method
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }


                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                return Table.selectMultipleDatabases(tb1, tb2, columnNames).toString(); //dont use join, but rather the other function i made


            }
        }
        else {
            //parse column names
            if (exprs.trim().equals("*")) {
                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                Table tb1 = db.getTable(table1);
                return tb1.selectFromOneDatabase(tb1.getColumnNames()).toString();
            }
            else {

                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }


                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                Table tb1 = db.getTable(table1);
                return tb1.selectFromOneDatabase(columnNames).toString();
            }

         */


        } else { //if there are conditions



            if (tables.split(",").length == 2) {

                if (exprs.trim().equals("*")) {
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    String table2 = strArr[1].trim();
                    Table tb1 = db.getTable(table1);
                    Table tb2 = db.getTable(table2);
                    Table tb3 = filterConds(Table.join(tb1, tb2, name), conds, db);
                    tb3.tableName = name;
                    db.loadTable(tb3);
                    return "";
                } else {
                    try {

                        Table newtable = filterConds(evaluateColumnExpressionTwoTables(exprs, tables, conds, db), conds, db);
                        newtable.tableName = name;
                        db.loadTable(newtable);
                        return "";
                    } catch (Exception e) {
                        return "ERROR: Bad input4";
                    }
                /*

                //parse columns
                //call my special method
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }

                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                Table tb3 = Table.selectMultipleDatabases(tb1, tb2, columnNames);
                tb3.tableName = name;
                db.loadTable(tb3);
                return "";
                */


                }
            } else if (tables.split(",").length == 1) {
                if (exprs.trim().equals("*")) {
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    Table tb1 = db.getTable(table1);
                    Table newtable = filterConds(tb1.selectFromOneDatabase(tb1.getColumnNames()), conds, db);
                    newtable.tableName = name;
                    db.loadTable(newtable);
                    return "";
                }

                //parse column names
                if (exprs.trim().equals("*")) { //if all columns with star
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    Table tb1 = db.getTable(table1);
                    Table newtable = filterConds(tb1.selectFromOneDatabase(tb1.getColumnNames()), conds, db);
                    newtable.tableName = name;
                    db.loadTable(newtable);
                    return "";
                } else { //if select columns

                    //write code that goes through each expression and evaulates
                    try {

                        Table tb = filterConds(evaluateColumnExpression(exprs, tables, conds, db), conds, db);
                        tb.tableName = name;
                        db.loadTable(tb);
                        return "";
                    } catch (Exception e) {
                        return "ERROR: Bad input5";
                    }
                }

                /*
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }

                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                Table tb1 = db.getTable(table1);
                Table newtable = tb1.selectFromOneDatabase(columnNames);
                newtable.tableName = name;
                db.loadTable(newtable);
                return "";
                */


            } else {

                if (exprs.trim().equals("*")) {
                /*
                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                Table tb3 = Table.join(tb1, tb2, name);
                tb3.tableName = name;
                db.loadTable(tb3);
                return "";
                */

                    String[] strArr = tables.split(",");

                    //String[] columnNameArr = exprs.split(",");
                    ArrayList<String> columnNames = new ArrayList<>();
                /*
                for (int i = 0; i < strArr.length; i++) {
                    strArr[i] = strArr[i].trim();
                    Table x = db.getTable(strArr[i]);
                    columnNames.addAll(x.columnNames);//  add(columnNameArr[i]);
                }
                */

                    ArrayList<Table> tblist = new ArrayList<>();
                    for (int i = 0; i < strArr.length; i++) {
                        tblist.add(db.getTable(strArr[i].trim()));
                    }

                    Table newTable = filterConds(Table.selectMultipleDatabasesMultipleTablesStar(tblist), conds, db);
                    newTable.tableName = name;
                    db.loadTable(newTable);
                    return ""; //dont use join, but rather the other function i made


                } else {

                    try {

                        Table x = filterConds(evaluateColumnExpressionMultipleTables(exprs, tables, conds, db), conds, db);
                        x.tableName = name;
                        db.loadTable(x);
                        return "";
                    } catch (Exception e) {
                        return "ERROR: Bad input6";
                    }

                /*


                //parse columns
                //call my special method
                String[] strArr = tables.split(",");

                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }

                ArrayList<Table> tblist = new ArrayList<>();
                for (int i = 0; i < strArr.length; i++) {
                    tblist.add(db.getTable(strArr[i].trim()));
                }

                Table newTable = Table.selectMultipleDatabasesMultipleTables(tblist, columnNames);
                newTable.tableName = name;
                db.loadTable(newTable);
                return ""; //dont use join, but rather the other function i made
                */
                }
            }

        /*
           if (tables.split(",").length == 2) {

            if (exprs.trim().equals("*")) {
                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                return Table.join(tb1,tb2).toString(); //dont use join, but rather the other function i made
            }
            else {
                //parse columns
                //call my special method
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }


                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                return Table.selectMultipleDatabases(tb1, tb2, columnNames).toString(); //dont use join, but rather the other function i made


            }
        }
        else {
            //parse column names
            if (exprs.trim().equals("*")) {
                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                Table tb1 = db.getTable(table1);
                return tb1.selectFromOneDatabase(tb1.getColumnNames()).toString();
            }
            else {

                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }


                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                Table tb1 = db.getTable(table1);
                return tb1.selectFromOneDatabase(columnNames).toString();
            }

         */






        }

    }

    private static String loadTable(String name, Database db) {
        System.out.printf("You are trying to load the table named %s\n", name);
        try {
            Table loaded = FileManager.readTable(name + ".tbl");
            if (loaded == null) {
                return "ERROR: File doesn't exist!";
            }
            db.loadTable(loaded);
            return "";

            //return FileManager.readTable(name + ".txt").toString();
        } catch (IOException e) {
            e.printStackTrace();
            return "ERROR: File doesn't exist!";
        } catch (Exception e) {
            return "ERROR: Malformed TBL file";
        }
    }

    private static String storeTable(String name, Database db) {
        System.out.printf("You are trying to store the table named %s\n", name);
        Table tableToStore = db.tablesInMemory.get(name);
        if (tableToStore != null) {
            FileManager.storeTable(tableToStore);
            return "";
        }
        return "ERROR: Table doesn't exist!";
    }

    private static String dropTable(String name, Database db) {
        System.out.printf("You are trying to drop the table named %s\n", name);
        try {
            db.removeTable(name);
        } catch (RuntimeException e) {
            return "ERROR: Table doesn't exist!";
        }
        return "";

    }

    private static String  insertRow(String expr, Database db) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            //System.err.printf("ERROR: Malformed insert: %s\n", expr);
            //System.out.println("Are we returning this?");
            return "ERROR: Malformed insert";
        }

        Table table = db.getTable(m.group(1));
        ArrayList<Item> rowToAdd = Row.fromString(m.group(2));
            /*
            String[] strArr = m.group(2).split(",");
            for (int i = 0; i < strArr.length; i++) {
                strArr[i] = strArr[i].trim();
            }
            Table table = db.getTable(m.group(1));
            ArrayList<Item> rowToAdd = new ArrayList<>();

            for (int i = 0; i < strArr.length; i++) {
                rowToAdd.add(new Item(strArr[i]));
            }
            */
            //Row row = new Row(rowToAdd);
            try {
                table.addRow(rowToAdd);
            } catch (RuntimeException e) {
                return "ERROR: Row is of incorrect format";

            }



        //TODO: Uncomment below for debugging
        //System.out.printf("You are trying to insert the row \"%s\" into the table %s\n", m.group(2), m.group(1));
        return "";
    }

    private static String printTable(String name, Database db) {
        try {
            Table x = db.getTable(name);
            return x.toString(); //db.tablesInMemory.get(name);
        } catch (NullPointerException e) {
            return "ERROR: Table doesn't exist";
        }
        //System.out.printf("You are trying to print the table named %s\n", name);
    }

    private static String select(String expr, Database db) {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            //System.err.printf("Malformed select: %s\n", expr);
            return "ERROR: Malformed select";
        }

        return select(m.group(1), m.group(2), m.group(3), db);
    }

    private static String select(String exprs, String tables, String conds, Database db) {
        //TODO: Add the where clause to the create select method
        System.out.printf("You are trying to select these expressions:" +
                " '%s' from the join of these tables: '%s', filtered by these conditions: '%s'\n", exprs, tables, conds);
        System.out.println(tables);
        //System.out.println(conds);
        //essentially, split exprs by space, and then pass each expr to a evaluate expressions method
        //if expr is of length one, do normal column crap
        //else, split expr by space and retrieve the operator and other crap and do special stuff

        if (conds == null) { //TODO: Add this if statemetn to create select

            if (tables.split(",").length == 2) { //if there are 2 tables

                if (exprs.trim().equals("*")) { //if star
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    String table2 = strArr[1].trim();
                    Table tb1 = db.getTable(table1);
                    Table tb2 = db.getTable(table2);
                    return Table.join(tb1, tb2).toString(); //dont use join, but rather the other function i made
                } else { //if column expressions
                    //write code that goes through each expression and evaulates
                    try {

                        return evaluateColumnExpressionTwoTables(exprs, tables, conds, db).toString();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return "ERROR: Bad input1";
                    }
                /*
                //parse columns
                //call my special method
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }


                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                return Table.selectMultipleDatabases(tb1, tb2, columnNames).toString(); //dont use join, but rather the other function i made

                */
                }
            } else if (tables.split(",").length == 1) { //if one table
                //TODO: Add column expression functionality to all if statements of this method
                //parse column names
                if (exprs.trim().equals("*")) { //if all columns with star
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    Table tb1 = db.getTable(table1);
                    return tb1.selectFromOneDatabase(tb1.getColumnNames()).toString();
                } else { //if select columns

                    //write code that goes through each expression and evaulates
                    try {

                        return evaluateColumnExpression(exprs, tables, conds, db).toString();
                    } catch (Exception e) {
                        return "ERROR: Bad input2";
                    }
                /*
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }


                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                Table tb1 = db.getTable(table1);
                return tb1.selectFromOneDatabase(columnNames).toString();
                */
                }


            } else { //if more than 2 tables

                if (exprs.trim().equals("*")) {
                    String[] strArr = tables.split(",");
                    ArrayList<Table> tblist = new ArrayList<>();
                    for (int i = 0; i < strArr.length; i++) {
                        tblist.add(db.getTable(strArr[i].trim()));
                    }
                    return Table.joinMultiple(tblist).toString(); //dont use join, but rather the other function i made
                } else {


                    try {

                        return evaluateColumnExpressionMultipleTables(exprs, tables, conds, db).toString();
                    } catch (Exception e) {
                        return "ERROR: Bad input3";
                    }


                /*

                //parse columns
                //call my special method

                String[] strArr = tables.split(",");

                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }

                ArrayList<Table> tblist = new ArrayList<>();
                for (int i = 0; i < strArr.length; i++) {
                    tblist.add(db.getTable(strArr[i].trim()));
                }


                return Table.selectMultipleDatabasesMultipleTables(tblist, columnNames).toString(); //dont use join, but rather the other function i made
            } */

                }


                //return "";
            }
        } else { //if conditions aren't null

            if (tables.split(",").length == 2) { //if there are 2 tables

                if (exprs.trim().equals("*")) { //if star
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    String table2 = strArr[1].trim();
                    Table tb1 = db.getTable(table1);
                    Table tb2 = db.getTable(table2);
                    //TODO: Filter table by conds
                    Table x = Table.join(tb1, tb2);
                    Table y = filterConds(x, conds, db);
                    return y.toString(); //dont use join, but rather the other function i made
                } else { //if column expressions
                    //write code that goes through each expression and evaulates
                    try {
                        //TODO: Filter table by conds
                        Table x = evaluateColumnExpressionTwoTables(exprs, tables, conds, db);
                        Table y = filterConds(x, conds, db);
                        return y.toString();

                    } catch (Exception e) {
                        return "ERROR: Bad input4";
                    }
                /*
                //parse columns
                //call my special method
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }


                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                String table2 = strArr[1].trim();
                Table tb1 = db.getTable(table1);
                Table tb2 = db.getTable(table2);
                return Table.selectMultipleDatabases(tb1, tb2, columnNames).toString(); //dont use join, but rather the other function i made

                */
                }
            } else if (tables.split(",").length == 1) { //if one table
                //parse column names
                if (exprs.trim().equals("*")) { //if all columns with star
                    String[] strArr = tables.split(",");
                    String table1 = strArr[0];
                    Table tb1 = db.getTable(table1);
                    //TODO: Filter table by conds
                    Table x = tb1.selectFromOneDatabase(tb1.getColumnNames());
                    Table y = filterConds(x, conds, db);
                    return y.toString();
                } else { //if select columns

                    //write code that goes through each expression and evaulates
                    try {
                        //TODO: Filter table by conds
                        Table x = evaluateColumnExpression(exprs, tables, conds, db);
                        Table y = filterConds(x, conds, db);
                        return y.toString();
                    } catch (Exception e) {
                        return "ERROR: Bad input5";
                    }
                /*
                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }


                String[] strArr = tables.split(",");
                String table1 = strArr[0];
                Table tb1 = db.getTable(table1);
                return tb1.selectFromOneDatabase(columnNames).toString();
                */
                }


            } else { //if more than 2 tables

                if (exprs.trim().equals("*")) {
                    String[] strArr = tables.split(",");
                    ArrayList<Table> tblist = new ArrayList<>();
                    for (int i = 0; i < strArr.length; i++) {
                        tblist.add(db.getTable(strArr[i].trim()));
                    }
                    //TODO: Filter table by conds
                    Table x = Table.joinMultiple(tblist);
                    Table y = filterConds(x, conds , db);
                    return y.toString(); //dont use join, but rather the other function i made
                } else {


                    try {
                        //TODO: Filter table by conds
                        Table x = evaluateColumnExpressionMultipleTables(exprs, tables, conds, db);
                        Table y = OurParse.filterConds(x, conds, db);
                        return y.toString();
                    } catch (Exception e) {
                        return "ERROR: Bad input6";
                    }


                /*

                //parse columns
                //call my special method

                String[] strArr = tables.split(",");

                String[] columnNameArr = exprs.split(",");
                ArrayList<String> columnNames = new ArrayList<>();
                for (int i = 0; i < columnNameArr.length; i++) {
                    columnNameArr[i] = columnNameArr[i].trim();
                    columnNames.add(columnNameArr[i]);
                }

                ArrayList<Table> tblist = new ArrayList<>();
                for (int i = 0; i < strArr.length; i++) {
                    tblist.add(db.getTable(strArr[i].trim()));
                }


                return Table.selectMultipleDatabasesMultipleTables(tblist, columnNames).toString(); //dont use join, but rather the other function i made
            } */

                }


                //return "";
            }
        }
    }

    public static Table evaluateColumnExpression(String exprs, String tables, String conds, Database db) {

        String[] strArr = tables.split(",");
        String table1 = strArr[0].trim();
        Table tb1 = db.getTable(table1); //the table

        ArrayList<String> columnNames = new ArrayList<>(); //all columns that should be in new table
        ArrayList<String> newColumnsFromExpression = new ArrayList<>(); //just the columns that are created by expression statement
        ArrayList<DataTypes> dataTypesOfColumnExpressions = new ArrayList<>(); //datatypes of above columns
        ArrayList<ArrayList<String>> columnOperation = new ArrayList<>(); //has arrays of format [first column, second column, operation], which are columm expression arrays
        String[] colExprs = exprs.split(",");
        for (int i = 0; i < colExprs.length; i++) {//iterate throughout all of the expressions
            colExprs[i] = colExprs[i].trim();

            //If there is no operator in colsExprs[i], then do columnNames.add(colExprs[i]);
            //String[] strExpression2 = colExprs[i].split("\\s+");
            //String firstExp = strExpression2[0];
            //TODO: Change below in the 2 other evaluateColumnExpression methods
            String[] stuff = colExprs[i].split("\\s+");
            String[] stuff2 = colExprs[i].split("(\\+|-|\\*|/)");
            String stuff3 = colExprs[i];
            if (colExprs[i].split("\\s+")[0].length() == colExprs[i].length() || colExprs[i].split("(\\+|-|\\*|/)")[0].length() == colExprs[i].length()) {





            //if (colExprs[i].length() == 1) { //if just a column with no expression BUG IS THAT STRINGS DONT WORK
                columnNames.add(colExprs[i]);
            }
            else {
                //Parse the name of the new column
                //add it to columnNames, make sure selectFromDatabase can handle columns with values on the actual table
                String operator = ""; //Assume this is the string of the operator
                //Operation op = getOperator(operator);
                String s = ""; //Assume this is the name of the new column
                String x = ""; //Assume this is the name of the first of the two columns that is being added/subtracted
                String y = ""; //Assume this is the name of one of the other column that is being added/subtracted
                String z = "";

                //Parsing below
                String[] strExpression = colExprs[i].split("\\s+");
                if (strExpression.length == 3 && strExpression[1].equals("as")) {
                    s = strExpression[2];
                    String firstExp = strExpression[0];
                    String[] firstExpBroken = firstExp.split("(\\+|-|\\*|/)");
                    if (firstExpBroken.length  != 2) {
                        throw new RuntimeException();
                        //return  "ERROR: Wrong input";
                    }
                    x = firstExpBroken[0];
                    y = firstExpBroken[1];
                    operator = firstExp.substring(firstExpBroken[0].length(), firstExp.length() - firstExpBroken[1].length() );



                }
                else if (strExpression.length == 5 && strExpression[3].equals("as"))
                {
                    x = strExpression[0];
                    operator = strExpression[1];
                    y = strExpression[2];
                    s = strExpression[4];
                } else {
                    throw new RuntimeException();
                }

                //Parsing ends

                if ( (tb1.columnNamesToTypes.get(x) == DataTypes.floatType && tb1.columnNamesToTypes.get(y) == DataTypes.intType) || (tb1.columnNamesToTypes.get(y) == DataTypes.floatType && tb1.columnNamesToTypes.get(x) == DataTypes.intType) ) {
                    dataTypesOfColumnExpressions.add(DataTypes.floatType);
                }
                else {
                    dataTypesOfColumnExpressions.add(tb1.columnNamesToTypes.get(x));        //Add the type of the new column to the arraylist
                }


                //dataTypesOfColumnExpressions.add(tb1.columnNamesToTypes.get(x));        //Add the type of the new column to the arraylist
                columnNames.add(s);
                newColumnsFromExpression.add(s);
                ArrayList<String> temp = new ArrayList<>();
                temp.add(x);
                temp.add(y);
                temp.add(operator);
                temp.add(Integer.toString(i));
                columnOperation.add(temp);
            }
        }
        try {

            return tb1.selectFromOneDatabaseColumnExpressions(columnNames, newColumnsFromExpression, dataTypesOfColumnExpressions, columnOperation);
        } catch (Exception e) {
            throw new RuntimeException();
        }
        //return tb1.selectFromOneDatabase(columnNames).toString();


        /*
        //if all expressions are column expressions
        String[] columnNameArr = exprs.split(",");
        ArrayList<String> columnNames = new ArrayList<>();
        for (int i = 0; i < columnNameArr.length; i++) {
            columnNameArr[i] = columnNameArr[i].trim();
            columnNames.add(columnNameArr[i]);
        }


        String[] strArr = tables.split(",");
        String table1 = strArr[0];
        Table tb1 = db.getTable(table1);
        return tb1.selectFromOneDatabase(columnNames).toString();
        */

    }

    public static Table evaluateColumnExpressionMultipleTables(String exprs, String tables, String conds, Database db) {

        String[] strArr = tables.split(",");
        String table1 = strArr[0].trim();
        String table2 = strArr[1].trim();
        Table xx = Table.join(db.getTable(table1),db.getTable(table2));

        for (int i = 2; i < strArr.length; i++) {
            xx = Table.join(xx, db.getTable(strArr[i].trim()));
        }

        //join all tables through iteration
        //Table tb1 = db.getTable(table1); //the table
        Table tb1 = xx;

        ArrayList<String> columnNames = new ArrayList<>(); //all columns that should be in new table
        ArrayList<String> newColumnsFromExpression = new ArrayList<>(); //just the columns that are created by expression statement
        ArrayList<DataTypes> dataTypesOfColumnExpressions = new ArrayList<>(); //datatypes of above columns
        ArrayList<ArrayList<String>> columnOperation = new ArrayList<>(); //has arrays of format [first column, second column, operation], which are columm expression arrays
        String[] colExprs = exprs.split(",");
        for (int i = 0; i < colExprs.length; i++) {//iterate throughout all of the expressions
            colExprs[i] = colExprs[i].trim();

            if (colExprs[i].split("\\s+")[0].length() == colExprs[i].length() || colExprs[i].split("(\\+|-|\\*|/)")[0].length() == colExprs[i].length()) {

            //if (colExprs[i].length() == 1) { //if just a column with no expression
                columnNames.add(colExprs[i]);
            }
            else {
                //Parse the name of the new column
                //add it to columnNames, make sure selectFromDatabase can handle columns with values on the actual table
                String operator = ""; //Assume this is the string of the operator
                //Operation op = getOperator(operator);
                String s = ""; //Assume this is the name of the new column
                String x = ""; //Assume this is the name of the first of the two columns that is being added/subtracted
                String y = ""; //Assume this is the name of one of the other column that is being added/subtracted

                //Parsing below
                String[] strExpression = colExprs[i].split("\\s+");
                if (strExpression.length == 3 && strExpression[1].equals("as")) {
                    s = strExpression[2];
                    String firstExp = strExpression[0];
                    String[] firstExpBroken = firstExp.split("(\\+|-|\\*|/)");
                    if (firstExpBroken.length  != 2) {
                        throw new RuntimeException();
                        //return  "ERROR: Wrong input";
                    }
                    x = firstExpBroken[0];
                    y = firstExpBroken[1];
                    operator = firstExp.substring(firstExpBroken[0].length(), firstExp.length() - firstExpBroken[1].length() );



                }
                else if (strExpression.length == 5 && strExpression[3].equals("as"))
                {
                    x = strExpression[0];
                    operator = strExpression[1];
                    y = strExpression[2];
                    s = strExpression[4];
                } else {
                    throw new RuntimeException();
                }

                //Parsing ends

                if ( (tb1.columnNamesToTypes.get(x) == DataTypes.floatType && tb1.columnNamesToTypes.get(y) == DataTypes.intType) || (tb1.columnNamesToTypes.get(y) == DataTypes.floatType && tb1.columnNamesToTypes.get(x) == DataTypes.intType) ) {
                    dataTypesOfColumnExpressions.add(DataTypes.floatType);
                }
                else {
                    dataTypesOfColumnExpressions.add(tb1.columnNamesToTypes.get(x));        //Add the type of the new column to the arraylist
                }


                //dataTypesOfColumnExpressions.add(tb1.columnNamesToTypes.get(x));        //Add the type of the new column to the arraylist
                columnNames.add(s);
                newColumnsFromExpression.add(s);
                ArrayList<String> temp = new ArrayList<>();
                temp.add(x);
                temp.add(y);
                temp.add(operator);
                temp.add(Integer.toString(i));
                columnOperation.add(temp);
            }
        }
        try {

            return tb1.selectFromOneDatabaseColumnExpressions(columnNames, newColumnsFromExpression, dataTypesOfColumnExpressions, columnOperation);
        } catch (Exception e) {
            throw new RuntimeException();
        }
        //return tb1.selectFromOneDatabase(columnNames).toString();


        /*
        //if all expressions are column expressions
        String[] columnNameArr = exprs.split(",");
        ArrayList<String> columnNames = new ArrayList<>();
        for (int i = 0; i < columnNameArr.length; i++) {
            columnNameArr[i] = columnNameArr[i].trim();
            columnNames.add(columnNameArr[i]);
        }


        String[] strArr = tables.split(",");
        String table1 = strArr[0];
        Table tb1 = db.getTable(table1);
        return tb1.selectFromOneDatabase(columnNames).toString();
        */

    }
    public static Table evaluateColumnExpressionTwoTables(String exprs, String tables, String conds, Database db) {

        String[] strArr = tables.split(",");
        String table1 = strArr[0].trim();
        String table2 = strArr[1].trim();
        Table tb1 = Table.join(db.getTable(table1),db.getTable(table2));//the table
        //Table tb1 = db.getTable(table1); //the table

        ArrayList<String> columnNames = new ArrayList<>(); //all columns that should be in new table
        ArrayList<String> newColumnsFromExpression = new ArrayList<>(); //just the columns that are created by expression statement
        ArrayList<DataTypes> dataTypesOfColumnExpressions = new ArrayList<>(); //datatypes of above columns
        ArrayList<ArrayList<String>> columnOperation = new ArrayList<>(); //has arrays of format [first column, second column, operation], which are columm expression arrays
        String[] colExprs = exprs.split(",");
        for (int i = 0; i < colExprs.length; i++) {//iterate throughout all of the expressions
            colExprs[i] = colExprs[i].trim();

            if (colExprs[i].split("\\s+")[0].length() == colExprs[i].length() || colExprs[i].split("(\\+|-|\\*|/)")[0].length() == colExprs[i].length()) {
            //if (colExprs[i].length() == 1) { //if just a column with no expression
                columnNames.add(colExprs[i]);
            }
            else {
                //Parse the name of the new column
                //add it to columnNames, make sure selectFromDatabase can handle columns with values on the actual table
                String operator = ""; //Assume this is the string of the operator
                //Operation op = getOperator(operator);
                String s = ""; //Assume this is the name of the new column
                String x = ""; //Assume this is the name of the first of the two columns that is being added/subtracted
                String y = ""; //Assume this is the name of one of the other column that is being added/subtracted

                //Parsing below
                String[] strExpression = colExprs[i].split("\\s+");
                if (strExpression.length == 3 && strExpression[1].equals("as")) {
                    s = strExpression[2];
                    String firstExp = strExpression[0];
                    String[] firstExpBroken = firstExp.split("(\\+|-|\\*|/)");
                    if (firstExpBroken.length  != 2) {
                        throw new RuntimeException();
                        //return  "ERROR: Wrong input";
                    }
                    x = firstExpBroken[0];
                    y = firstExpBroken[1];
                    operator = firstExp.substring(firstExpBroken[0].length(), firstExp.length() - firstExpBroken[1].length() );



                }
                else if (strExpression.length == 5 && strExpression[3].equals("as"))
                {
                    x = strExpression[0];
                    operator = strExpression[1];
                    y = strExpression[2];
                    s = strExpression[4];
                } else {
                    throw new RuntimeException();
                }

                //Parsing ends

                //TODO: Do this in 3 places
                if ( (tb1.columnNamesToTypes.get(x) == DataTypes.floatType && tb1.columnNamesToTypes.get(y) == DataTypes.intType) || (tb1.columnNamesToTypes.get(y) == DataTypes.floatType && tb1.columnNamesToTypes.get(x) == DataTypes.intType) ) {
                    dataTypesOfColumnExpressions.add(DataTypes.floatType);
                }
                else {
                    dataTypesOfColumnExpressions.add(tb1.columnNamesToTypes.get(x));        //Add the type of the new column to the arraylist
                }
                columnNames.add(s);
                newColumnsFromExpression.add(s);
                ArrayList<String> temp = new ArrayList<>();
                temp.add(x);
                temp.add(y);
                temp.add(operator);
                temp.add(Integer.toString(i));
                columnOperation.add(temp);
            }
        }
        try {

            return tb1.selectFromOneDatabaseColumnExpressions(columnNames, newColumnsFromExpression, dataTypesOfColumnExpressions, columnOperation);
        } catch (Exception e) {
            throw new RuntimeException();
        }
        //return tb1.selectFromOneDatabase(columnNames).toString();


        /*
        //if all expressions are column expressions
        String[] columnNameArr = exprs.split(",");
        ArrayList<String> columnNames = new ArrayList<>();
        for (int i = 0; i < columnNameArr.length; i++) {
            columnNameArr[i] = columnNameArr[i].trim();
            columnNames.add(columnNameArr[i]);
        }


        String[] strArr = tables.split(",");
        String table1 = strArr[0];
        Table tb1 = db.getTable(table1);
        return tb1.selectFromOneDatabase(columnNames).toString();
        */

    }


    public static Table filterConds(Table table, String conds, Database db) {

        //Get arraylist of conditions, each element is an arraylist
        ArrayList<ArrayList<String>> columnConditionsLiterals = new ArrayList<ArrayList<String>>();//arraylist of an arraylist that contains [column 1, conditional, literal]
        ArrayList<ArrayList<String>> columnConditionsColumns = new ArrayList<ArrayList<String>>(); //arraylist of an arraylist that contains [column 1, conditional, column 2]


        //parse conds
        String[] strArr = conds.split("\\s*and\\s*" );//
        for (int i = 0; i < strArr.length; i++) {
            String expression = strArr[i].trim();
            String[] expressionsBroken = expression.split("\\s+");
            ArrayList<String> exp = new ArrayList<>();
            exp.add(expressionsBroken[0].trim());
            exp.add(expressionsBroken[1].trim());
            exp.add(expressionsBroken[2].trim());
            //If the last expression is a column in the table
            if (table.columnNames.contains(expressionsBroken[2].trim())) {
                columnConditionsColumns.add(exp);
            }
            else {
                columnConditionsLiterals.add(exp);
            }

        }

//        ArrayList<String> test1 = new ArrayList<>();
//        test1.add("x");
//        test1.add("<");
//        test1.add("y");
//        columnConditionsColumns.add(test1);

        return Table.filterTableWithConds(table, columnConditionsLiterals, columnConditionsColumns, db);

    }


    public static Operation getOperator(String s) {
        if (s.trim().equals("+")) {
            return Operation.Plus;
        }
        else if (s.trim().equals("-")) {
            return Operation.Minus;
        } else if (s.trim().equals("*")) {
            return Operation.Multiply;
        }
        else if (s.trim().equals("/")) {
            return Operation.Divide;
        }
        else {
            return null;
        }

    }

    public static ComparisonOperators getComparisonOperator(String s) {
        if (s.trim().equals("<")) {
            return ComparisonOperators.LessThan;
        }
        else if (s.trim().equals("<=")) {
            return ComparisonOperators.LessThanOrEqualTo;
        }
        else if (s.trim().equals("==")) {
            return ComparisonOperators.EqualTo;
        }
        else if (s.trim().equals("!=")) {
            return ComparisonOperators.DoesNotEqual;
        }
        else if (s.trim().equals(">=")) {
            return ComparisonOperators.GreaterThanOrEqualTo;
        }
        else if (s.trim().equals(">")) {
            return ComparisonOperators.GreaterThan;
        }
        else {
            return null;
        }
    }
}
