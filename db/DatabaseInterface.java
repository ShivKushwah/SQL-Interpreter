package db;

import java.util.ArrayList;

/**
 * Created by CoryM on 3/1/2017.
 */
public interface DatabaseInterface {
    void addRow(ArrayList<Item> row);
    void printTable();
    String getName();



}
