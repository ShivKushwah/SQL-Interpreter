package db;

import java.util.ArrayList;

import static java.lang.Integer.parseInt;

/**
 * Created by Shiv on 2/25/17.
 */
public class Row {

    ArrayList<Item> items;

    public Row(ArrayList<Item> items) {
        this.items = items;
    }

    public int getLength() {
        return items.size();
    }

    public Item getItem(int index) {
        return items.get(index);
    }

    @Override
    public String toString() {
        String s = "";
        for (int i = 0; i < getLength(); i++) {
            if (i == getLength() - 1) {
                s += items.get(i).toString();

            } else {
                s += items.get(i).toString() + ",";
            }
        }
        return s;

    }

    public Row copyRow() {
        ArrayList<Item> copyitems = new ArrayList<>();
        for (int i =0; i < items.size(); i++) {
            Item x = new Item(items.get(i).openItem());
            copyitems.add(x);
        }
        return new Row(copyitems);
    }

    //Convert the String representation of a row to a list of Item objects
    //Convert the String representation of a row to a list of Item objects
    public static ArrayList<Item> fromString(String inputString) {
        String[] strArr = inputString.split(",");
        for (int i = 0; i < strArr.length; i++) {
            strArr[i] = strArr[i].trim();
        }
        ArrayList<Item> rowItems = new ArrayList<>();
        for (String string : strArr) {
            if(string.startsWith("'") || string.startsWith("N")) {
                rowItems.add(new Item(string));
            } else if (string.contains(".")) {
                rowItems.add(new Item(Float.parseFloat(string)));
            } else {
                rowItems.add(new Item(parseInt(string)));
            }
        }
        return rowItems;
    }

    public int size() {
        return items.size();
    }

}
