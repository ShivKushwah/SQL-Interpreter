package db;

import java.util.HashMap;

public class Database {

    HashMap<String, Table> tablesInMemory = new HashMap<>();

    //ArrayList<Table> tablesInMemory = new ArrayList<>();
    public Database() {
        // YOUR CODE HERE

    }

    public String transact(String query) {
        //System.out.println(query);
        return OurParse.eval(query, this);
    }

    public void loadTable(Table x) {
        tablesInMemory.put(x.getName(), x);
        //tablesInMemory.add(x);
    }

    public Table getTable(String name) {
        return tablesInMemory.get(name);
    }

    public void removeTable(String name) {
        if (tablesInMemory.get(name) != null) {
            tablesInMemory.remove(name);
        } else {
            throw new RuntimeException();
        }
    }


}
