package db;



/**
 * Created by Shiv on 2/25/17.
 */
public class Item<Type> {

    private Type value;
    private DataTypes typ;

    public Item(Type item) {
        addItem(item);
        if (item instanceof String) {
            switch ((String) item) {
                case "NOVALUE":
                    typ = DataTypes.NOVALUEType;
                    break;
                case "NaN":
                    typ = DataTypes.NaNType;
                    break;
                default:
                    typ = DataTypes.stringType;
                    break;
            }
        } else if (item instanceof Integer) {
            typ = DataTypes.intType;
        }
        //does not explicitly check for float
        else {
            typ = DataTypes.floatType;
        }
    }


    public void addItem(Type item) {
        try {

            value = item;
        } catch (RuntimeException e) {
            System.out.println("Added invalid item of wrong type");
        }
    }

    public Type openItem() {
        return value;
    }

    public DataTypes getType() {
        return typ;
    }

    @Override
    public String toString() {
        if (value.toString().equals("NaN") || value.toString().equals("NOVALUE")) {
            return value.toString();
        }

        if (typ == DataTypes.floatType) {
            String[] splitter = value.toString().split("\\.");
            if (splitter[1].length() < 3) {// After  Decimal Count
                String result = value.toString();
                for (int i = 0; i < 3 - splitter[1].length(); i ++) {
                    result += "0";
                }
                return result;
                //BigDecimal result;
                //result = round((float) (Object) value, 3);
            } else {
                String returnV = String.format("%.3f", value);
                return returnV;
            }
            //}
        }
        return value.toString();

    }

    public static Item copyItem(Item x) {
        Item y = new Item(x.openItem()); //For a string object, will the new item have a field pointing to a field on x????
        return y;
    }

    public void convertFromSpecialType(DataTypes type) {
        if (getType() == DataTypes.NOVALUEType || getType() == DataTypes.NaNType) {
            typ = type;
        }
    }

   /* public static BigDecimal round(float d, int decimalPlace) {
        BigDecimal bd = new BigDecimal(Float.toString(d));
        bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP);
        return bd;
    }*/
}