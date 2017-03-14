package db;

import java.util.IntSummaryStatistics;

/**
 * Created by Shiv on 3/3/17.
 */
public class OperationClass {

    public static Item operate(Item a, Item b, Operation op) {

        if (op == Operation.Plus || op == Operation.Minus || op == Operation.Multiply || op == Operation.Divide) {
            if (a.openItem().equals("NaN") || b.openItem().equals("NaN")) {
                return new Item("NaN");
            }

            if (a.openItem().equals("NOVALUE") && b.openItem().equals("NOVALUE")) {
                return new Item("NOVALUE");
            }
            if (a.openItem().equals("NOVALUE")) {
                if (a.getType() == DataTypes.intType) {
                    a = new Item(0);
                }
                if (a.getType() == DataTypes.stringType) {
                    a = new Item("\\");
                }
                if (a.getType() == DataTypes.floatType) {
                    a = new Item(0.0f);
                }
            }
            if (b.openItem().equals("NOVALUE")) {
                if (b.getType() == DataTypes.intType) {
                    b = new Item(0);
                }
                if (b.getType() == DataTypes.stringType) {
                    b = new Item("\\");
                }
                if (b.getType() == DataTypes.floatType) {
                    b = new Item(0.0f);
                }
            }

            if (a.getType() == DataTypes.floatType && b.getType() == DataTypes.intType && !a.openItem().equals("NOVALUE") && !a.openItem().equals("NaN") && !b.openItem().equals("NOVALUE") && !b.openItem().equals("NaN") ) {
                b = new Item( ((Integer) b.openItem() )* 1.0f);
            }
            if (a.getType() == DataTypes.intType && b.getType() == DataTypes.floatType && !a.openItem().equals("NOVALUE") && !a.openItem().equals("NaN") && !b.openItem().equals("NOVALUE") && !b.openItem().equals("NaN")) {
                a = new Item(((Integer) a.openItem()) * 1.0f);
            }


        }

        //TODO: Add support for floats
        if (op == Operation.Plus && a.getType() == DataTypes.intType && b.getType() == DataTypes.intType) {
            return new Item((Integer) a.openItem() + (Integer) b.openItem());

        } else if (op == Operation.Plus && a.getType() == DataTypes.stringType && b.getType() == DataTypes.stringType) {
            String s = (String) a.openItem();
            String z = (String) b.openItem();
            String fin = s.substring(0, s.length() - 1) + "" + z.substring(1);

            return new Item(fin);

        } else if (op == Operation.Minus && a.getType() == DataTypes.intType && b.getType() == DataTypes.intType) {
            return new Item((Integer) a.openItem() - (Integer) b.openItem());
        } else if (op == Operation.Multiply && a.getType() == DataTypes.intType && b.getType() == DataTypes.intType) {
            return new Item((Integer) a.openItem() * (Integer) b.openItem());
        } else if (op == Operation.Divide && a.getType() == DataTypes.intType && b.getType() == DataTypes.intType) {
            try {
                int x = (Integer) a.openItem() / (Integer) b.openItem();
                return new Item(x);
            } catch (ArithmeticException e) {
                return new Item("NaN");
            }
        } else if (op == Operation.Plus && a.getType() == DataTypes.floatType && b.getType() == DataTypes.floatType) {
            return new Item((Float) a.openItem() + (Float) b.openItem());
        } else if (op == Operation.Minus && a.getType() == DataTypes.floatType && b.getType() == DataTypes.floatType) {
            return new Item((Float) a.openItem() - (Float) b.openItem());
        } else if (op == Operation.Multiply && a.getType() == DataTypes.floatType && b.getType() == DataTypes.floatType) {
            return new Item((Float) a.openItem() * (Float) b.openItem());
        } else if (op == Operation.Divide && a.getType() == DataTypes.floatType && b.getType() == DataTypes.floatType) {
            try {
                float x = ((Float) a.openItem()) / ((Float) b.openItem());
                if (x == Float.POSITIVE_INFINITY ||  x == Float.NEGATIVE_INFINITY) {
                    throw new ArithmeticException();
                }
                return new Item(x);
            } catch (ArithmeticException e) {
                return new Item("NaN");
            }
        }


        return null;

    }
}
