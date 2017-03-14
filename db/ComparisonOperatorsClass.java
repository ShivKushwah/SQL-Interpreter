package db;

import java.util.Comparator;

/**
 * Created by Shiv on 3/4/17.
 */
public class ComparisonOperatorsClass {

    public static boolean compareValid(Item a, Item b, ComparisonOperators operator) {
        //TODO: Add support for other operations

        if (operator != null && operator instanceof ComparisonOperators) {

            if (a.openItem().equals("NOVALUE") || b.openItem().equals("NOVALUE")) {
                return false;
            }

            if (a.openItem().equals("NaN")) {
                if (a.getType() == DataTypes.intType) {
                    a = new Item(Integer.MAX_VALUE);
                }
                if (a.getType() == DataTypes.stringType) {

                }
                if (a.getType() == DataTypes.floatType) {
                    a = new Item(Float.MAX_VALUE);
                }
            }
            if (b.openItem().equals("NaN")) {
                if (b.getType() == DataTypes.intType) {
                    b = new Item(Integer.MAX_VALUE);
                }
                if (b.getType() == DataTypes.stringType) {

                }
                if (b.getType() == DataTypes.floatType) {
                    b = new Item(Float.MAX_VALUE);
                }
            }
            if (a.openItem().equals("NaN") && b.openItem().equals("NaN")) {
                return compareNan(a, b, operator, 2);
            }
            if (a.openItem().equals("NaN")) {

                return compareNan(a, b, operator, 0);
            }
            if (b.openItem().equals("NaN")) {
                return compareNan(a, b, operator, 1);
            }


            if (a.getType() == DataTypes.floatType && b.getType() == DataTypes.intType && !a.openItem().equals("NOVALUE") && !a.openItem().equals("NaN") && !b.openItem().equals("NOVALUE") && !b.openItem().equals("NaN") ) {
                b = new Item( ((Integer) b.openItem() )* 1.0f);
            }
            if (a.getType() == DataTypes.intType && b.getType() == DataTypes.floatType && !a.openItem().equals("NOVALUE") && !a.openItem().equals("NaN") && !b.openItem().equals("NOVALUE") && !b.openItem().equals("NaN")) {
                a = new Item(((Integer) a.openItem()) * 1.0f);
            }



        }
        if (operator ==  ComparisonOperators.LessThan) {
            if ( ((Comparable<Object>) a.openItem()).compareTo(b.openItem()) < 0) {
                return true;
            }
            else {
                return false;
            }
        }
        else if (operator == ComparisonOperators.LessThanOrEqualTo) {
            if ( ((Comparable<Object>) a.openItem()).compareTo(b.openItem()) <= 0) {
                return true;
            }
            else {
                return false;
            }

        }
        else if (operator == ComparisonOperators.EqualTo) {
            if ( ((Comparable<Object>) a.openItem()).compareTo(b.openItem()) == 0) {
                return true;
            }
            else {
                return false;
            }

        }
        else if (operator == ComparisonOperators.DoesNotEqual) {
            if ( ((Comparable<Object>) a.openItem()).compareTo(b.openItem()) != 0) {
                return true;
            }
            else {
                return false;
            }

        }
        else if (operator == ComparisonOperators.GreaterThanOrEqualTo) {
            if ( ((Comparable<Object>) a.openItem()).compareTo(b.openItem()) >= 0) {
                return true;
            }
            else {
                return false;
            }

        }
        else if (operator == ComparisonOperators.GreaterThan) {
            if ( ((Comparable<Object>) a.openItem()).compareTo(b.openItem()) > 0) {
                return true;
            }
            else {
                return false;
            }

        }
        else {
            throw new RuntimeException();
        }

    }


    public static boolean compareNan(Item a, Item b, ComparisonOperators operator, int nanIndex) {
        if (nanIndex == 2) {
            if (operator == ComparisonOperators.EqualTo || operator == ComparisonOperators.GreaterThanOrEqualTo || operator == ComparisonOperators.LessThanOrEqualTo) {
                return true;
            }
            else {
                return false;
            }
        }
        else if (nanIndex == 1) { //b is nan string and greater than a
            if (operator == ComparisonOperators.LessThanOrEqualTo || operator == ComparisonOperators.LessThan) {
                return true;
            }
            else {
                return false;
            }
        }
        else { //a is nan string and greater than b;
            if (operator == ComparisonOperators.GreaterThanOrEqualTo || operator == ComparisonOperators.GreaterThan) {
                return true;
            }
            else {
                return false;
            }
        }

    }

}
