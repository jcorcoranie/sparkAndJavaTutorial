package com.virtualpairprogrammers;

public class IntegerWithSquareRoot{
    private int originalNumber;
    private double squareRoot;

    public IntegerWithSquareRoot(Integer value) {

        this.originalNumber = value;
        this.squareRoot = Math.sqrt(originalNumber);
    }

    public int getOriginalNumber() {
        return originalNumber;
    }

    public double getSquareRoot() {
        return squareRoot;
    }
}
