package com.virtualpairprogrammers;

import java.io.Serializable;

public class CalculatePoints implements Serializable {

    public CalculatePoints() {
    }

    protected int calculatePoint(Double percentComplete){

        if(percentComplete < 25){
            return 0;
        } else if(percentComplete < 50){
            return 2;
        } else if(percentComplete < 90){
            return 4;
        } else {
            return 10;
        }
    }
}
