package com.datinko.prototype.bigdata.spark.core;

/**
 * Java Bean class to be used with the example NetworkWordStreamAnalyser.
 **/
public class JavaRecord implements java.io.Serializable {
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
