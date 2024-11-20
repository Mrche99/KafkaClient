package org.example;


import lombok.Getter;

@Getter
public class MyMessage {
    private String field1;
    private int field2;


    public MyMessage(String field1, int field2) {
        this.field1 = field1;
        this.field2 = field2;
    }
    @Override
    public String toString() {
        return "MyMessage{" +
                "field1='" + field1 + '\'' +
                ", field2='" + field2 + '\'' +
                '}';
    }

}
