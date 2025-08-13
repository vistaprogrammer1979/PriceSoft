/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

/**
 *
 * @author wfakhra
 */
public class CustomCode {

    private String code;
    private int type;
    private double price;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
   

    public CustomCode() {
    }

    public CustomCode(String code, int type, double price) {
        this.code = code;
        this.type = type;
        this.price = price;
    }

    
    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

   
    

}
