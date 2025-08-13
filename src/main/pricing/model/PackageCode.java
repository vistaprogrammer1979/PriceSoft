/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

/**
 *
 * @author wfakhra
 */
public class PackageCode {

    private String code;
  //  private int type;
    private double price;
    private int PackageGroupID;
    private boolean isItemLevel;
    private Status  status;

    public PackageCode(String code, int type, double price, int PackageGroupID, boolean isItemLevel) {
        this.code = code;
      //  this.type = type;
        this.price = price;
        this.PackageGroupID = PackageGroupID;
        this.isItemLevel = isItemLevel;
    }

    public PackageCode(String code, double price, int PackageGroupID, boolean isItemLevel) {
        this.code = code;
        this.price = price;
        this.PackageGroupID = PackageGroupID;
        this.isItemLevel = isItemLevel;
    }

    

    public boolean isIsItemLevel() {
        return isItemLevel;
    }

    public void setIsItemLevel(boolean isItemLevel) {
        this.isItemLevel = isItemLevel;
    }
 

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
   

    public PackageCode() {
    }

    public int getPackageGroupID() {
        return PackageGroupID;
    }

    public void setPackageGroupID(int PackageGroupID) {
        this.PackageGroupID = PackageGroupID;
    }

   
    
    public String getCode() {
        return code;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setCode(String code) {
        this.code = code;
    }

   
    

}
