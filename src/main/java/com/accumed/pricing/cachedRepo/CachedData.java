/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.accumed.pricing.cachedRepo;

import com.accumed.pricing.model.Status;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author smutlak
 * 
 */
public class CachedData<T> {
    
    private String logicalName;
    private HashMap<String, Long> tables;
    private Date checksumTime;
    private Collection<T> data;// Lazy-loaded data
    private Status status;
    private HashMap<Long, Long> tablesIds; 
    private HashMap<String, T> dataMap;// Lazy-loaded data map
  private Long maxAuditId;


 

    public void setDataMap(HashMap<String, T> dataMap) {
        this.dataMap = dataMap;
    }
 
    public CachedData() {
    }

    public CachedData(String logicalName, HashMap<String, Long> tables, Collection<T> data) {
        this.logicalName = logicalName;
        this.tables = tables;
        this.checksumTime = new java.util.Date();
        this.data = data;
        status = Status.VALID;
        
    }
public CachedData(String logicalName, HashMap<String, Long> tables, Collection<T> data,HashMap<String, T> dataMap) {
        this.logicalName = logicalName;
        this.tables = tables;
        this.checksumTime = new java.util.Date();
        this.data = data;
        status = Status.VALID;
                this.dataMap =dataMap;

        
    }
   public CachedData(String logicalName, HashMap<String, Long> tables, HashMap<Long, Long> tablesIds,  Collection<T> data ) {
        this.logicalName = logicalName;
        this.tables = tables;
        this.tablesIds = tablesIds;
        this.checksumTime = new java.util.Date();
        this.data = data;
        status = Status.VALID;
    }
   public CachedData(String logicalName, HashMap<String, Long> tables, HashMap<Long, Long> tablesIds,  Collection<T> data,HashMap<String, T> dataMap ) {
        this.logicalName = logicalName;
        this.tables = tables;
        this.tablesIds = tablesIds;
        this.checksumTime = new java.util.Date();
        this.data = data;
        status = Status.VALID;
        this.dataMap =dataMap;
    
    }
   

 public HashMap<Long, Long> getTablesIds() {
        return tablesIds;
    }

    public void setTablesIds(HashMap<Long, Long> tablesIds) {
        this.tablesIds = tablesIds;
    }
    public String getLogicalName() {
        return logicalName;
    }

    public void setLogicalName(String logicalName) {
        this.logicalName = logicalName;
    }

    public HashMap<String, Long> getTables() {
        return tables;
    }

    public void setTables(HashMap<String, Long> tables) {
        this.tables = tables;
    }

    

    public Date getChecksumTime() {
        return checksumTime;
    }

    public void setChecksumTime(Date checksumTime) {
        this.checksumTime = checksumTime;
        status = Status.VALID;
    }

   public Collection<T> getData() {
      return data;
   }
   
       public HashMap<String, T> getDataMap() {
       return dataMap;
   }
       
    
       
    public void setData(Collection<T> data) {
        this.data = data;
    }
    
    public void invalidCache(){
        status = Status.INVALID;
        Logger.getLogger(CachedData.class.getName()).log(Level.INFO, "InvalidCache {0}.",this.logicalName);
    }
    public void validCache(){
        status = Status.VALID;
    }

    public Status getStatus() {
        return status;
    }

//    public void setStatus(Status status) {
//        this.status = status;
//    }

    public Long getMaxAuditId() {
        return maxAuditId;
    }

    public void setMaxAuditId(Long maxAuditId) {
        this.maxAuditId = maxAuditId;
    }
    
    
    public boolean isInvalid(){
        return Status.VALID != getStatus();
    }
    

    
}
