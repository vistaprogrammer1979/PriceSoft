/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.cachedRepo;

import com.accumed.pricing.model.Status;
import com.accumed.pricing.model.CusContract;
import com.accumed.pricing.model.CusPriceListItem;
import com.accumed.pricing.model.MasterPriceList;
import com.accumed.pricing.model.MasterPriceListItem;
import com.accumed.pricing.model.PackageItemCode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 *
 * @author smutlak
 */
public class CachedRepository {

    private ConcurrentHashMap<String, CachedData> cachedDB;

    public CachedRepository() {
         //this.cachedDB = new ConcurrentHashMap<>();
    }

    public Date getTimeStamp() {
        Date ret = null;
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();
            if (ret == null) {
                ret = cachedData.getChecksumTime();
            } else if (ret.before(cachedData.getChecksumTime())) {
                ret = cachedData.getChecksumTime();
            }
        }
        return ret;
    }

    public void addCachedData(String logicalName, CachedData cachedData) {
        if (cachedDB == null) {
            cachedDB = new ConcurrentHashMap();
        }
        cachedDB.put(logicalName, cachedData);
//cachedDB.put(logicalName, cachedData);
    }

    public void addCachedData(ConcurrentHashMap<String, CachedData> cachedMap) {
        if (cachedDB == null) {
            cachedDB = new ConcurrentHashMap();
        }
        for (Map.Entry<String, CachedData> entry : cachedMap.entrySet()) {
            String key = entry.getKey();
            CachedData value = entry.getValue();
            cachedDB.put(key, value);
        }
//cachedDB.putAll(cachedMap);

    }

    public final HashMap<String, Long> getDistinctTableList() {
        HashMap<String, Long> ret;
        ret = new HashMap();
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();

            for (Map.Entry<String, Long> entry2
                    : ((HashMap<String, Long>) cachedData.getTables()).entrySet()) {
                if (!ret.containsKey(entry2.getKey())) {
                    ret.put(entry2.getKey(), entry2.getValue());
                }
            }
        }
        return ret;
    }

    public final List<String> getLoadedCustomeContracts() {
        List<String> ret;
        ret = new ArrayList();
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();
            String logicalName = cachedData.getLogicalName();
            if (logicalName.startsWith("PL_CUS_CON")) {
                String arr[] = logicalName.split(Pattern.quote("|"));
                ret.add(arr[1] + ", " + arr[2]);

            }

        }
        return ret;
    }

    private void InvalidateCachedData(List<String> changedTables) {
        for (String changedTableName : changedTables) {
            for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
                CachedData cachedData = entry.getValue();
                if (cachedData.getTables().get(changedTableName) != null) {
                    cachedData.invalidCache();
                }
            }
        }
    }

    public Boolean isValid() {
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();
            if (Status.INVALID == cachedData.getStatus()) {
                return false;
            }
        }
        return true;
    }

    public int checkSynchronization(Connection conn) {

        HashMap<String, Long> allTablesList = getDistinctTableList();
        List<String> changedTables = new ArrayList();

        boolean ret = false;
        Statement stmt = null;
        ResultSet rs = null;
        if (allTablesList.size() <= 0) {
            return 0;
        }
        String sQuery = "";
        Set<String> tableNames = allTablesList.keySet();

        for (String t : tableNames) {
            //   sQuery += "SELECT '" + t + "', CHECKSUM_AGG(BINARY_CHECKSUM(*)) FROM dbo." + t + " WITH (NOLOCK) union ";
            sQuery += "SELECT '" + t + "', count(1)  FROM dbo." + t + "_TAUD  WITH (NOLOCK)  where  DATEDIFF(day,getdate(), AUD_DATE)= 0 union ";
        }
        //remove last union
        sQuery = sQuery.substring(0, sQuery.length() - 7);

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    Long newchecksum = rs.getLong(2);

                    Long currentChecksum = allTablesList.get(tableName);

                    if (currentChecksum != null
                            && currentChecksum != newchecksum.longValue()) {
                        changedTables.add(tableName);
                        Logger.getLogger(CachedRepository.class.getName()).log(Level.INFO, "Mark {0} as changed.", tableName);
                        ret = true;
                    }

                }
                rs.close();
                rs = null;
                stmt.close();
                stmt = null;
            }

            InvalidateCachedData(changedTables);
            if (!changedTables.isEmpty()) {
                return changedTables.size();
            }

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }


    public ConcurrentHashMap<String, CachedData> getCachedDB() {
        return cachedDB;
    }

    public boolean isCached(String logicalName) {
        return null != cachedDB.get(logicalName);
    }

    public CachedData get(String logicalName) {
        return cachedDB.get(logicalName);
    }

    public int getInvalidCachedDataCount() {
        int rett = 0;
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();
            if (Status.INVALID == cachedData.getStatus()) {
                rett++;
            }
        }
        return rett;
    }


    public int checkSynchronizationJob(Connection dbConn) {
        int returnValue = 0;
        returnValue += checkSynchronizationPriceListJob(dbConn);
        returnValue += checkSynchronizationContractJob(dbConn);
        returnValue += checkSynchronizationPriceListItemJob(dbConn);
        returnValue += checkSynchronizationSPCPriceListItemJob(dbConn);
        returnValue += checkSynchronizationSPCPriceListJob(dbConn);
        returnValue += checkSynchronizationSPCGroupFactorJob(dbConn);
        returnValue += checkSynchronizationSPCCodeFactorJob(dbConn);
        returnValue += checkSynchronizationRCMFacilityCodesMappingJob(dbConn);
        returnValue += checkSynchronizationRCMPackageCodeJob(dbConn);
        returnValue += checkSynchronizationRCMPackageItemJob(dbConn);
        returnValue += checkSynchronizationRCMCustomCodeJob(dbConn);
        return returnValue;
    }

    public int checkSynchronization(Connection conn, String receiverLicense, String facilityLicense, String packageName) {

        HashMap<Long, Long> allTablesList = getDistinctContractTableList();
        List<Long> changedTables = new ArrayList();
        List<Long> ids = new ArrayList();
        List<Long> prieListids = new ArrayList();
        if (packageName != null) {
            ids = fetchingContractIds(conn, receiverLicense, facilityLicense, packageName);
            prieListids = fetchingContractPriceListIdsIds(conn, receiverLicense, facilityLicense, packageName);
        } else {
            ids = fetchingContractIds(conn, receiverLicense, facilityLicense);
            prieListids = fetchingContractPriceListIdsIds(conn, receiverLicense, facilityLicense);
        }
        if (ids == null || ids.isEmpty()) {
            return 0;
        }

        int returnValue = 0;
        for (Long prieListid : prieListids) {
            int changed = checkSynchronization(conn, prieListid);
            if (changed > 0) {
                updateCheckSumPriceList(conn, prieListid);
            }
            returnValue += returnValue;
        }
        boolean ret = false;
        Statement stmt = null;
        ResultSet rs = null;

        String sQuery = "";
        for (int i = 0; i < ids.size(); i++) {
            sQuery += "SELECT  '" + ids.get(i) + "' ,  count(1)  FROM dbo.PL_CUS_Contract_TAUD WITH (NOLOCK)   WHERE id=" + ids.get(i);
            if (i < (ids.size() - 1)) {
                sQuery += " union  ";
            }
        }
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    Long newchecksum = rs.getLong(2);
                    Long id = Long.parseLong(tableName);
                    Long currentChecksum = allTablesList.get(id);
                    if (currentChecksum != null
                            && currentChecksum != newchecksum.longValue()) {
                        changedTables.add(id);
                        Logger.getLogger(CachedRepository.class.getName()).log(Level.INFO, "Mark {0} as changed.", tableName);
                        ret = true;
                    } else if (currentChecksum == null) {
                        addNewContract(conn, receiverLicense, facilityLicense, id);
                        returnValue++;
                    }

                }
                rs.close();
                rs = null;
                stmt.close();
                stmt = null;
            }

            InvalidateCachedContractData(changedTables);

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        returnValue += changedTables.size();
        return returnValue;
    }

    public void updateCheckSumPriceList(Connection conn, long priceListId) {
        String sLogicalName2 = "PL_CUS_PL|" + priceListId;

        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();
            if (cachedData.getLogicalName().startsWith(sLogicalName2)) {
                cachedData.getTables().put("PL_NEW_CUS_PriceListItem", RepoUtils.getTableChecksum(conn, sLogicalName2));
            }
        }

    }

    public List<Long> fetchingContractPriceListIdsIds(Connection conn, String receiverLicense, String facilityLicense, String packageName) {
        List<Long> changedTables = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        String sQuery = "";
        sQuery += "SELECT top 1  contract.priceListId  FROM dbo.PL_CUS_Contract contract WITH (NOLOCK)   WHERE  deleted=0 and  facility_license='" + facilityLicense + "' and insurer_license='" + receiverLicense + "' and package_name='" + packageName + "'";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    Long contrctId = rs.getLong(1);
                    changedTables.add(contrctId);
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return changedTables;
    }

    public int checkSynchronizationPriceListJob(Connection conn) {

        List<Long> changedTables = new ArrayList();
        List<Long> ids = new ArrayList();
        List<Long> prieListids = new ArrayList();
        long maxAuditId = getTableMaxAuditID("PRICE_LIST_CHECK_SUM");
        cachedDB.get("PRICE_LIST_CHECK_SUM").setMaxAuditId(getTableChecksum(conn, "PL_CUS_PriceList"));
        ids = fetchingPriceListIds(conn, maxAuditId);
        if (ids == null || ids.isEmpty()) {
            return 0;
        }

        int returnValue = 0;
        for (Long id : ids) {
            updatePriceListJob(conn, id);
            returnValue++;
        }

        returnValue += changedTables.size();
        return returnValue;
    }

    public int checkSynchronizationSPCPriceListJob(Connection conn) {
        return checkSynchronizationGeneralJob(conn, "SPC_PRICE_LIST_CHECK_SUM", "PL_SPC_PriceList", "spcContracts","id");

    }

    public int checkSynchronizationSPCGroupFactorJob(Connection conn) {
        return checkSynchronizationGeneralJob(conn, "SPC_GROUP_FACTOR_CHECK_SUM", "PL_SPC_Group_FACTOR", "spcGroupFactors","id");

    }

    public int checkSynchronizationSPCCodeFactorJob(Connection conn) {
        return checkSynchronizationGeneralJob(conn, "SPC_CODE_FACTOR_CHECK_SUM", "PL_SPC_CODES_FACTOR", "spcCodeFactors","id");

    }
     public int checkSynchronizationRCMFacilityCodesMappingJob(Connection conn) {
        return checkSynchronizationGeneralJob(conn, "RCM_FACILITY_CODE_MAPPING_CHECK_SUM", "RCM_FACILITY_CODES_MAPPING", "facilityCodesMapping","id");

    }
      public int checkSynchronizationRCMPackageCodeJob(Connection conn) {
        return checkSynchronizationGeneralJob(conn, "RCM_PACKAGE_CODE_CHECK_SUM", "RCM_Package_Group", "RCM_Package_Group","PackageGroupID");

    }
      public int checkSynchronizationRCMPackageItemJob(Connection conn) {
        return checkSynchronizationGeneralJob(conn, "RCM_PACKAGE_ITEM_CHECK_SUM", "RCM_Package_Item", "RCM_Package_Item","PackageItemID");

    }
      public int checkSynchronizationRCMCustomCodeJob(Connection conn) {
        return checkSynchronizationGeneralJob(conn, "RCM_CUSTOM_CODE_CHECK_SUM", "RCM_Custom_Codes", "RCM_Custom_Codes","CustomCodeID");

    }
      

   
    public int checkSynchronizationGeneralJob(Connection conn, String cachingKey, String tableName, String cachedObjectName,String idColumnName) {
        List<Long> changedTables = new ArrayList();
        List<Long> ids = new ArrayList();
        long maxAuditId = getTableMaxAuditID(cachingKey);
        cachedDB.get(cachingKey).setMaxAuditId(getTableChecksum(conn, tableName));
        ids = fetchingGeneralIds(conn, maxAuditId, tableName,idColumnName);
        int returnValue = 0;
        if (ids == null || ids.isEmpty()) {
            return 0;
        } else {
            switch (cachingKey) {
                case "SPC_PRICE_LIST_CHECK_SUM":
                    cachedDB.put(cachedObjectName, RepoUtils.getSPCContracts(conn, cachedObjectName));
                    break;
                case "SPC_GROUP_FACTOR_CHECK_SUM":
                    cachedDB.put(cachedObjectName, RepoUtils.getSPCGroupFactors(conn, cachedObjectName));
                    break;
                case "SPC_CODE_FACTOR_CHECK_SUM":
                    cachedDB.put(cachedObjectName, RepoUtils.getSPCCodeFactors(conn, cachedObjectName));
                    break;
                case "RCM_FACILITY_CODE_MAPPING_CHECK_SUM":
                    cachedDB.put(cachedObjectName, RepoUtils.getRCMFacilityCodesMapping(conn, cachedObjectName));
                    break;
                  case "RCM_PACKAGE_CODE_CHECK_SUM":
                    cachedDB.put(cachedObjectName, RepoUtils.getAllPackageCodes(conn, cachedObjectName));
                    break;
                    case "RCM_CUSTOM_CODE_CHECK_SUM":
                    cachedDB.put(cachedObjectName, RepoUtils.getAllCustomeCodes(conn, cachedObjectName));
                    break;
                    case "RCM_PACKAGE_ITEM_CHECK_SUM":
                    cachedDB.put(cachedObjectName, RepoUtils.getAllPackageItemCodes(conn, cachedObjectName));
                    break;
                default:
                    // Handle unexpected keys or log a warning
                    System.out.println("Unrecognized caching key: " + cachingKey);
            }
            returnValue++;
        }
        returnValue += changedTables.size();
        return returnValue;
    }

    public int checkSynchronizationPriceListItemJob(Connection conn) {
        int returnValue = 0;
        List<Long> changedTables = new ArrayList();
        List<Long> ids = new ArrayList();
        List<Long> prieListids = new ArrayList();
        HashMap<Long, Long> allTablesList = getDistinctContractPriceListItemsTableList();
        String sLogicalName2 = "PL_CUS_PL|" + "";
        String table_check_Sum = "PRICE_LIST_ITEM_CHECK_SUM";
        long maxAuditId = getTableMaxAuditID(table_check_Sum);

        cachedDB.get(table_check_Sum).setMaxAuditId(getTableChecksum(conn, "PL_NEW_CUS_PriceListItem"));

        ids = fetchingTableIds(conn, "PL_NEW_CUS_PriceListItem_TAUD", maxAuditId);

        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        try {
            for (Long id : ids) {
                updatePriceListItemJob(conn, id);
                returnValue++;

            }
            InvalidateCachedPriceListItemData(changedTables);
        } catch (Exception ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
        }
        returnValue += changedTables.size();
        return returnValue;
    }

    public int checkSynchronizationTableJob(Connection conn, String sLogicalName, String tableCheckSum) {
        int returnValue = 0;
        List<Long> changedTables = new ArrayList();
        List<Long> ids = new ArrayList();
        List<Long> prieListids = new ArrayList();
        HashMap<Long, Long> allTablesList = getDistinctContractPriceListItemsTableList();
        String sLogicalName2 = "" + sLogicalName + "|" + "";
        String table_check_Sum = tableCheckSum;
        long maxAuditId = getTableMaxAuditID(table_check_Sum);

        cachedDB.get(table_check_Sum).setMaxAuditId(getTableChecksum(conn, "PL_NEW_CUS_PriceListItem"));

        ids = fetchingTableIds(conn, "PL_NEW_CUS_PriceListItem_TAUD", maxAuditId);

        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        try {
            for (Long id : ids) {
                updatePriceListItemJob(conn, id);
                returnValue++;

            }
            InvalidateCachedPriceListItemData(changedTables);
        } catch (Exception ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
        }
        returnValue += changedTables.size();
        return returnValue;
    }

    public int checkSynchronizationSPCPriceListItemJob(Connection conn) {
        int returnValue = 0;
        List<Long> changedTables = new ArrayList();
        List<Long> ids = new ArrayList();
        //  List<Long> prieListids = new ArrayList();
        // HashMap<Long, Long> allTablesList = getDistinctMapTableList("PL_SPC_MasterPriceListItem");
        //   String sLogicalName2 = "PL_SPC_PriceList|" + "";
        String tableCheckSum = "SPC_PRICE_LIST_ITEM_CHECK_SUM";
        long maxAuditId = getTableMaxAuditID(tableCheckSum);

        cachedDB.get(tableCheckSum).setMaxAuditId(getTableChecksum(conn, "PL_SPC_MasterPriceListItem"));

        ids = fetchingTableIds(conn, "PL_SPC_MasterPriceListItem_TAUD", maxAuditId);

        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        try {
            for (Long id : ids) {
                updateSPCPriceListItemJob(conn, id);
                returnValue++;

            }
            InvalidateCachedSPCPriceListItemData(changedTables);
        } catch (Exception ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
        }
        returnValue += changedTables.size();
        return returnValue;
    }

    public int checkSynchronizationRCM_Package_ItemJob(Connection conn) {
        int returnValue = 0;
        List<Long> changedTables = new ArrayList();
        List<Long> ids = new ArrayList();
        String tableCheckSum = "RCM_PACKAGE_ITEM_CHECK_SUM";
        long maxAuditId = getTableMaxAuditID(tableCheckSum);

        cachedDB.get(tableCheckSum).setMaxAuditId(getTableChecksum(conn, "RCM_Package_Item"));

        ids = fetchingTableIds(conn, "RCM_Package_Item_TAUD", maxAuditId);

        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        try {
            for (Long id : ids) {
                updateRCMPackageItemJob(conn, id);
                returnValue++;

            }
            // InvalidateCachedRCMPackageItemData(changedTables);
        } catch (Exception ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
        }
        returnValue += changedTables.size();
        return returnValue;
    }

    public int checkSynchronizationContractJob(Connection conn) {

        HashMap<Long, Long> allTablesList = getDistinctContractTableList();
        List<Long> changedTables = new ArrayList();
        List<Long> ids = new ArrayList();
        List<Long> prieListids = new ArrayList();
        long maxAuditId = getTableMaxAuditID("CONTARCT_CHECK_SUM");
        cachedDB.get("CONTARCT_CHECK_SUM").setMaxAuditId(getTableChecksum(conn, "PL_CUS_Contract"));
        ids = fetchingContractIds(conn, maxAuditId);
        if (ids == null || ids.isEmpty()) {
            return 0;
        }

        int returnValue = 0;
        for (Long id : ids) {
            updateContractJob(conn, id);
            returnValue++;
        }

        InvalidateCachedContractData(changedTables);
        returnValue += changedTables.size();
        return returnValue;
    }

    public List<Long> fetchingPriceListIds(Connection conn, long maxAuditId) {
        List<Long> changedTables = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        String sQuery = "";
        sQuery += "SELECT  id  FROM dbo.PL_CUS_PriceList_taud WITH (NOLOCK)  where  AUD_ID>" + maxAuditId + " ";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    Long contrctId = rs.getLong(1);
                    changedTables.add(contrctId);
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return changedTables;
    }


    public List<Long> fetchingIds(Connection conn, long maxAuditId, String tableName) {//PL_SPC_PriceList
        List<Long> changedTables = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        String sQuery = "";
        sQuery += "SELECT  id  FROM dbo." + tableName + "_taud WITH (NOLOCK)  where  AUD_ID>" + maxAuditId + " ";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    Long id = rs.getLong(1);
                    changedTables.add(id);
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return changedTables;
    }
public List<Long> fetchingGeneralIds(Connection conn, long maxAuditId, String tableName,String idColumnName) {//PL_SPC_PriceList
        List<Long> changedTables = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        String sQuery = "";
        sQuery += "SELECT " +idColumnName+"  FROM dbo." + tableName + "_taud WITH (NOLOCK)  where  AUD_ID>" + maxAuditId + " ";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    Long id = rs.getLong(1);
                    changedTables.add(id);
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return changedTables;
    }
    public static Long getTableChecksum(Connection db, String sTableName) {

        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = db.createStatement();
            rs = stmt.executeQuery("SELECT  max(AUD_ID)  FROM dbo." + sTableName + "_TAUD  WITH (NOLOCK)  ");
            if (rs != null) {
                if (rs.next()) {
                    Long checksum = rs.getLong(1);
                    Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, "Set Table ({0}) Taud table checking.", sTableName);
                    return checksum;
                }

            }
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return null;
    }


    public long getTableMaxAuditID(String table_CHECK_SUM) {
        if (cachedDB.get(table_CHECK_SUM) != null && cachedDB.get(table_CHECK_SUM).getMaxAuditId() != null) {
            return cachedDB.get(table_CHECK_SUM).getMaxAuditId();
        }
        return 0;

    }

    public void updatePriceListJob(Connection con, long priceListId) {
        String sLogicalName = "PL_CUS_PL|" + priceListId;
        if (cachedDB.get(sLogicalName) == null) {
            cachedDB.put(sLogicalName, RepoUtils.getCustomPriceListsById(con, priceListId));
        }

    }

    public List<Long> fetchingTableIds(Connection conn, String table, long maxAuditId) {
        List<Long> changedTables = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        String sQuery = "";
        sQuery += "SELECT  id  FROM " + table + " WITH (NOLOCK)  where  AUD_ID>" + maxAuditId + " ";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    Long contrctId = rs.getLong(1);
                    changedTables.add(contrctId);
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return changedTables;
    }

    public void updatePriceListItemJob(Connection con, long pricelistItemId) {

        CusPriceListItem item = RepoUtils.getCustomPricelistItemsById("", con, pricelistItemId);

        String sLogicalName = "PL_CUS_PL|" + item.getPricListId();
        CachedData cachedData = null;
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            if (entry.getValue().getLogicalName().startsWith(sLogicalName)) {
                cachedData = entry.getValue();
            }

        }
        java.util.HashMap<String, Long> tables = new java.util.HashMap<String, Long>();
        if (cachedData == null) {
            cachedData = cachedDB.put(sLogicalName, new CachedData(sLogicalName, tables, new java.util.HashMap<Long, Long>(), new java.util.ArrayList()));
            for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
                if (entry.getValue().getLogicalName().startsWith(sLogicalName)) {
                    cachedData = entry.getValue();
                }
            }
        }
        List<CusPriceListItem> con2Removed = new ArrayList<CusPriceListItem>();
        if (cachedData.getDataMap().get(item.getCode()) != null) {
            for (Object object : cachedData.getData()) {
                if (((CusPriceListItem) object).getCode().equals(item.getCode())) {
                    con2Removed.add((CusPriceListItem) object);
                }
            }
            cachedData.getData().removeAll(con2Removed);
        }
        if (item.getDeleted() == null || !item.getDeleted()) {
            cachedData.getData().add(item);
            cachedData.getDataMap().put(item.getCode(), item);
        }

    }

    public void updateSPCPriceListItemJob(Connection con, long pricelistItemId) {

        MasterPriceListItem item = RepoUtils.getSPCPricelistItemsById("PL_SPC_MasterPriceListItem", con, pricelistItemId);

        String sLogicalName = "PL_SPC_MasterPriceListItem|" + item.getId();
        CachedData cachedData = null;
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            if (entry.getValue().getLogicalName().startsWith(sLogicalName)) {
                cachedData = entry.getValue();
            }

        }
        java.util.HashMap<String, Long> tables = new java.util.HashMap<String, Long>();
        if (cachedData == null) {
            cachedData = cachedDB.put(sLogicalName, new CachedData(sLogicalName, tables, new java.util.HashMap<Long, Long>(), new java.util.ArrayList()));
            for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
                if (entry.getValue().getLogicalName().startsWith(sLogicalName)) {
                    cachedData = entry.getValue();
                }
            }
        }
        List<MasterPriceListItem> con2Removed = new ArrayList<MasterPriceListItem>();
        if (cachedData.getDataMap() == null) {
            cachedData.setDataMap(new HashMap<String, String>());
        }
        if (cachedData.getDataMap().get(item.getCode()) != null) {
            for (Object object : cachedData.getData()) {
                if (((MasterPriceListItem) object).getCode().equals(item.getCode())) {
                    con2Removed.add((MasterPriceListItem) object);
                }
            }
            cachedData.getData().removeAll(con2Removed);
        }
        if (item.getDeleted() == null || !item.getDeleted()) {
            cachedData.getData().add(item);
            cachedData.getDataMap().put(item.getCode(), item);
        }

    }

    private void InvalidateCachedPriceListItemData(List<Long> changedTables) {
        for (Long changedTableName : changedTables) {
            for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
                CachedData cachedData = entry.getValue();
                if (cachedData.getLogicalName().startsWith("PL_CUS_CON")) {
                    for (Object obj : cachedData.getData()) {
                        CusContract contract = (CusContract) obj;
                        if (contract.getID().equals(changedTableName)) {
                            ((CusContract) obj).setStatus(com.accumed.pricing.model.Status.INVALID);
                        }
                    }
                }
                if (cachedData.getLogicalName().startsWith("PL_CUS_PL")) {
                    for (Object obj : cachedData.getData()) {
                        CusPriceListItem item = (CusPriceListItem) obj;
                        if (item.getId().equals(changedTableName)) {
                            ((CusPriceListItem) obj).setStatus(com.accumed.pricing.model.Status.INVALID);
                        }
                    }
                }
            }
        }
    }

    private void InvalidateCachedSPCPriceListItemData(List<Long> changedTables) {
        for (Long changedTableName : changedTables) {
            for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
                CachedData cachedData = entry.getValue();
                if (cachedData.getLogicalName().startsWith("PL_SPC_MasterPriceListItem")) {
                    for (Object obj : cachedData.getData()) {
                        MasterPriceList contract = (MasterPriceList) obj;
                        if (contract.getId().equals(changedTableName)) {
                            ((MasterPriceList) obj).setStatus(com.accumed.pricing.model.Status.INVALID);
                        }
                    }
                }
                if (cachedData.getLogicalName().startsWith("PL_SPC_MasterPriceListItem")) {
                    for (Object obj : cachedData.getData()) {
                        MasterPriceListItem item = (MasterPriceListItem) obj;
                        if (item.getId().equals(changedTableName)) {
                            ((MasterPriceListItem) obj).setStatus(com.accumed.pricing.model.Status.INVALID);
                        }
                    }
                }
            }
        }
    }


    public final HashMap<Long, Long> getDistinctContractPriceListItemsTableList() {
        HashMap<Long, Long> ret;
        ret = new HashMap();
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();
            if (cachedData.getLogicalName().startsWith("PL_CUS_PL")) {
                for (Map.Entry<Long, Long> entry2
                        : ((HashMap<Long, Long>) cachedData.getTablesIds()).entrySet()) {
                    if (!ret.containsKey(entry2.getKey())) {
                        ret.put(entry2.getKey(), entry2.getValue());
                    }
                }
            }
        }

        return ret;
    }

    public final HashMap<Long, Long> getDistinctMapTableList(String logicalName) {
        HashMap<Long, Long> ret;
        ret = new HashMap();
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();
            if (cachedData.getLogicalName().startsWith(logicalName)) {
                for (Map.Entry<Long, Long> entry2
                        : ((HashMap<Long, Long>) cachedData.getTablesIds()).entrySet()) {
                    if (!ret.containsKey(entry2.getKey())) {
                        ret.put(entry2.getKey(), entry2.getValue());
                    }
                }
            }
        }

        return ret;
    }

    public final HashMap<Long, Long> getDistinctContractTableList() {
        HashMap<Long, Long> ret;
        ret = new HashMap();
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();
            if (cachedData.getLogicalName().startsWith("PL_CUS_CON")) {
                for (Map.Entry<Long, Long> entry2
                        : ((HashMap<Long, Long>) cachedData.getTablesIds()).entrySet()) {
                    if (!ret.containsKey(entry2.getKey())) {
                        ret.put(entry2.getKey(), entry2.getValue());
                    }
                }
            }
        }

        return ret;
    }

    public List<Long> fetchingContractIds(Connection conn, String receiverLicense, String facilityLicense, String packageName) {
        List<Long> changedTables = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        String sQuery = "";
        sQuery += "SELECT top 1  contract.id  FROM dbo.PL_CUS_Contract contract WITH (NOLOCK)   WHERE deleted=0 and  facility_license='" + facilityLicense + "' and insurer_license='" + receiverLicense + "' and package_name='" + packageName + "'";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    Long contrctId = rs.getLong(1);
                    changedTables.add(contrctId);
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return changedTables;
    }

    public List<Long> fetchingContractIds(Connection conn, String receiverLicense, String facilityLicense) {
        List<Long> changedTables = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        String sQuery = "";
        sQuery += "SELECT top 1  contract.id  FROM dbo.PL_CUS_Contract contract WITH (NOLOCK)   WHERE deleted=0 and  facility_license='" + facilityLicense + "' and insurer_license='" + receiverLicense + "' ";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    Long contrctId = rs.getLong(1);
                    changedTables.add(contrctId);
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return changedTables;
    }

    public List<Long> fetchingContractIds(Connection conn, long maxAuditId) {
        List<Long> changedTables = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        String sQuery = "";
        sQuery += "SELECT  id  FROM dbo.PL_CUS_Contract_TAUD WITH (NOLOCK)  where  AUD_ID>" + maxAuditId + " ";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    Long contrctId = rs.getLong(1);
                    changedTables.add(contrctId);
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return changedTables;
    }

    public void updateContractJob(Connection con, long contractId) {
        CusContract contract = RepoUtils.getCustomContractById(con, "", contractId);
       
        String sLogicalName = "PL_CUS_CON|" + contract.getInsurerLicense() + "|" + contract.getFacilityLicense();

        CachedData cachedData = null;
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            if (entry.getValue().getLogicalName().startsWith(sLogicalName)) {
                cachedData = entry.getValue();
            }
        }
        java.util.HashMap<String, Long> tables = new java.util.HashMap<String, Long>();
        if (cachedData == null) {
            cachedData = cachedDB.put(sLogicalName, new CachedData(sLogicalName, tables, new java.util.HashMap<Long, Long>(), new java.util.ArrayList()));
            for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
                if (entry.getValue().getLogicalName().startsWith(sLogicalName)) {
                    cachedData = entry.getValue();
                }
            }
        }
        List<CusContract> con2Removed = new ArrayList<CusContract>();
        if (cachedData != null) {
            for (Object object : cachedData.getData()) {
                if (((CusContract) object).getID().equals(contract.getID())) {
                    con2Removed.add((CusContract) object);
                    break;
                }
            }
            cachedData.getData().removeAll(con2Removed);
        }
        if (contract.getDeleted() == null || !contract.getDeleted()) {
            cachedData.getData().add(contract);
        }

    }

    private void InvalidateCachedContractData(List<Long> changedTables) {
        for (Long changedTableName : changedTables) {
            for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
                CachedData cachedData = entry.getValue();
                if (cachedData.getLogicalName().startsWith("PL_CUS_CON")) {
                    for (Object obj : cachedData.getData()) {
                        CusContract contract = (CusContract) obj;
                        if (contract.getID().equals(changedTableName)) {
                            ((CusContract) obj).setStatus(com.accumed.pricing.model.Status.INVALID);
                        }
                    }
                }
                if (cachedData.getLogicalName().startsWith("PL_CUS_PL")) {
                    for (Object obj : cachedData.getData()) {
                        CusPriceListItem item = (CusPriceListItem) obj;
                        if (item.getId().equals(changedTableName)) {
                            ((CusPriceListItem) obj).setStatus(com.accumed.pricing.model.Status.INVALID);
                        }
                    }
                }
            }
        }
    }

    public List<Long> fetchingContractPriceListIdsIds(Connection conn, String receiverLicense, String facilityLicense) {
        List<Long> changedTables = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        String sQuery = "";
        sQuery += "SELECT top 1  contract.priceListId  FROM dbo.PL_CUS_Contract contract WITH (NOLOCK)   WHERE  deleted=0 and  facility_license='" + facilityLicense + "' and insurer_license='" + receiverLicense + "' ";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sQuery);
            if (rs != null) {
                while (rs.next()) {
                    Long contrctId = rs.getLong(1);
                    changedTables.add(contrctId);
                }
            }
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;

        } catch (SQLException ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException ex) {
                //Statistics.addException(ex);
                Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return changedTables;
    }

    public void addNewContract(Connection conn, String receiverLicense, String facilityLicense, long contractId) {
        String sLogicalName = "PL_CUS_CON|" + receiverLicense + "|" + facilityLicense;
        CusContract con = RepoUtils.getCustomContractById(conn, sLogicalName, contractId);
        con.setUpdatedStatus(com.accumed.pricing.model.Status.NEW);
        // con.setOriginalContract(con);
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            CachedData cachedData = entry.getValue();
            if (cachedData.getLogicalName().startsWith(sLogicalName)) {
                cachedData.getData().add(con);
            }

        }
    }

    public int checkSynchronization(Connection conn, long priceListId) {

        HashMap<Long, Long> allTablesList = getDistinctContractPriceListItemsTableList();
        String sLogicalName2 = "PL_CUS_PL|" + priceListId;
        long maxAuditId = getMaxPricelistItemAuditID(sLogicalName2);
        List<Long> changedTables = new ArrayList();
        List<Long> ids = new ArrayList();
//        ids = fetchingPriceListItemsIds(conn, priceListId, maxAuditId);
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        try {
            for (Long id : ids) {

                Long currentChecksum = allTablesList.get(id);
                if (currentChecksum != null) {
                    changedTables.add(id);
                }
            }
            InvalidateCachedContractData(changedTables);
        } catch (Exception ex) {
            Logger.getLogger(CachedRepository.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
        }
        return changedTables.size();
    }

    public long getMaxPricelistItemAuditID(String logicName) {
        if (cachedDB.get("PRICE_LIST_ITEM_CHECK_SUM").getMaxAuditId() != null) {
            return cachedDB.get("PRICE_LIST_ITEM_CHECK_SUM").getMaxAuditId();
        }
        return 0;

    }

    private void updateRCMPackageItemJob(Connection conn, Long id) {

        PackageItemCode item = RepoUtils.getRCMPackageItemsById("RCM_Package_Item", conn, id);

        String sLogicalName = "RCM_Package_Item|" + item.getCode();
        CachedData cachedData = null;
        for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
            if (entry.getValue().getLogicalName().startsWith(sLogicalName)) {
                cachedData = entry.getValue();
            }

        }
        java.util.HashMap<String, Long> tables = new java.util.HashMap<String, Long>();
        if (cachedData == null) {
            cachedData = cachedDB.put(sLogicalName, new CachedData(sLogicalName, tables, new java.util.HashMap<Long, Long>(), new java.util.ArrayList()));
            for (Map.Entry<String, CachedData> entry : cachedDB.entrySet()) {
                if (entry.getValue().getLogicalName().startsWith(sLogicalName)) {
                    cachedData = entry.getValue();
                }
            }
        }
        List<PackageItemCode> con2Removed = new ArrayList<PackageItemCode>();
        if (cachedData.getDataMap() == null) {
            cachedData.setDataMap(new HashMap<String, String>());
        }
        if (cachedData.getDataMap().get(item.getCode()) != null) {
            for (Object object : cachedData.getData()) {
                if (((PackageItemCode) object).getCode().equals(item.getCode())) {
                    con2Removed.add((PackageItemCode) object);
                }
            }
            cachedData.getData().removeAll(con2Removed);
        }
        if (item.getDeleted() == null || !item.getDeleted()) {
            cachedData.getData().add(item);
            cachedData.getDataMap().put(item.getCode(), item);
        }

    }


}
