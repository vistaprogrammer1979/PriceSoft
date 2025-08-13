/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.cachedRepo;

import com.accumed.pricing.PLContract;
import com.accumed.pricing.model.MasterPriceList;
import com.accumed.pricing.model.MasterPriceListItem;
import com.accumed.pricing.model.SPCContract;
import com.accumed.pricing.model.SPCGroupFactor;
import com.accumed.pricing.model.SPCCodeFactor;
import com.accumed.pricing.model.CusContract;
import com.accumed.pricing.model.CusPriceListItem;
import com.accumed.pricing.model.Clinician;
import com.accumed.pricing.model.CodeGroup;
import com.accumed.pricing.model.CustomCode;
import com.accumed.pricing.model.PackageCode;
import com.accumed.pricing.model.DHA_DRG;
import com.accumed.pricing.model.DHA_DRG_COST_PER_ACTIVITY;
import com.accumed.pricing.model.DHA_DRG_HighCost;
import com.accumed.pricing.model.DRG;
import com.accumed.pricing.model.DRGExcludedCpts;
import com.accumed.pricing.model.GroupCodesRange;
import com.accumed.pricing.model.request.CodeType;
import com.accumed.pricing.model.DrugPrice;
import com.accumed.pricing.model.Facility;
import com.accumed.pricing.model.FacilityType;
import com.accumed.pricing.model.PackageItemCode;
import com.accumed.pricing.model.RCMFacilityCodeMapping;
import com.accumed.pricing.model.Regulator;
import com.accumed.pricing.model.request.Activity;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.HashMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author smutlak
 */
public class RepoUtils {

    static Long getTableChecksum(Connection db, String sTableName) {

        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = db.createStatement();
            //rs = stmt.executeQuery("SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) FROM dbo." + sTableName + " WITH (NOLOCK)");
            rs = stmt.executeQuery(" SELECT  max(AUD_ID)  FROM dbo." + sTableName + "_TAUD  WITH (NOLOCK)  ");
            if (rs != null) {
                if (rs.next()) {
                    Long checksum = rs.getLong(1);
                    Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, "Set Table ({0}) check TAud  .", sTableName);
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
    public static Long getTableChecksumById(Connection db, String sTableName, long Id) {

        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = db.createStatement();
            rs = stmt.executeQuery("SELECT count(1)  FROM dbo." + sTableName + " WITH  (NOLOCK)   WHERE id = " + Id);
            if (rs != null) {
                if (rs.next()) {
                    Long checksum = rs.getLong(1);
                    Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, "Set Table ({0}) check  TAud .", sTableName);
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

    public static CachedData getSPCContracts(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<SPCContract> list = new ArrayList<>();
        try {
            tables.put("PL_SPC_PriceList", getTableChecksum(db, "PL_SPC_PriceList"));
            String Sql = "SELECT [ID] ,[type] ,[insurer_license] ,[facility_license] ,[package_name] ,[clinician_license] ,[startDate] "
                    + "      ,[endDate] ,[factor] ,[approved] ,[deleted] ,[parentId] ,[PHARM_DISCOUNT] ,[IP_DISCOUNT],[OP_DISCOUNT] "
                    + "      ,[BASE_RATE] ,[regulator_id] ,[GAP] ,[MARGINAL]  FROM [PL_SPC_PriceList]"
                    + " Where deleted = 0 AND approved = 1";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Long ID = rs.getLong("ID");
                    Integer type = rs.getInt("type");
                    String insurerLicense = rs.getString("insurer_license");
                    String facilityLicense = rs.getString("facility_license");
                    String packageName = rs.getString("package_name");
                    String clinicianLicense = rs.getString("clinician_license");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");
                    Double factor = rs.getDouble("factor");
                    Boolean approved = rs.getBoolean("approved");
                    Boolean deleted = rs.getBoolean("deleted");
                    Integer parentId = rs.getInt("parentId");
                    Double PHARM_DISCOUNT = rs.getDouble("PHARM_DISCOUNT");
                    Double IP_DISCOUNT = rs.getDouble("IP_DISCOUNT");
                    Double OP_DISCOUNT = rs.getDouble("OP_DISCOUNT");
                    Double BASE_RATE = rs.getDouble("BASE_RATE");
                    Integer regulator = rs.getInt("regulator_id");
                    Double GAP = rs.getDouble("GAP");
                    Double MARGINAL = rs.getDouble("MARGINAL");

                    SPCContract spcContract = new SPCContract(ID, type,
                            insurerLicense, facilityLicense, packageName,
                            clinicianLicense, startDate, endDate, factor,
                            approved, deleted, parentId, PHARM_DISCOUNT,
                            IP_DISCOUNT, OP_DISCOUNT, BASE_RATE, regulator,
                            GAP, MARGINAL);
                    list.add(spcContract);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }
public static CachedData  getRCMFacilityCodesMapping(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<RCMFacilityCodeMapping> list = new ArrayList<>();
        try {
            tables.put("RCM_FACILITY_CODES_MAPPING", getTableChecksum(db, "RCM_FACILITY_CODES_MAPPING"));
            String Sql = "SELECT    Facility_license, HIS_CODE, ACTIVITY_CODE,   ACTIVITY_TYPE,   ISNULL(PRICE, 0) AS PRICE \n" +
"  FROM    RCM_FACILITY_CODES_MAPPING ";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    String provider = rs.getString("Facility_license");
                    String providerActivityCode = rs.getString("HIS_CODE");
                    String activityCode = rs.getString("ACTIVITY_CODE");
                    int type = rs.getInt("ACTIVITY_TYPE");
                    double price = rs.getDouble("PRICE");                     
                    RCMFacilityCodeMapping mapping = new RCMFacilityCodeMapping( activityCode,   price,   providerActivityCode,   type, provider );
                    list.add(mapping);
                }
                System.out.println("RCM_FACILITY_CODES_MAPPING size:"+list.size() );
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getSPCGroupFactors(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<String, Long>();
        List<SPCGroupFactor> list = new ArrayList<SPCGroupFactor>();
        try {
            tables.put("PL_SPC_Group_FACTOR", getTableChecksum(db, "PL_SPC_Group_FACTOR"));

            String Sql = "SELECT [ID] ,[pl_pricelist_id] ,[group_id] ,[factor] ,[startDate] ,[endDate] "
                    + "FROM PL_SPC_Group_FACTOR "
                    + "WHERE [approved] = 1 AND [deleted] = 0";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Long ID = rs.getLong("ID");
                    Long pl_pricelist_id = rs.getLong("pl_pricelist_id");
                    Long group_id = rs.getLong("group_id");
                    Double factor = rs.getDouble("factor");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");

                    SPCGroupFactor spcGroupFactor = new SPCGroupFactor(ID, pl_pricelist_id,
                            group_id, factor, startDate, endDate);
                    list.add(spcGroupFactor);
                }
                rs.close();
                rs = null;
            }

            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }

        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getSPCCodeFactors(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<String, Long>();
        List<SPCCodeFactor> list = new ArrayList<SPCCodeFactor>();
        try {
            tables.put("PL_SPC_CODES_FACTOR", getTableChecksum(db, "PL_SPC_CODES_FACTOR"));

            String Sql = "SELECT [ID], [pl_pricelist_id] "
                    + " , CASE"
                    + " WHEN CPT_code IS NOT NULL THEN 3 "
                    + " WHEN HCPCS_code IS NOT NULL THEN 4 "
                    + " WHEN TradeDrug_code IS NOT NULL THEN 5 "
                    + " WHEN Dental_code IS NOT NULL THEN 6 "
                    + " WHEN Service_code IS NOT NULL THEN 8 "
                    + " WHEN IRDrug_code IS NOT NULL THEN 9 "
                    + " WHEN GenericDrug_code IS NOT NULL THEN 10 "
                    + " END AS [Type] "
                    + " , CASE "
                    + " WHEN CPT_code IS NOT NULL THEN CPT_code "
                    + " WHEN HCPCS_code IS NOT NULL THEN HCPCS_code "
                    + " WHEN TradeDrug_code IS NOT NULL THEN TradeDrug_code "
                    + " WHEN Dental_code IS NOT NULL THEN Dental_code "
                    + " WHEN Service_code IS NOT NULL THEN Service_code "
                    + " WHEN IRDrug_code IS NOT NULL THEN IRDrug_code "
                    + " WHEN GenericDrug_code IS NOT NULL THEN GenericDrug_code "
                    + " END AS [Code] "
                    + " , [factor], [startDate], [endDate] "
                    + "FROM "
                    + " [PL_SPC_CODES_FACTOR] "
                    + " WHERE "
                    + " [approved] = 1 "
                    + " AND [deleted] = 0";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Long ID = rs.getLong("ID");
                    Integer pl_pricelist_id = rs.getInt("pl_pricelist_id");
                    Integer Type = rs.getInt("Type");
                    String Code = rs.getString("Code");
                    Double factor = rs.getDouble("factor");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");

                    SPCCodeFactor spcCodeFactor = new SPCCodeFactor(ID, pl_pricelist_id,
                            CodeType.from(Type), Code, factor, startDate, endDate);
                    list.add(spcCodeFactor);
                }
                rs.close();
                rs = null;
            }

            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }

        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getMasterPriceLists(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<MasterPriceList> list = new ArrayList<>();
        try {

            tables.put("PL_SPC_MasterPriceList", getTableChecksum(db, "PL_SPC_MasterPriceList"));
            tables.put("PL_SPC_MasterPriceListItem", getTableChecksum(db, "PL_SPC_MasterPriceListItem"));

            String Sql = "SELECT [ID], [name], [startDate], [endDate], [regulator_id] "
                    + "  FROM [PL_SPC_MasterPriceList]";
            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Long ID = rs.getLong("ID");
                    String sName = rs.getString("name");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");
                    Integer regulator = rs.getInt("regulator_id");
                    MasterPriceList masterPriceList = new MasterPriceList(ID, sName, startDate,
                            endDate, regulator);
                    list.add(masterPriceList);
                }
                rs.close();
            }

            ps.close();

            //GetItems
//            for (MasterPriceList masterPriceList : list) {
//                masterPriceList.setItems(getMasterPricelistItems(db, masterPriceList));
//            }
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

//    protected static List<MasterPriceListItem> getMasterPricelistItems(Connection db, MasterPriceList master) {
//
//        List<MasterPriceListItem> ret = new ArrayList<MasterPriceListItem>();
//        try {
//            String Sql = "SELECT [ID], "
//                    + "CASE	WHEN CPT_code IS NOT NULL THEN  3  "
//                    + "		WHEN HCPCS_code IS NOT NULL THEN 4  "
//                    + "		WHEN TradeDrug_code IS NOT NULL THEN 5  "
//                    + "		WHEN Dental_code IS NOT NULL THEN 6  "
//                    + "		WHEN Service_code IS NOT NULL THEN 8  "
//                    + "		WHEN IRDrug_code IS NOT NULL THEN 9  "
//                    + "		WHEN GenericDrug_code IS NOT NULL THEN 10  "
//                    + "END AS [Type], "
//                    + "CASE	WHEN CPT_code IS NOT NULL THEN  CPT_code  "
//                    + "		WHEN HCPCS_code IS NOT NULL THEN HCPCS_code  "
//                    + "		WHEN TradeDrug_code IS NOT NULL THEN TradeDrug_code  "
//                    + "		WHEN Dental_code IS NOT NULL THEN Dental_code "
//                    + "		WHEN Service_code IS NOT NULL THEN Service_code "
//                    + "		WHEN IRDrug_code IS NOT NULL THEN IRDrug_code "
//                    + "		WHEN GenericDrug_code IS NOT NULL THEN GenericDrug_code "
//                    + "END AS [Code] "
//                    + " ,[price] ,[startDate] ,[endDate] ,[anaesthesiaBaseUnits] "
//                    + "From [PL_SPC_MasterPriceListItem] "
//                    + "where MasterPriceList_id =  " + master.getId();
//            PreparedStatement ps = db.prepareStatement(Sql);
//            ResultSet rs = ps.executeQuery();
//            if (rs != null) {
//                while (rs.next()) {
//                    Long ID = rs.getLong("ID");
//                    Integer type = rs.getInt("Type");
//                    String code = rs.getString("Code");
//                    Double price = rs.getDouble("price");
//                    Date startDate = rs.getTimestamp("startDate");
//                    Date endDate = rs.getTimestamp("endDate");
//                    Integer anaesthesiaBaseUnits = rs.getInt("anaesthesiaBaseUnits");
//
//                    MasterPriceListItem item = new MasterPriceListItem(ID, master.getId(), code,
//                            CodeType.from(type),
//                            price, startDate, endDate, anaesthesiaBaseUnits);
//                    ret.add(item);
//                }
//                rs.close();
//            }
//            ps.close();
//            return ret;
//
//        } catch (SQLException ex) {
//            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return null;
//    }

    protected static CachedData getMasterPricelistItems(Connection db, String logicalName) {

        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();

        List<MasterPriceListItem> list = new ArrayList();
        try {
            tables.put("PL_SPC_MasterPriceList", getTableChecksum(db, "PL_SPC_MasterPriceList"));
            tables.put("PL_SPC_MasterPriceListItem", getTableChecksum(db, "PL_SPC_MasterPriceListItem"));

            String Sql = "SELECT [ID], "
                    + "CASE	WHEN CPT_code IS NOT NULL THEN  3  "
                    + "		WHEN HCPCS_code IS NOT NULL THEN 4  "
                    + "		WHEN TradeDrug_code IS NOT NULL THEN 5  "
                    + "		WHEN Dental_code IS NOT NULL THEN 6  "
                    + "		WHEN Service_code IS NOT NULL THEN 8  "
                    + "		WHEN IRDrug_code IS NOT NULL THEN 9  "
                    + "		WHEN GenericDrug_code IS NOT NULL THEN 10  "
                    + "END AS [Type], "
                    + "CASE	WHEN CPT_code IS NOT NULL THEN  CPT_code  "
                    + "		WHEN HCPCS_code IS NOT NULL THEN HCPCS_code  "
                    + "		WHEN TradeDrug_code IS NOT NULL THEN TradeDrug_code  "
                    + "		WHEN Dental_code IS NOT NULL THEN Dental_code "
                    + "		WHEN Service_code IS NOT NULL THEN Service_code "
                    + "		WHEN IRDrug_code IS NOT NULL THEN IRDrug_code "
                    + "		WHEN GenericDrug_code IS NOT NULL THEN GenericDrug_code "
                    + "END AS [Code] "
                    + " ,[price] ,[startDate] ,[endDate] ,[anaesthesiaBaseUnits], MasterPriceList_id "
                    + "From [PL_SPC_MasterPriceListItem] ";
            //+ "where MasterPriceList_id =  " + master.getId();
            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Long ID = rs.getLong("ID");
                    Integer type = rs.getInt("Type");
                    String code = rs.getString("Code");
                    Double price = rs.getDouble("price");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");
                    Integer anaesthesiaBaseUnits = rs.getInt("anaesthesiaBaseUnits");
                    Long MasterPriceList_id = rs.getLong("MasterPriceList_id");

                    MasterPriceListItem item = new MasterPriceListItem(ID, MasterPriceList_id, code,
                            CodeType.from(type),
                            price, startDate, endDate, anaesthesiaBaseUnits);
                    list.add(item);
                }
                rs.close();
            }
            ps.close();

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        /*Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);*/
         HashMap<Long, List<MasterPriceListItem>> dataMap = new HashMap<Long, List<MasterPriceListItem>>();
        for (MasterPriceListItem mpli : list) {
            if (dataMap.get(mpli.getCode()) == null)  {
                dataMap.put(mpli.getMasterListId(), new ArrayList<MasterPriceListItem>());
            }
            dataMap.get(mpli.getMasterListId()).add(mpli);

        }
        return new CachedData(logicalName, tables, list, dataMap);
    }

    /**
     * **********************CUSTOM CONTRACT Loads custom contracts from
     * database.
     *
     * @param db the connection to that database to be used.
     * @param logicalName the logical name of the data that will be loaded
     * syntax PL_CUS_CON|receiverLicense| facilityLicense
     * @param receiverLicenseParam receiver license
     * @param facilityLicenseParam facility license
     *
     * @return **************************
     */
    public static CachedData getCustomContracts(Connection db, String logicalName, String receiverLicenseParam,
            String facilityLicenseParam) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<String, Long>();
         HashMap<Long, Long> tablesIds = new HashMap<Long, Long>();
        List<CusContract> list = new ArrayList<CusContract>();
        try {

            tables.put("PL_CUS_Contract", getTableChecksum(db, "PL_CUS_Contract"));
            //tables.put("PL_SPC_MasterPriceListItem", getTableChecksum(db, "PL_SPC_MasterPriceListItem"));

            String Sql = "SELECT [ID],[priceListId],[insurer_license],[facility_license],[package_name],[clinician_license],[startDate] "
                    + " ,[endDate],[approved],[deleted],[regulator_id],[PHARM_DISCOUNT],[IP_DISCOUNT],[OP_DISCOUNT],[BASE_RATE] "
                    //Change back vpande
                    + " ,[GAP],[MARGINAL],[IP_DRG_Factor],  "
                    + " IsNull((Select 1 from PL_CUS_PriceList where PL_CUS_PriceList.ID=priceListId and PL_CUS_PriceList.priceListType='Dental'), 0) As isDental"
                    + "  , multipleProc, primaryProc, secondaryProc, thirdProc, forthProc,HCSPCS_markup,DayCase_DRG_Factor "
                    + " FROM [PL_CUS_Contract] "
                    + " where [approved] =1 "
                    + " and insurer_license='" + receiverLicenseParam + "' "
                    + " and facility_license='" + facilityLicenseParam + "' "
                    + " and deleted = 0";
            Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, Sql);
            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Long ID = rs.getLong("ID");
                    Long priceListId = rs.getLong("priceListId");
                    String insurerLicense = rs.getString("insurer_license");
                    String facilityLicense = rs.getString("facility_license");
                    String packageName = rs.getString("package_name");
                    String clinicianLicense = rs.getString("clinician_license");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");
                    Boolean approved = rs.getBoolean("approved");
                    Boolean deleted = rs.getBoolean("deleted");
                    Integer regulator = rs.getInt("regulator_id");
                    Double PHARM_DISCOUNT = rs.getDouble("PHARM_DISCOUNT");
                    Double IP_DISCOUNT = rs.getDouble("IP_DISCOUNT");
                    Double OP_DISCOUNT = rs.getDouble("OP_DISCOUNT");
                    Double BASE_RATE = rs.getDouble("BASE_RATE");
                    Double GAP = rs.getDouble("GAP");
                    Double MARGINAL = rs.getDouble("MARGINAL");
                    Integer isDental = rs.getInt("isDental");

                    Integer multipleProc = rs.getInt("multipleProc");
                    Double primaryProc = rs.getDouble("primaryProc");
                    Double secondaryProc = rs.getDouble("secondaryProc");
                    Double thirdProc = rs.getDouble("thirdProc");
                    Double forthProc = rs.getDouble("forthProc");
                    Double ip_DRGFactor = rs.getDouble("IP_DRG_Factor");
                    Double dayCase_DRGFactor=rs.getDouble("DayCase_DRG_Factor");
                     Double hspcsMarkUp=rs.getDouble("HCSPCS_markup");
                    CusContract cusContract = new CusContract(ID, priceListId,
                            insurerLicense, facilityLicense, packageName,
                            clinicianLicense, startDate, endDate,
                            approved, deleted, PHARM_DISCOUNT,
                            IP_DISCOUNT, OP_DISCOUNT, BASE_RATE, regulator,
                            GAP, MARGINAL, isDental,
                            multipleProc, primaryProc,
                            secondaryProc, thirdProc, forthProc, ip_DRGFactor,dayCase_DRGFactor,hspcsMarkUp);
                    list.add(cusContract);
                }
                rs.close();
            }

            ps.close();

            //GetItems
            /*for (MasterPriceList masterPriceList : list) {
             masterPriceList.setItems(getMasterPricelistItems(db, masterPriceList));
             }*/
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
         for (CusContract cusContract : list) {
            tablesIds.put(cusContract.getID(), getTableChecksumById(db, "PL_CUS_Contract_TAUD", cusContract.getID()));
        }
        return new CachedData(logicalName, tables, tablesIds, list);
     //   return new CachedData(logicalName, tables, list);
    }
 public static ConcurrentHashMap<String, CachedData> getCustomContracts(Connection db) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<String, Long>();
        HashMap<Long, Long> tablesIds = new HashMap<Long, Long>();
        List<CusContract> list = new ArrayList<CusContract>();
        try {

            tables.put("PL_CUS_Contract", getTableChecksum(db, "PL_CUS_Contract"));
//            tables.put("ACCUMED_INSURERS", getTableChecksum(db, "ACCUMED_INSURERS"));

            String Sql = "SELECT CONTRACT.[ID],CONTRACT.[priceListId],CONTRACT.[insurer_license],CONTRACT.[facility_license],CONTRACT.[package_name]"
                    + ",CONTRACT.[clinician_license],CONTRACT.[startDate] "
                    + " ,CONTRACT.[endDate],CONTRACT.[approved],CONTRACT.[deleted],CONTRACT.[regulator_id],CONTRACT.[PHARM_DISCOUNT],CONTRACT.[IP_DISCOUNT]"
                    + ",CONTRACT.[OP_DISCOUNT],CONTRACT.[BASE_RATE] "
                    + " ,CONTRACT.[GAP],CONTRACT.[MARGINAL], "
                    + " IsNull((Select 1 from PL_CUS_PriceList where PL_CUS_PriceList.ID=CONTRACT.priceListId and PL_CUS_PriceList.priceListType='Dental'), 0) As isDental"
                    + "  ,CONTRACT. multipleProc,CONTRACT. primaryProc, CONTRACT.secondaryProc, CONTRACT.thirdProc,CONTRACT. forthProc ,CONTRACT.[IP_PHARM_DISCOUNT],CONTRACT.[OP_PHARM_DISCOUNT]   "
                    + ",ltrim(rtrim(CONTRACT.policy_name )) as policy_name ,ltrim(rtrim(CONTRACT.class_name)) as class_name,CONTRACT.ip_Copayment,CONTRACT.op_Copayment,CONTRACT.IP_MAX_PATIENT_SHARE,CONTRACT.OP_MAX_PATIENT_SHARE  , CONTRACT.ROOM_LIMIT, CONTRACT.PRIOR_APPROVAL_LIMIT,CONTRACT.ROOM_TYPE ,CONTRACT.COMPANY_CODE, ISNULL( INS.IP_SUSPENSION,CONTRACT.IP_SUSPENSION) AS IP_SUSPENSION,ISNULL( INS.OP_SUSPENSION,CONTRACT.OP_SUSPENSION) AS OP_SUSPENSION "
                    + ",HCSPCS_markup,IP_DRG_Factor,DayCase_DRG_Factor"
                    + " FROM [PL_CUS_Contract] AS CONTRACT LEFT JOIN ACCUMED_INSURERS AS INS ON CONTRACT.insurer_license= INS.AUTH_NO"
                    + " where CONTRACT.[approved] =1 "
                    + " and CONTRACT.deleted = 0";
            Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, Sql);
            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Long ID = rs.getLong("ID");
                    Long priceListId = rs.getLong("priceListId");
                    String insurerLicense = rs.getString("insurer_license");
                    String facilityLicense = rs.getString("facility_license");
                    String packageName = rs.getString("package_name");                    
                    String clinicianLicense = rs.getString("clinician_license");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");
                    Boolean approved = rs.getBoolean("approved");
                    Boolean deleted = rs.getBoolean("deleted");                    
                    Integer regulator = rs.getInt("regulator_id");
                    Double PHARM_DISCOUNT = rs.getObject("PHARM_DISCOUNT") == null ? null : rs.getDouble("PHARM_DISCOUNT");                    
                    Double IP_DISCOUNT = rs.getObject("IP_DISCOUNT") == null ? null : rs.getDouble("IP_DISCOUNT");
                    Double OP_DISCOUNT = rs.getObject("OP_DISCOUNT") == null ? null : rs.getDouble("OP_DISCOUNT");
                    Double BASE_RATE = rs.getObject("BASE_RATE") == null ? null : rs.getDouble("BASE_RATE");
                    Double GAP = rs.getObject("GAP") == null ? null : rs.getDouble("GAP");
                    Double MARGINAL = rs.getObject("MARGINAL") == null ? null : rs.getDouble("MARGINAL");
                    Integer isDental = rs.getObject("isDental") == null ? null : rs.getInt("isDental");                   
                    Integer multipleProc = rs.getObject("multipleProc") == null ? null : rs.getInt("multipleProc");
                    Double primaryProc = rs.getObject("primaryProc") == null ? null : rs.getDouble("primaryProc");
                    Double secondaryProc = rs.getObject("secondaryProc") == null ? null : rs.getDouble("secondaryProc");
                    Double thirdProc = rs.getObject("thirdProc") == null ? null : rs.getDouble("thirdProc");
                    Double forthProc = rs.getObject("forthProc") == null ? null : rs.getDouble("forthProc");
                     Double hspcsMarkUp=rs.getObject("HCSPCS_markup") == null ? null : rs.getDouble("IP_DRG_Factor");
                     Double ip_drg_factor=rs.getObject("IP_DRG_Factor") == null ? null : rs.getDouble("HCSPCS_markup");
                     Double dayCase_drg_factor=rs.getObject("DayCase_DRG_Factor") == null ? null : rs.getDouble("DayCase_DRG_Factor");
                    CusContract cusContract = new CusContract(ID, priceListId,
                            insurerLicense, facilityLicense, packageName,
                            clinicianLicense, startDate, endDate,
                            approved, deleted, PHARM_DISCOUNT,
                            IP_DISCOUNT, OP_DISCOUNT, BASE_RATE, regulator,
                            GAP, MARGINAL, isDental,
                            multipleProc, primaryProc,
                            secondaryProc, thirdProc, forthProc,ip_drg_factor,dayCase_drg_factor,hspcsMarkUp);
                    
                    cusContract.setStatus(com.accumed.pricing.model.Status.VALID);
                    
                    list.add(cusContract);
                }
                rs.close();
            }

            ps.close();

            //GetItems
            /*for (MasterPriceList masterPriceList : list) {
             masterPriceList.setItems(getMasterPricelistItems(db, masterPriceList));
             }*/
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }

        ConcurrentHashMap<String, CachedData> cachedDB = new ConcurrentHashMap();
        for (CusContract cusContract : list) {
            String sLogicalName = "PL_CUS_CON|" + cusContract.getInsurerLicense() + "|" + cusContract.getFacilityLicense();
            if (cachedDB.get(sLogicalName) == null) {
                cachedDB.put(sLogicalName, new CachedData(sLogicalName, tables, new HashMap<Long, Long>(), new java.util.ArrayList()));
            }
            cachedDB.get(sLogicalName).getData().add(cusContract);
        }
        return cachedDB;
    }
  public static ConcurrentHashMap<String, CachedData> getCustomPriceLists(Connection db) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<String, Long>();
        HashMap<Long, Long> tablesIds = new HashMap<Long, Long>();
        List<CusPriceListItem> list = new ArrayList<CusPriceListItem>();
        try {

            tables.put("PL_NEW_CUS_PriceListItem", getTableChecksum(db, "PL_NEW_CUS_PriceListItem"));
//            tables.put("ACCUMED_INSURERS", getTableChecksum(db, "ACCUMED_INSURERS"));

            String Sql = "SELECT [ID] "
                    + " , PL_NEW_CUS_PriceListItem.[type] AS [Type] "
                    + " , PL_NEW_CUS_PriceListItem.code AS [Code]  "
                    + " , [price] "
                    + " , [discount] "
                    + " , [startDate] "
                    + " , [endDate] "
                    + " , PriceList_Id "
                    + " , covered , need_approval  FROM "
                    + " [PL_NEW_CUS_PriceListItem] "
                    + " where deleted = 0 ";

            try (  
                    PreparedStatement ps = db.prepareStatement(Sql)) {
                ResultSet rs = ps.executeQuery();
                if (rs != null) {
                    while (rs.next()) {
                        Long ID = rs.getLong("ID");
                        Integer type = rs.getInt("Type");
                        String code = rs.getString("Code");
                        Double price = rs.getDouble("price");
                        Long cusPricListId = rs.getLong("PriceList_Id");
                        Double discount = null;
                        if (rs.getObject("discount") != null) {
                            discount = rs.getDouble("discount");
                        }
                      
                       

                        /*if(rs.wasNull()){
                         discount = null;
                         }*/
                        Date startDate = rs.getTimestamp("startDate");
                        Date endDate = rs.getTimestamp("endDate");
                        if (code == null) {
                            continue;
                        }
                       
                        CusPriceListItem item = new CusPriceListItem(ID, cusPricListId, code,
                                CodeType.from(type),
                                price, discount, startDate, endDate);
                       
                        item.setStatus(com.accumed.pricing.model.Status.VALID);
                        list.add(item);
                    }
                }
                rs.close();
            }

            //GetItems
            /*for (MasterPriceList masterPriceList : list) {
             masterPriceList.setItems(getMasterPricelistItems(db, masterPriceList));
             }*/
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }

        ConcurrentHashMap<String, CachedData> cachedDB = new ConcurrentHashMap();

        for (CusPriceListItem item : list) {
            String sLogicalName = "PL_CUS_PL|" + item.getPricListId();
            if (cachedDB.get(sLogicalName) == null) {
                cachedDB.put(sLogicalName, new CachedData(sLogicalName, tables, new HashMap<Long, Long>(), new java.util.ArrayList(), new HashMap<String, CusPriceListItem>()));
            }
            cachedDB.get(sLogicalName).getDataMap().put(item.getCode(), item);
            cachedDB.get(sLogicalName).getData().add(item);
        }
        return cachedDB;
    }

    public static CachedData getCustomPriceLists(Connection db, String logicalName, Long priceListID) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<String, Long>();
        List<CusPriceListItem> list = new ArrayList<CusPriceListItem>();
         Long auditId = getTableChecksum(db, "PL_NEW_CUS_PriceListItem");
          HashMap<Long, Long> tablesIds = new HashMap<Long, Long>();

        tables.put("PL_CUS_PriceList", getTableChecksum(db, "PL_CUS_PriceList"));
        tables.put("PL_NEW_CUS_PriceListItem", getTableChecksum(db, "PL_NEW_CUS_PriceListItem"));

        list.addAll(getCustomPricelistItems(logicalName, db, priceListID));
         for (CusPriceListItem item : list) {
            tablesIds.put(item.getId(), auditId);
        }
         return new CachedData(logicalName, tables, tablesIds, list);
    
    }

    public static List<CusPriceListItem> getCustomPricelistItems(String logicalName, Connection db, Long cusPricListId) {

        long lBegin = System.nanoTime();
        List<CusPriceListItem> list = new ArrayList<>();
        try {

            String Sql = "SELECT [ID] "
                    + " , PL_NEW_CUS_PriceListItem.[type] AS [Type] "
                    + " , PL_NEW_CUS_PriceListItem.code AS [Code]  "
                    + " , [price] "
                    + " , [discount] "
                    + " , [startDate] "
                    + " , [endDate] "
                    + " FROM "
                    + " [PL_NEW_CUS_PriceListItem] "
                    + " where deleted = 0 "
                    + " and PriceList_id = " + cusPricListId;

            try ( /*if(master.getID() == 486){
                     Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, Sql);
                     }*/PreparedStatement ps = db.prepareStatement(Sql)) {
                ResultSet rs = ps.executeQuery();
                if (rs != null) {
                    while (rs.next()) {
                        Long ID = rs.getLong("ID");
                        Integer type = rs.getInt("Type");
                        String code = rs.getString("Code");
                        Double price = rs.getDouble("price");
                        Double discount = null;
                        if (rs.getObject("discount") != null) {
                            discount = rs.getDouble("discount");
                        }

                        /*if(rs.wasNull()){
                         discount = null;
                         }*/
                        Date startDate = rs.getTimestamp("startDate");
                        Date endDate = rs.getTimestamp("endDate");
                        if (code == null) {
                            continue;
                        }
                        /*if (code.equalsIgnoreCase("92222")) {
                         Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, "test " + discount + " " + master.getID());
                         }
                         if ("17-03".equals(code)) {
                         Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, " priceListID=" + master.getID() + " code=" + code);
                         }*/
                        CusPriceListItem item = new CusPriceListItem(ID, cusPricListId, code,
                                CodeType.from(type),
                                price, discount, startDate, endDate);
                        list.add(item);
                    }
                    rs.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return list;
    }

    public static CachedData getFacilityClinicians(Connection db, String logicalName, String facilityLicense) {

        long lBegin = System.nanoTime();

        HashMap<String, Long> tables = new HashMap<String, Long>();
        List<Clinician> list = new ArrayList<Clinician>();
        try {

            tables.put("ACCUMED_CLINICIANS", getTableChecksum(db, "ACCUMED_CLINICIANS"));

            String Sql = "SELECT c.Clinician_License\n"
                    + ", c.Profession "
                    + ", c.Category "
                    + ", c.Facility_License "
                    + ", c.Facility_Type "
                    + ", c.Status "
                    + ", Convert(datetime, c.Valid_From, 103) As Valid_From "
                    + ", Convert(datetime, c.Valid_To, 103) As  Valid_To "
                    + ", c.regulator "
                    + "FROM "
                    + "ACCUMED_CLINICIANS c"
                    + " WHERE c.Facility_License='" + facilityLicense + "'";

           // Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, Sql);
            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    String license = rs.getString("Clinician_License");
                    String facility_license = rs.getString("Facility_License");
                    Date validFrom = rs.getTimestamp("Valid_From");
                    Date validTo = rs.getTimestamp("Valid_To");
                    String status = rs.getString("Status");
                    String profession = rs.getString("Profession");
                    String category = rs.getString("Category");
                    String facility_type = rs.getString("Facility_Type");
                    Integer regulator = rs.getInt("regulator");
                    list.add(new Clinician(license, facility_license, validFrom,
                            validTo, status, profession, category, facility_type,
                            regulator));
                }
                rs.close();
            }
            ps.close();

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getFacilities(Connection db, String logicalName) {

        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<Facility> list = new ArrayList<>();
        try {

            tables.put("ACCUMED_FACILITY", getTableChecksum(db, "ACCUMED_FACILITY"));

            String Sql = "Select F.FACILITY_LICENSE, F.REGULATOR, \n"
                    + "F.IS_ACTIVE, F.FACILITY_TYPE_ID, \n"
                    + "F.FACILITY_STATUS_ID\n"
                    + "from ACCUMED_FACILITY F\n";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    String FACILITY_LICENSE = rs.getString("FACILITY_LICENSE");
                    Integer REGULATOR = rs.getInt("REGULATOR");
                    Integer IS_ACTIVE = rs.getInt("IS_ACTIVE");
                    Integer FACILITY_TYPE_ID = rs.getInt("FACILITY_TYPE_ID");
//                    String FACILITY_STATUS = rs.getString("FACILITY_STATUS_ID");

                    list.add(new Facility(FACILITY_LICENSE, FacilityType.from(FACILITY_TYPE_ID),
                            Regulator.from(REGULATOR), IS_ACTIVE != 0));
                }
                rs.close();
            }
            ps.close();

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static Date getMidnightYesterday() {
        Calendar calStart = new GregorianCalendar();
        calStart.setTime(new Date());
        calStart.set(Calendar.HOUR_OF_DAY, 0);
        calStart.set(Calendar.MINUTE, 0);
        calStart.set(Calendar.SECOND, 0);
        calStart.set(Calendar.MILLISECOND, 0);
        Date midnightYesterday = calStart.getTime();
        return midnightYesterday;
    }

    public static CachedData getCodeGroups(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<CodeGroup> list = new ArrayList<>();
        try {
            tables.put("ACCUMED_CODE_GROUPS", getTableChecksum(db, "ACCUMED_CODE_GROUPS"));
            tables.put("ACCUMED_GROUP_CODES", getTableChecksum(db, "ACCUMED_GROUP_CODES"));
            tables.put("PL_SPC_GROUP_FACTOR", getTableChecksum(db, "PL_SPC_GROUP_FACTOR"));

            String Sql = "SELECT c.ID as id ,c.NAME as name,c.TYPE as type ,[VERSION] , g.[FROM] as f ,g.[TO] as t,"
                    + " IsNull((Select name + '|' AS [text()] from ACCUMED_CODE_GROUPS where PARENT_ID = c.id "
                    + " order by ID "
                    + " For XML PATH ('')), '') as childGroups "
                    + "FROM  "
                    + " ACCUMED_GROUP_CODES  g right  JOIN     ACCUMED_CODE_GROUPS  c"
                    + " ON g.GROUP_ID=c.ID"
                    + " where c.name in (\n"
                    + "Select distinct name from \n"
                    + "  ACCUMED_CODE_GROUPS where ID in(\n"
                    + " Select distinct group_id from PL_SPC_GROUP_FACTOR)\n"
                    + " OR NAME in ('Anaesthesia',\n"
                    + " 'Evaluation And Management',\n"
                    + " 'Physical Medicine & Rehabilitation',\n"
                    + " 'Radiology',\n"
                    + " 'Pathology & Laboratory',\n"
                    + " 'DGTest_CPT_9_Series',\n"
                    + " 'Maternity Care & Delivery',\n"
                    + " 'Ophthalmology', 'Anaesthesia', 'Antenatal-Screening ICD_10', "
                    + "'Antenatal-Screening ICD_9', 'Dental Consultation', 'Orthodontic Procedures', "
                    + "'Prosthodontics_removable', 'Prosthodontics_fixed', 'Consultations', 'X-RAY',"
                    + "'Anesthesia Discount Exclusion', 'OT Discount Exclusion', 'DSL codes')"
                    + ") ORDER   BY   c.ID";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Integer id = rs.getInt("id");
                    String name = rs.getString("name");
                    String type = rs.getString("type");
                    String version = rs.getString("VERSION");
                    String from = rs.getString("f");
                    String to = rs.getString("t");
                    String childGroups = rs.getString("childGroups");
                    CodeType ttype = null;
                    CodeGroup codeGroup = new CodeGroup(CodeType.CPT, id, name, childGroups);
                    if ("CPT".equalsIgnoreCase(type)) {
                        ttype = CodeType.CPT;
                    }
                    if ("HCPCS".equalsIgnoreCase(type)) {
                        ttype = CodeType.HCPCS;
                    }
                    if ("DRUG".equalsIgnoreCase(type)) {
                        ttype = CodeType.TRADE_DRUG;
                    }
                    if ("DENTAL".equalsIgnoreCase(type)) {
                        ttype = CodeType.DENTAL;
                    }
                    if ("SERVICE".equalsIgnoreCase(type)) {
                        ttype = CodeType.SERVICE;
                    }
                    if ("IR_DRUG".equalsIgnoreCase(type)) {
                        ttype = CodeType.IR_DRG;
                    }
                    if ("GENERIC_DRUG".equalsIgnoreCase(type)) {
                        ttype = CodeType.GENERIC_DRUG;
                    }

                    if ("ICD".equalsIgnoreCase(type)) {
                        if (version != null) {
                            if ("ICD-10".equalsIgnoreCase(version)) {
                                ttype = CodeType.ICD10;
                            }
                            if ("ICD-9".equalsIgnoreCase(version)) {
                                ttype = CodeType.ICD9;
                            }
                        }
                    }

                    if (ttype == null) {
                        continue;
                    }
                    codeGroup.setType(ttype);

                    GroupCodesRange groupCodesRange = new GroupCodesRange(from, to);
                    boolean alreadyExisted = false;
                    for (CodeGroup cg : list) {
                        if (cg.getName().equals(name) && cg.getType() == codeGroup.getType()) {
                            if (cg.getItems() != null) {
                                cg.getItems().add(groupCodesRange);

                            } else {
                                List<GroupCodesRange> groupCodesRangeList = new ArrayList();
                                groupCodesRangeList.add(groupCodesRange);
                                cg.setItems(groupCodesRangeList);
                            }
                            alreadyExisted = true;
                            break;
                        }
                    }
                    if (!alreadyExisted) {
                        List<GroupCodesRange> groupCodesRangeList = new ArrayList();
                        groupCodesRangeList.add(groupCodesRange);
                        codeGroup.setItems(groupCodesRangeList);
                        //Logger.getLogger(DB.class.getName()).info("added new  group :" + codeGroup.getName() + "");
                        //Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, "added new  group :" + codeGroup.getName() + "");
                        list.add(codeGroup);
                    }

                }
                rs.close();
                rs = null;
            }

            ps.close();
            ps = null;

//            //getChildren
//            for (CodeGroup codeGroup : list) {
//                codeGroup.setChildren(getGroupChildren(db, codeGroup));
//            }
//            //GetItems
//            for (CodeGroup codeGroup : list) {
//                codeGroup.setItems(getGroupCodes(db, codeGroup));
//            }
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }

        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    //getDRGCodes
    public static CachedData getDRGCodes(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DRG> list = new ArrayList<>();
        try {
            tables.put("ACCUMED_DRG_CODES", getTableChecksum(db, "ACCUMED_DRG_CODES"));

            String Sql = "Select CODE, [WEIGHT], EFFECTIVE_DATE, [EXPIRY_DATE], PORTION  from ACCUMED_DRG_CODES";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    String code = rs.getString("CODE");
                    Double weight = rs.getDouble("WEIGHT");
                    Double portion = rs.getDouble("PORTION");
                    Date startDate = rs.getTimestamp("EFFECTIVE_DATE");
                    Date endDate = rs.getTimestamp("EXPIRY_DATE");
                    DRG drg = new DRG(code, weight,
                            portion, startDate, endDate);
                    list.add(drg);
                }
                rs.close();
                rs = null;
            }

            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }

        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }
    public static CachedData getDrugPrices(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DrugPrice> list = new ArrayList<>();
        ResultSet rs=null;
        PreparedStatement ps =null;
        try {
            tables.put("ACCUMED_DRUG_PRICES", getTableChecksum(db, "ACCUMED_DRUG_PRICES"));
            tables.put("ACCUMED_DHA_DRUG", getTableChecksum(db, "ACCUMED_DHA_DRUG"));

            String Sql = "SELECT [CODE], [Package_Price_to_Public], [Unit_Price_to_Public], [DATE_FROM]\n"
                    + " ,[DATE_TO], [regulator], [Dosage_Form] , [Discount_Tier] "
                    + ",[Package_Markup],[UPP_SCOPE],[INCLUDED_THIQA],[Unit_Markup] FROM [DRUG_PRICES_VIEW] order by regulator,code";

             ps = db.prepareStatement(Sql);
             rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {                    
                    String code = rs.getString("CODE");                    
                    Double package_Price_to_Public = rs.getDouble("Package_Price_to_Public");
                    Double unit_Price_to_Public = rs.getDouble("Unit_Price_to_Public");
                    Date startDate = rs.getTimestamp("DATE_FROM");
                    Date endDate = rs.getTimestamp("DATE_TO");
                    Integer regulator = rs.getInt("regulator");
                    String dosage_Form = rs.getString("Dosage_Form");
                    Integer discountTier = rs.getInt("Discount_Tier");
        Double packageMarkup = rs.getDouble("Package_Markup");
Boolean  uppScope = rs.getBoolean("UPP_SCOPE");
            
Boolean includedThiqa = rs.getBoolean("INCLUDED_THIQA");
Double unitMarkup = rs.getDouble("Unit_Markup");
                 //   System.out.println("getDrugPrices:"+regulator+":code:"+code);
                    DrugPrice drugPrice = new DrugPrice(code, package_Price_to_Public,
                            unit_Price_to_Public, startDate, endDate, regulator,dosage_Form, discountTier,packageMarkup,
                    uppScope,includedThiqa,unitMarkup);
                    list.add(drugPrice);
                }
                
            }

           
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally
        {
            try {
                if(rs !=null  )
                {
                    rs.close();
                    rs = null;
                }
            } catch (SQLException ex) {
                Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                if(ps !=null  )
                {
                     ps.close();
                      ps = null;

                }
            } catch (SQLException ex) {
                Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        HashMap<String, DrugPrice> dataMap = new HashMap<String, DrugPrice>();
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        for (DrugPrice drugPrice : list) {
            dataMap.put(drugPrice.getCode(), drugPrice);
        }
        return new CachedData(logicalName, tables, list, dataMap);
    }

    public static List<PLContract> getContracts(Connection db, String sfacilityLicense,
            String sReceiverLicense, Date startDate, Date endDate) {
        java.text.SimpleDateFormat fmt = new java.text.SimpleDateFormat("dd/MM/yyyy");
        List<PLContract> ret = new ArrayList();
        long lBegin = System.nanoTime();

        try {
            String sQuery = "Select * from (\n"
                    + "SELECT id, 1 as [type],\n"
                    + "p.MARGINAL, p.GAP, p.BASE_RATE, p.NEGOTIATION_DRG_FACTOR as IP_DRG_Factor,p.NEGOTIATION_DRG_FACTOR as DayCase_DRG_FACTOR, p.package_name\n"
                    + "FROM PL_SPC_PriceList p \n"
                    + "WHERE p.deleted=0 \n"
                    + "AND p.facility_license = '" + sfacilityLicense.trim() + "' \n"
                    + "AND p.insurer_license = '" + sReceiverLicense.trim() + "' \n"
                    + "AND (Convert(datetime, '" + fmt.format(startDate) + "', 103) BETWEEN p.startDate AND p.endDate) \n"
                    + "AND (Convert(datetime, '" + fmt.format(endDate) + "', 103) BETWEEN p.startDate AND p.endDate)\n"
                    + "UNION ALL\n"
                    + "SELECT id, 2 as [type],\n"
                    //Change back vpande
                    + "cus.MARGINAL, cus.GAP, cus.BASE_RATE, cus.IP_DRG_Factor,cus.DayCase_DRG_FACTOR, cus.package_name\n"
                    + "FROM PL_CUS_Contract cus \n"
                    + "WHERE cus.deleted=0 \n"
                    + "AND cus.facility_license = '" + sfacilityLicense.trim() + "' \n"
                    + "AND cus.insurer_license = '" + sReceiverLicense.trim() + "' \n"
                    + "AND (Convert(datetime, '" + fmt.format(startDate) + "', 103) BETWEEN cus.startDate AND cus.endDate) \n"
                    + "AND (Convert(datetime, '" + fmt.format(endDate) + "', 103) BETWEEN cus.startDate AND cus.endDate))A \n"
                    + "order by A.type, A.package_name\n";

            Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, "sQuery=" + sQuery);
            PreparedStatement ps = db.prepareStatement(sQuery);
            ResultSet rs = ps.executeQuery();
            boolean rsHasRows = false;
            if (rs != null) {
                while (rs.next()) {
                    Integer id = rs.getInt("id");
                    Integer type = rs.getInt("type");
                    Double marginal = rs.getDouble("MARGINAL");
                    Double gap = rs.getDouble("GAP");
                    Double baseRate = rs.getDouble("BASE_RATE");
                    Double IP_DRG_Factor = rs.getDouble("IP_DRG_Factor");
                    Double DayCase_DRG_FACTOR=rs.getDouble("DayCase_DRG_FACTOR");
                    String packageName = rs.getString("package_name");

                    PLContract plContract = new PLContract(id, type,
                            baseRate, gap, marginal, IP_DRG_Factor,DayCase_DRG_FACTOR, packageName);

                    ret.add(plContract);
                    rsHasRows=true;
                }
                rs.close();
                rs = null;
            }
           
            //Added by vpande on 27 Feb 2023 to account for emergency cases where there is only fixed contract for Req#4
            if (rsHasRows==false)
            {
               sQuery="SELECT id, 3 as [type],\n"
                    + "cus.MARGINAL, cus.GAP, cus.BASE_RATE, cus.IP_DRG_Factor,cus.DayCase_DRG_FACTOR, cus.package_name\n"
                    + "FROM PL_CUS_Contract cus \n"
                    + "WHERE cus.deleted=0 \n"
                    + "AND cus.facility_license = 'EmergencyFacility' \n"
                    + "AND cus.insurer_license = 'EmergencyInsurance' \n"
                    + "AND (Convert(datetime, '" + fmt.format(startDate) + "', 103) BETWEEN cus.startDate AND cus.endDate) \n"
                    + "AND (Convert(datetime, '" + fmt.format(endDate) + "', 103) BETWEEN cus.startDate AND cus.endDate) \n"
                    + "order by [type], package_name\n";
               Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, "sQuery=" + sQuery);
               PreparedStatement ps1 = db.prepareStatement(sQuery);
               ResultSet rs1 = ps1.executeQuery();
               if(rs1!=null)
               {
                 while (rs1.next()) 
                 {
                    Integer id1 = rs1.getInt("id");
                    Integer type1 = rs1.getInt("type");
                    Double marginal1 = rs1.getDouble("MARGINAL");
                    Double gap1 = rs1.getDouble("GAP");
                    Double baseRate1 = rs1.getDouble("BASE_RATE");
                    Double IP_DRG_Factor1 = rs1.getDouble("IP_DRG_Factor");
                    Double DayCase_DRG_FACTOR1=rs1.getDouble("DayCase_DRG_FACTOR");
                    String packageName1 = rs1.getString("package_name");

                    PLContract plContract1 = new PLContract(id1, type1,
                            baseRate1, gap1, marginal1, IP_DRG_Factor1,DayCase_DRG_FACTOR1, packageName1);

                    ret.add(plContract1);
                }
                    rs1.close();
                    rs1 = null;
               }
               
                ps1.close();
                ps1 = null;
            }
            //Added by vpande on 27 Feb 2023 to account for emergency cases where there is only fixed contract for Req#4
    
         
            
            ps.close();
            ps = null;
            
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }

        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, "getContracts  time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return ret;
    }

    public static CachedData getDHA_DRG(Connection db, String logicalName,List<Activity> activityList) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DHA_DRG> list = new ArrayList<>();
        try {
            String whereClause=" where  DRG_Code in  ( ";
            for (Activity a :activityList){
             
                whereClause=whereClause+"'"+a.getCode()+"',";
                
             }
            if (activityList!=null && activityList.size()>0){
            whereClause=whereClause.substring(0,  whereClause.length()-1)+")";
            } else{
                whereClause=" where  1<>1 ";
            }
            tables.put("STT_DHA_DRG_CODES", getTableChecksum(db, "STT_DHA_DRG_CODES"));
            String Sql = "SELECT distinct  DRG_Code , Admission_Type  , Relative_Weight  , HCPCS_Portion,   " +
"                  DRUG_Portion , Surgery_Portion , ALOS   FROM   STT_DHA_DRG_CODES  "+whereClause;

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    String addmissionType = rs.getString("Admission_Type");
                    String code = rs.getString("DRG_Code");
                   // String description = rs.getString("DRG_Description");
                    Double relativeWeight = rs.getDouble("Relative_Weight");
                    Double hcpcsPortion = rs.getDouble("HCPCS_Portion");
                    Double drugPortion = rs.getDouble("DRUG_Portion");
                    Double surgeryPortion = rs.getDouble("Surgery_Portion");
                    Double aLos = rs.getDouble("ALOS");
                    DHA_DRG drg = new DHA_DRG(code, addmissionType, relativeWeight, hcpcsPortion, drugPortion, surgeryPortion, aLos );
                    list.add(drg);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getDHA_DRG_HighCost(Connection db, String logicalName,List<Activity>  activityList) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DHA_DRG_HighCost> list = new ArrayList<>();
        try {
             String whereClause=" where  Activity_Type in  (3,4,5) and  Activity_Code in  ( ";
            for (Activity a :activityList){             
                whereClause=whereClause+"'"+a.getCode()+"',";                
             }
            if (activityList!=null && activityList.size()>0){
            whereClause=whereClause.substring(0,  whereClause.length()-1)+")";
            } else{
                whereClause=" where 1<>1";
            }
            tables.put("STT_DHA_DRG_HIGHCOST", getTableChecksum(db, "STT_DHA_DRG_HIGHCOST"));
            String Sql = "SELECT distinct Activity_Code     , Activity_Type      FROM   STT_DHA_DRG_HIGHCOST   "+whereClause;

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Integer type = rs.getInt("Activity_Type");
                    String code = rs.getString("Activity_Code");
                   // String description = rs.getString("Description");

                    DHA_DRG_HighCost drg = new DHA_DRG_HighCost(code, CodeType.from(type));
                    list.add(drg);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getDRGExcludedCPTs(Connection db, String logicalName,List<Activity> activityList ) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DRGExcludedCpts> list = new ArrayList<>();
        try {
            String whereClause=" where  CODE in  ( ";
            for (Activity a :activityList){           
                whereClause=whereClause+"'"+a.getCode()+"',";                
            }
            if (activityList!=null && activityList.size()>0){
            whereClause=whereClause.substring(0,  whereClause.length()-1)+")";
            } else{
                whereClause=" where  1<>1 ";
            }
            tables.put("STT_DRG_EXCLUDED_CPTS", getTableChecksum(db, "STT_DRG_EXCLUDED_CPTS"));
            String Sql = "SELECT   CODE   FROM  STT_DRG_EXCLUDED_CPTS  "+whereClause;

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {

                    String code = rs.getString("CODE");

                    DRGExcludedCpts cpt = new DRGExcludedCpts(code, CodeType.from(3));
                    list.add(cpt);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getDHA_DRG_COST_PER_ACTIVITY(Connection db, String logicalName,List <Activity> activityList) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DHA_DRG_COST_PER_ACTIVITY> list = new ArrayList<>();
        try {
            String whereClause=" where  Activity_Type in (3,4,5) and   Activity_Code in  ( ";
            for (Activity a :activityList){           
                whereClause=whereClause+"'"+a.getCode()+"',";                
            }
            if (activityList!=null && activityList.size()>0){
            whereClause=whereClause.substring(0,  whereClause.length()-1)+")";
            } else{
                whereClause=" where  1<>1";
            }
            tables.put("STT_DHA_COSTPERACTIVITY", getTableChecksum(db, "STT_DHA_COSTPERACTIVITY"));
            String Sql = "select  Activity_Code,Activity_Type,Base_Units_Anesthesia,Cost   from  STT_DHA_COSTPERACTIVITY "+whereClause;

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {

                    Integer type = rs.getInt("Activity_Type");
                    String code = rs.getString("Activity_Code");
                    Integer baseUnitsAnesthesia = rs.getInt("Base_Units_Anesthesia");
                    Double cost = rs.getDouble("Cost");

                    DHA_DRG_COST_PER_ACTIVITY act = new DHA_DRG_COST_PER_ACTIVITY(code, CodeType.from(type), baseUnitsAnesthesia, cost);
                    list.add(act);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }


      public static CachedData getAllDHA_DRG(Connection db, String logicalName ) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DHA_DRG> list = new ArrayList<>();
        try {
            
            tables.put("STT_DHA_DRG_CODES", getTableChecksum(db, "STT_DHA_DRG_CODES"));
            String Sql = "SELECT distinct  DRG_Code , Admission_Type  , Relative_Weight  , HCPCS_Portion,   " +
"                  DRUG_Portion , Surgery_Portion , ALOS   FROM   STT_DHA_DRG_CODES  ";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    String addmissionType = rs.getString("Admission_Type");
                    String code = rs.getString("DRG_Code");
                   // String description = rs.getString("DRG_Description");
                    Double relativeWeight = rs.getDouble("Relative_Weight");
                    Double hcpcsPortion = rs.getDouble("HCPCS_Portion");
                    Double drugPortion = rs.getDouble("DRUG_Portion");
                    Double surgeryPortion = rs.getDouble("Surgery_Portion");
                    Double aLos = rs.getDouble("ALOS");
                    DHA_DRG drg = new DHA_DRG(code, addmissionType, relativeWeight, hcpcsPortion, drugPortion, surgeryPortion, aLos );
                    list.add(drg);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getAllDHA_DRG_HighCost(Connection db, String logicalName ) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DHA_DRG_HighCost> list = new ArrayList<>();
        try {
           
            tables.put("STT_DHA_DRG_HIGHCOST", getTableChecksum(db, "STT_DHA_DRG_HIGHCOST"));
            String Sql = "SELECT distinct Activity_Code     , Activity_Type      FROM   STT_DHA_DRG_HIGHCOST   " ;

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Integer type = rs.getInt("Activity_Type");
                    String code = rs.getString("Activity_Code");
                   // String description = rs.getString("Description");

                    DHA_DRG_HighCost drg = new DHA_DRG_HighCost(code, CodeType.from(type));
                    list.add(drg);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getAllDRGExcludedCPTs(Connection db, String logicalName  ) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DRGExcludedCpts> list = new ArrayList<>();
        try {
            
            tables.put("STT_DRG_EXCLUDED_CPTS", getTableChecksum(db, "STT_DRG_EXCLUDED_CPTS"));
            String Sql = "SELECT   CODE   FROM  STT_DRG_EXCLUDED_CPTS  " ;

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {

                    String code = rs.getString("CODE");

                    DRGExcludedCpts cpt = new DRGExcludedCpts(code, CodeType.from(3));
                    list.add(cpt);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    public static CachedData getAllDHA_DRG_COST_PER_ACTIVITY(Connection db, String logicalName ) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<DHA_DRG_COST_PER_ACTIVITY> list = new ArrayList<>();
        try {
            
            tables.put("STT_DHA_COSTPERACTIVITY", getTableChecksum(db, "STT_DHA_COSTPERACTIVITY"));
            String Sql = "select  Activity_Code,Activity_Type,Base_Units_Anesthesia,Cost   from  STT_DHA_COSTPERACTIVITY " ;

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {

                    Integer type = rs.getInt("Activity_Type");
                    String code = rs.getString("Activity_Code");
                    Integer baseUnitsAnesthesia = rs.getInt("Base_Units_Anesthesia");
                    Double cost = rs.getDouble("Cost");

                    DHA_DRG_COST_PER_ACTIVITY act = new DHA_DRG_COST_PER_ACTIVITY(code, CodeType.from(type), baseUnitsAnesthesia, cost);
                    list.add(act);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }
     public static CachedData getAllCustomeCodes(Connection db, String logicalName ) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<CustomCode> list = new ArrayList<>();
        try {            
            tables.put("RCM_Custom_Codes", getTableChecksum(db, "RCM_Custom_Codes"));
            //String Sql = "select   ActivityType,ActivityCode,Price from RCM_Custom_Code_View ";
            String Sql = "select   ActivityType,ActivityCode,Price from RCM_Custom_Codes";
            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    int type = rs.getInt("Activitytype");
                    Double price = rs.getDouble("Price");
                    String code = rs.getString("ActivityCode");
                    CustomCode cus = new CustomCode(code, type, price);
                    list.add(cus);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    }

    static CachedData getAllPackageCodes(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<PackageCode> list = new ArrayList<>();
        try {
            
            tables.put("RCM_Package_Group", getTableChecksum(db, "RCM_Package_Group"));
            String Sql = "select  packageGroupID,ActivityType,PackageCode,NetAmount,IS_ITEM_LEVEL from RCM_Package_Group ";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    //int type=rs.getInt("Activitytype");
                     double price = rs.getDouble("NetAmount");
                    String code = rs.getString("PackageCode");
                    int packageGroupID=rs.getInt("packageGroupID");
                    boolean isItemLevel=rs.getBoolean("IS_ITEM_LEVEL");
                    PackageCode cus = new PackageCode(code, price,packageGroupID,isItemLevel);
                   
                    
                     
                    list.add(cus);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    
    }
static CachedData getAllPackageItemCodes(Connection db, String logicalName) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<PackageItemCode> list = new ArrayList<>();
        try {
            
            tables.put("RCM_Package_Item", getTableChecksum(db, "RCM_Package_Item"));
            String Sql = "select  PackageGroupID,ActivityType,ActivityCode,NetAmount from RCM_Package_Item where  IsActive=1";

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    int type=rs.getInt("ActivityType");
                     Double price = rs.getDouble("NetAmount");
                    String code = rs.getString("ActivityCode");
                    int packageID=rs.getInt("PackagegroupID");
                    PackageItemCode cus = new PackageItemCode(code,type,price,packageID);                     
                    list.add(cus);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData(logicalName, tables, list);
    
    }
  public static ConcurrentHashMap<String, CachedData> getMaxAuditIds(Connection db) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<String, Long>();

        ConcurrentHashMap<String, CachedData> cachedDB = new ConcurrentHashMap();
      addCashingKeyAndData(cachedDB, "CONTARCT_CHECK_SUM", "PL_CUS_Contract", db, tables);
      addCashingKeyAndData(cachedDB, "PRICE_LIST_ITEM_CHECK_SUM", "PL_NEW_CUS_PriceListItem", db, tables);      
      addCashingKeyAndData(cachedDB, "PRICE_LIST_CHECK_SUM", "PL_CUS_PriceList", db, tables);
      addCashingKeyAndData(cachedDB, "SPC_PRICE_LIST_CHECK_SUM", "PL_SPC_PriceList", db, tables);
      addCashingKeyAndData(cachedDB, "SPC_PRICE_LIST_ITEM_CHECK_SUM", "PL_SPC_MasterPriceListItem", db, tables);      
      addCashingKeyAndData(cachedDB, "SPC_GROUP_FACTOR_CHECK_SUM", "PL_SPC_Group_FACTOR", db, tables);
      addCashingKeyAndData(cachedDB, "SPC_CODE_FACTOR_CHECK_SUM", "PL_SPC_CODES_FACTOR", db, tables);
       addCashingKeyAndData(cachedDB, "RCM_FACILITY_CODE_MAPPING_CHECK_SUM", "RCM_FACILITY_CODES_MAPPING", db, tables);
       addCashingKeyAndData(cachedDB, "RCM_PACKAGE_CODE_CHECK_SUM", "RCM_Package_Group", db, tables);
       addCashingKeyAndData(cachedDB, "RCM_PACKAGE_ITEM_CHECK_SUM", "RCM_Package_Item", db, tables);
       addCashingKeyAndData(cachedDB, "RCM_CUSTOM_CODE_CHECK_SUM", "RCM_Custom_Codes", db, tables);

        return cachedDB;
    }
  static void addCashingKeyAndData(ConcurrentHashMap<String, CachedData> cachedDB,String key,String tablename,Connection db,HashMap<String, Long> tables){
      cachedDB.put(key, new CachedData(key, tables, new HashMap<Long, Long>(), new java.util.ArrayList()));
        cachedDB.get(key).setMaxAuditId(getTableChecksum(db, tablename));
  }
    static CusPriceListItem getCustomPricelistItemsById(String sLogicalName2, Connection db, Long cusPricListItemId) {

        long lBegin = System.nanoTime();
        List<CusPriceListItem> list = new ArrayList<>();
        try {

            String Sql = "SELECT [ID] "
                    + " , PL_NEW_CUS_PriceListItem.[type] AS [Type] "
                    + " , PL_NEW_CUS_PriceListItem.code AS [Code]  "
                    + " , [price] "
                    + " , [discount] "
                    + " , [startDate] "
                    + " , [endDate] "
                  
                    + " , covered   ,PriceList_id  ,deleted FROM "
                    + " [PL_NEW_CUS_PriceListItem] "
                    + " where  "
                    + "   id = " + cusPricListItemId;

            try ( /*if(master.getID() == 486){
                     Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, Sql);
                     }*/ PreparedStatement ps = db.prepareStatement(Sql)) {
                ResultSet rs = ps.executeQuery();
                if (rs != null) {
                    while (rs.next()) {
                        Long ID = rs.getLong("ID");
                        Long PriceList_id = rs.getLong("PriceList_id");
                        Integer type = rs.getInt("Type");
                        String code = rs.getString("Code");
                        Double price = rs.getDouble("price");
                        Boolean deleted = rs.getBoolean("deleted");
                        Double discount = null;
                        if (rs.getObject("discount") != null) {
                            discount = rs.getDouble("discount");
                        }
                         
                        Date startDate = rs.getTimestamp("startDate");
                        Date endDate = rs.getTimestamp("endDate");
                        if (code == null) {
                            continue;
                        }
                         
                        CusPriceListItem item = new CusPriceListItem(ID, PriceList_id, code,
                                CodeType.from(type),
                                price, discount, startDate, endDate);
                       
                        item.setStatus(com.accumed.pricing.model.Status.VALID);
                        item.setDeleted(deleted);
                        list.add(item);
                    }
                    rs.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, sLogicalName2 + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
     }

    static CachedData getCustomPriceListsById(Connection db, long priceListId) {
       
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<String, Long>();
        HashMap<Long, Long> tablesIds = new HashMap<Long, Long>();
        List<CusPriceListItem> list = new ArrayList<CusPriceListItem>();
        try {

            tables.put("PL_NEW_CUS_PriceListItem", getTableChecksum(db, "PL_NEW_CUS_PriceListItem"));
//            tables.put("ACCUMED_INSURERS", getTableChecksum(db, "ACCUMED_INSURERS"));

            String Sql = "SELECT [ID] "
                    + " , PL_NEW_CUS_PriceListItem.[type] AS [Type] "
                    + " , PL_NEW_CUS_PriceListItem.code AS [Code]  "
                    + " , [price] "
                    + " , [discount] "
                    + " , [startDate] "
                    + " , [endDate] "
                    +"  ,PriceList_Id "
                    + "    FROM "
                    + " [PL_NEW_CUS_PriceListItem] "
                    + " where PriceList_Id =  " + priceListId;

            try ( /*if(master.getID() == 486){
                     Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, Sql);
                     }*/ PreparedStatement ps = db.prepareStatement(Sql)) {
                ResultSet rs = ps.executeQuery();
                if (rs != null) {
                    while (rs.next()) {
                        Long ID = rs.getLong("ID");
                        Integer type = rs.getInt("Type");
                        String code = rs.getString("Code");
                        Double price = rs.getDouble("price");
                        Long cusPricListId = rs.getLong("PriceList_Id");
                        Double discount = null;
                        if (rs.getObject("discount") != null) {
                            discount = rs.getDouble("discount");
                        }
                         

                        /*if(rs.wasNull()){
                         discount = null;
                         }*/
                        Date startDate = rs.getTimestamp("startDate");
                        Date endDate = rs.getTimestamp("endDate");
                        if (code == null) {
                            continue;
                        }
                        
                        CusPriceListItem item = new CusPriceListItem(ID, cusPricListId, code,
                                CodeType.from(type),
                                price, discount, startDate, endDate);
                        
                        item.setStatus(com.accumed.pricing.model.Status.VALID);
                        list.add(item);
                    }
                }
                rs.close();
            }

            //GetItems
            /*for (MasterPriceList masterPriceList : list) {
             masterPriceList.setItems(getMasterPricelistItems(db, masterPriceList));
             }*/
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        String logicalName = "PL_CUS_PL|" + priceListId;
        CachedData cashDataObject = new CachedData(logicalName, tables, new HashMap<Long, Long>(), new java.util.ArrayList(), new HashMap<String, CusPriceListItem>());
        for (CusPriceListItem item : list) {
            cashDataObject.getDataMap().put(item.getCode(), item);
            cashDataObject.getData().add(item);
        }
        return cashDataObject;
    }

    public static CusContract getCustomContractById(Connection db, String logicalName, long contractId) {
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<String, Long>();
        List<CusContract> list = new ArrayList<CusContract>();
        try {

            tables.put("PL_CUS_Contract", getTableChecksum(db, "PL_CUS_Contract"));
//            tables.put("ACCUMED_INSURERS", getTableChecksum(db, "ACCUMED_INSURERS"));

            String Sql = "SELECT CONTRACT.[ID],CONTRACT.[priceListId],CONTRACT.[insurer_license],CONTRACT.[facility_license],CONTRACT.[package_name]"
                    + ",CONTRACT.[clinician_license],CONTRACT.[startDate] "
                    + " ,CONTRACT.[endDate],CONTRACT.[approved],CONTRACT.[deleted],CONTRACT.[regulator_id],CONTRACT.[PHARM_DISCOUNT],CONTRACT.[IP_DISCOUNT]"
                    + ",CONTRACT.[OP_DISCOUNT],CONTRACT.[BASE_RATE] "
                    + " ,CONTRACT.[GAP],CONTRACT.[MARGINAL],IP_DRG_Factor,DayCase_DRG_Factor, "
                    + " IsNull((Select 1 from PL_CUS_PriceList where PL_CUS_PriceList.ID=CONTRACT.priceListId and PL_CUS_PriceList.priceListType='Dental'), 0) As isDental"
                    + "  ,CONTRACT. multipleProc,CONTRACT. primaryProc, CONTRACT.secondaryProc, CONTRACT.thirdProc,CONTRACT. forthProc ,CONTRACT.[IP_PHARM_DISCOUNT],CONTRACT.[OP_PHARM_DISCOUNT]   "
                    + ",ltrim(rtrim(CONTRACT.policy_name )) as policy_name,ltrim(rtrim(CONTRACT.class_name)) as class_name ,CONTRACT.ip_Copayment,CONTRACT.op_Copayment,CONTRACT.IP_MAX_PATIENT_SHARE,CONTRACT.OP_MAX_PATIENT_SHARE  , CONTRACT.ROOM_LIMIT, CONTRACT.PRIOR_APPROVAL_LIMIT,CONTRACT.ROOM_TYPE ,CONTRACT.COMPANY_CODE, ISNULL( INS.IP_SUSPENSION,CONTRACT.IP_SUSPENSION) AS IP_SUSPENSION,ISNULL( INS.OP_SUSPENSION,CONTRACT.OP_SUSPENSION) AS OP_SUSPENSION  ,HCSPCS_markup"
                    + " FROM [PL_CUS_Contract] AS CONTRACT LEFT JOIN ACCUMED_INSURERS AS INS ON CONTRACT.insurer_license= INS.AUTH_NO"
                    + " where CONTRACT.[approved] =1 "
                    + " and CONTRACT.id=" + contractId + " ";
            //Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, Sql);
            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Long ID = rs.getLong("ID");
                    Long priceListId = rs.getLong("priceListId");
                    String insurerLicense = rs.getString("insurer_license");
                    String facilityLicense = rs.getString("facility_license");
                    String packageName = rs.getString("package_name");
                    String clinicianLicense = rs.getString("clinician_license");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");
                    Boolean approved = rs.getBoolean("approved");
                    Boolean deleted = rs.getBoolean("deleted");
                    Integer regulator = rs.getInt("regulator_id");
                    Double PHARM_DISCOUNT = rs.getObject("PHARM_DISCOUNT") == null? null : rs.getDouble("PHARM_DISCOUNT");
                    Double IP_DISCOUNT = rs.getObject("IP_DISCOUNT") == null? null : rs.getDouble("IP_DISCOUNT");
                    Double OP_DISCOUNT = rs.getObject("OP_DISCOUNT") == null? null : rs.getDouble("OP_DISCOUNT");
                    Double BASE_RATE = rs.getObject("BASE_RATE") == null? null : rs.getDouble("BASE_RATE");
                    Double GAP = rs.getObject("GAP") == null? null : rs.getDouble("GAP");
                    Double MARGINAL = rs.getObject("MARGINAL") == null? null : rs.getDouble("MARGINAL");
                    Integer isDental = rs.getObject("isDental") == null? null : rs.getInt("isDental");
                    Integer multipleProc = rs.getObject("multipleProc") == null? null : rs.getInt("multipleProc");
                    Double primaryProc = rs.getObject("primaryProc") == null? null : rs.getDouble("primaryProc");
                    Double secondaryProc = rs.getObject("secondaryProc") == null? null : rs.getDouble("secondaryProc");
                    Double thirdProc = rs.getObject("thirdProc")== null? null : rs.getDouble("thirdProc");
                    Double forthProc = rs.getObject("forthProc") == null? null : rs.getDouble("forthProc");
                    Double hspcsMarkUp=rs.getObject("HCSPCS_markup") == null ? null : rs.getDouble("HCSPCS_markup");
                    Double ip_drg_factor=rs.getObject("IP_DRG_Factor") == null ? null : rs.getDouble("IP_DRG_Factor");
                     Double dayCase_drg_factor=rs.getObject("DayCase_DRG_Factor") == null ? null : rs.getDouble("DayCase_DRG_Factor");
                    CusContract cusContract = new CusContract(ID, priceListId,
                            insurerLicense, facilityLicense, packageName,
                            clinicianLicense, startDate, endDate,
                            approved, deleted, PHARM_DISCOUNT,
                            IP_DISCOUNT, OP_DISCOUNT, BASE_RATE, regulator,
                            GAP, MARGINAL, isDental,
                            multipleProc, primaryProc,
                            secondaryProc, thirdProc, forthProc,ip_drg_factor,dayCase_drg_factor,hspcsMarkUp );
                    
                    cusContract.setStatus(com.accumed.pricing.model.Status.VALID);
                    
                    list.add(cusContract);
                }
                rs.close();
            }

            ps.close();

            //GetItems
            /*for (MasterPriceList masterPriceList : list) {
             masterPriceList.setItems(getMasterPricelistItems(db, masterPriceList));
             }*/
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return list.get(0);
    }

    static MasterPriceListItem getSPCPricelistItemsById(String logicalName, Connection db, long pricelistItemId) {
       

        long lBegin = System.nanoTime();
        List<MasterPriceListItem> list = new ArrayList<>();
        try {

            String Sql = "SELECT [ID], "
                    + "CASE	WHEN CPT_code IS NOT NULL THEN  3  "
                    + "		WHEN HCPCS_code IS NOT NULL THEN 4  "
                    + "		WHEN TradeDrug_code IS NOT NULL THEN 5  "
                    + "		WHEN Dental_code IS NOT NULL THEN 6  "
                    + "		WHEN Service_code IS NOT NULL THEN 8  "
                    + "		WHEN IRDrug_code IS NOT NULL THEN 9  "
                    + "		WHEN GenericDrug_code IS NOT NULL THEN 10  "
                    + "END AS [Type], "
                    + "CASE	WHEN CPT_code IS NOT NULL THEN  CPT_code  "
                    + "		WHEN HCPCS_code IS NOT NULL THEN HCPCS_code  "
                    + "		WHEN TradeDrug_code IS NOT NULL THEN TradeDrug_code  "
                    + "		WHEN Dental_code IS NOT NULL THEN Dental_code "
                    + "		WHEN Service_code IS NOT NULL THEN Service_code "
                    + "		WHEN IRDrug_code IS NOT NULL THEN IRDrug_code "
                    + "		WHEN GenericDrug_code IS NOT NULL THEN GenericDrug_code "
                    + "END AS [Code] "
                    + " ,[price] ,[startDate] ,[endDate] ,[anaesthesiaBaseUnits], MasterPriceList_id "
                    + " From [PL_SPC_MasterPriceListItem] " 
            + " where  id =  " + pricelistItemId;

            try ( /*if(master.getID() == 486){
                     Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, Sql);
                     }*/ PreparedStatement ps = db.prepareStatement(Sql)) {
                ResultSet rs = ps.executeQuery();
                if (rs != null) {
                    while (rs.next()) {
                       
                    Long ID = rs.getLong("ID");
                    Integer type = rs.getInt("Type");
                    String code = rs.getString("Code");
                    Double price = rs.getDouble("price");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");
                    Integer anaesthesiaBaseUnits = rs.getInt("anaesthesiaBaseUnits");
                    Long MasterPriceList_id = rs.getLong("MasterPriceList_id");

                    MasterPriceListItem item = new MasterPriceListItem(ID, MasterPriceList_id, code,
                            CodeType.from(type),
                            price, startDate, endDate, anaesthesiaBaseUnits);
                    list.add(item);
                
                      
                    }
                    rs.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName  + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
     
    }

    static PackageItemCode getRCMPackageItemsById(String logicalName, Connection db, Long id) {
       

        long lBegin = System.nanoTime();
        List< PackageItemCode> list = new ArrayList<>();
        try {

            String Sql = "select  PackageGroupID,ActivityType,ActivityCode,NetAmount from RCM_Package_Item where  IsActive=1 and    PackageItemID="+id;
            try (  
			PreparedStatement ps = db.prepareStatement(Sql)) {
                ResultSet rs = ps.executeQuery();
                if (rs != null) {
                    while (rs.next()) {
                      int type=rs.getInt("ActivityType");
                     Double price = rs.getDouble("NetAmount");
                    String    code=rs.getString("ActivityCode");
                    int packageID=rs.getInt("PackagegroupID");
                    PackageItemCode item = new PackageItemCode(code,type,price,packageID);                     
                    
                    list.add(item);
                
                      
                    }
                    rs.close();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, logicalName  + " loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
     
    }

    static CachedData getSPCPriceListsById(Connection db, Long  Id) {
       
       
        long lBegin = System.nanoTime();
        HashMap<String, Long> tables = new HashMap<>();
        List<SPCContract> list = new ArrayList<>();
        try {
            tables.put("PL_SPC_PriceList", getTableChecksum(db, "PL_SPC_PriceList"));
            String Sql = "SELECT [ID] ,[type] ,[insurer_license] ,[facility_license] ,[package_name] ,[clinician_license] ,[startDate] "
                    + "      ,[endDate] ,[factor] ,[approved] ,[deleted] ,[parentId] ,[PHARM_DISCOUNT] ,[IP_DISCOUNT],[OP_DISCOUNT] "
                    + "      ,[BASE_RATE] ,[regulator_id] ,[GAP] ,[MARGINAL]  FROM [PL_SPC_PriceList]"
                    + " Where deleted = 0 AND approved = 1 and ID="+ Id;

            PreparedStatement ps = db.prepareStatement(Sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    Long ID = rs.getLong("ID");
                    Integer type = rs.getInt("type");
                    String insurerLicense = rs.getString("insurer_license");
                    String facilityLicense = rs.getString("facility_license");
                    String packageName = rs.getString("package_name");
                    String clinicianLicense = rs.getString("clinician_license");
                    Date startDate = rs.getTimestamp("startDate");
                    Date endDate = rs.getTimestamp("endDate");
                    Double factor = rs.getDouble("factor");
                    Boolean approved = rs.getBoolean("approved");
                    Boolean deleted = rs.getBoolean("deleted");
                    Integer parentId = rs.getInt("parentId");
                    Double PHARM_DISCOUNT = rs.getDouble("PHARM_DISCOUNT");
                    Double IP_DISCOUNT = rs.getDouble("IP_DISCOUNT");
                    Double OP_DISCOUNT = rs.getDouble("OP_DISCOUNT");
                    Double BASE_RATE = rs.getDouble("BASE_RATE");
                    Integer regulator = rs.getInt("regulator_id");
                    Double GAP = rs.getDouble("GAP");
                    Double MARGINAL = rs.getDouble("MARGINAL");

                    SPCContract spcContract = new SPCContract(ID, type,
                            insurerLicense, facilityLicense, packageName,
                            clinicianLicense, startDate, endDate, factor,
                            approved, deleted, parentId, PHARM_DISCOUNT,
                            IP_DISCOUNT, OP_DISCOUNT, BASE_RATE, regulator,
                            GAP, MARGINAL);
                    list.add(spcContract);
                }
                rs.close();
                rs = null;
            }
            ps.close();
            ps = null;

        } catch (SQLException ex) {
            Logger.getLogger(RepoUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(RepoUtils.class.getName()).log(Level.INFO, "spcContracts  loading time is {0}ms.",
                (new Long((System.nanoTime() - lBegin) / 1000000)).toString());
        return new CachedData("spcContracts", tables, list);
    
    }

     
   
    }

    

  
 
