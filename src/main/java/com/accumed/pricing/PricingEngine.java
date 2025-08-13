/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing;

import com.accumed.pricing.cachedRepo.CachedRepository;
import com.accumed.pricing.cachedRepo.CachedRepositoryService;
import com.accumed.pricing.model.Severity;
import com.accumed.pricing.model.request.Activity;
import com.accumed.pricing.model.request.CodeType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jws.WebService;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import com.accumed.pricing.model.request.Claim;
import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * @author smutlak
 */
@WebService(serviceName = "PricingEngine", targetNamespace = "http://pricing.accumed.com/")
public class PricingEngine {

    protected static AccountantPool accountantPool = new AccountantPool(true);
    public static final int POOL_MIN_COUNT = 1;
    public static final int POOL_MAX_COUNT = 4;
    protected static CachedRepositoryService cachedRepositoryService = null; 

    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");

    protected static PricingLog minRequest;
    protected static PricingLog maxRequest;
    protected static Long averageClaimPricingTime;
    protected static Long lastRequestTime;

    protected static long totalRequests;
    protected static long totalPricedClaims;
    protected static long totalPricedClaimsTime;
    protected static PricingLogger pricingLogger;
    private static final Logger LOGGER = Logger.getLogger(PricingEngine.class.getName());

    public static void setCachedRepositoryService(CachedRepositoryService cachedRepositoryService) {
        PricingEngine.cachedRepositoryService = cachedRepositoryService;
        Initialize();
    }
    
  

    private static void Initialize() {
        CachedRepository repo = PricingEngine.cachedRepositoryService.getRepo();
        try {
            java.util.ArrayList<Accountant> array = new java.util.ArrayList();
            accountantPool.setSynchronized(false);

            int curr = accountantPool.getValidCount(true);
            if (curr < POOL_MIN_COUNT) {
                int required = POOL_MIN_COUNT - curr;
                for (int i = 0; i < required; i++) {
                    array.add(accountantPool.checkOut(repo));
                }
                for (Accountant accountant : array) {
                    accountantPool.checkIn(accountant);
                }
            }
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }

    }

    private boolean validClaim(Claim claim) {
        //claim
        boolean ret = claim != null;

        if (!ret) {
            LOGGER.log(Level.SEVERE, "Exception: Invalid claim, returning null response.");
            return ret;
        }

        //contract and package name
        ret = ret && claim.getContract() != null && claim.getContract().getPackageName() != null
                && !claim.getContract().getPackageName().trim().isEmpty();

        if (!ret) {
            LOGGER.log(Level.SEVERE, "Exception: Invalid claim, Invalid contract or package name, returning null response.");
            return ret;
        }

        //receiver 
        ret = ret && claim.getReceiverID() != null && !claim.getReceiverID().trim().isEmpty();
        if (!ret) {
            LOGGER.log(Level.SEVERE, "Exception: Invalid claim, Invalid receiver id, returning null response.");
            return ret;
        }

        //facility
        ret = ret && claim.getProviderID() != null && !claim.getProviderID().trim().isEmpty();
        if (!ret) {
            LOGGER.log(Level.SEVERE, "Exception: Invalid claim, Invalid provider id, returning null response.");
            return ret;
        }
        ret = ret && claim.getActivity() != null && !claim.getActivity().isEmpty();
        if (!ret) {
            LOGGER.log(Level.SEVERE, "Exception: Invalid claim, no activities, returning null response.");
            return ret;
        }
        return ret;
    }

    private void resetPriceData(Claim claim) {
        claim.setSPC_ID(null);
        claim.setCUS_ID(null);
        claim.setNet(null);
        claim.setGross(null);
        claim.setPatientShare(null);
        claim.setOutcome(null);
        claim.setMultipleProcedures(null);
        for (com.accumed.pricing.model.request.Activity act : claim.getActivity()) {
            act.setDiscount(null);
            act.setSPCFactor(null);
            if (act.getCustom_Price_Types() == null || !act.getCustom_Price_Types().contains("3")) {
                act.setList(null);
            }
            act.setDiscountPercentage(null);
            act.setDeductible(null);
            act.setCopayment(null);
            act.setGross(null);
            act.setNet(null);
            act.setOutcome(null);
            act.setPatientShare(null);
            act.setAnaesthesiaBaseUnits(null);
            act.setActivityGroup(null);
            act.setListPricePredifined(null);
        }
    }

    @WebMethod(operationName = "Price")
    public Claim Price(@WebParam(name = "Claim", targetNamespace = "http://pricing.accumed.com/") Claim claim,
            @WebParam(name = "logRequest") Boolean logRequest) {

        Logger.getLogger(PricingEngine.class
                .getName()).log(Level.INFO, "Pricing start");
        PricingEngine.totalRequests++;

        long lBegin = System.currentTimeMillis();
        long lStart = lBegin;
        if (cachedRepositoryService != null) {
            cachedRepositoryService.refreshRepository(claim.getReceiverID(), claim.getProviderID(), claim.getContract().getPackageName());
        }
        if (claim == null || !validClaim(claim)) {
            return null;
        }

        //Enable Logging
        claim.setLogInfo(LoggingManager.getInstance().isLogInfoEnabled());
        //End Enable Logging

        resetPriceData(claim);
        Connection dbConn = null;
        DAO dao = null;
        Accountant accountant = null;
        CachedRepository repo = null;
        boolean logRequestResponse = LoggingManager.getInstance().isLogRequest();
        if (logRequestResponse || logRequest) {

            Utils.saveRequest(claim, false, logRequestResponse);
        } 
        else {
            Utils.saveRequest(claim, true, logRequestResponse);
        }
        

        /* Using:
         1- ENCOUNTER_END_DATE match with SPC Master Price List start & end date
         1- ENCOUNTER_END_DATE match with SPC Master Price List start & end date
         */
        try {
            lBegin = printPeriod(lBegin, "get CachedRepository", logRequest);
            if (cachedRepositoryService == null || cachedRepositoryService.getRepo() == null) {
                Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, null, "Cached Repository is not available yet!! Cancelling request");
                return null;
            }
            if (PricingEngine.accountantPool.getValidCount(true) < PricingEngine.POOL_MIN_COUNT) {
                Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, null, "Accountants are not ready, try again after few seconds.");
                return null;
            }
            if (pricingLogger == null) {
                Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, null, "Pricing logger is not initialized.");
                return null;
            }
            repo = PricingEngine.cachedRepositoryService.getRepo();// repo.getCachedDB().get("spcContracts").getData();

            lBegin = printPeriod(lBegin, "Load custom contracts", logRequest);
            boolean bCusContractsLoaded = PricingEngine.cachedRepositoryService.loadCustomContracts(
                    claim.getReceiverID(), claim.getProviderID()/*,
             claim.getContract().getPackageName()*/);
            lBegin = printPeriod(lBegin, "Load DHA DRG", logRequest);
           boolean bDHA_DRGLoaded = loadAllDrgObjects();
//            lBegin = printPeriod(lBegin, "Load Custome Codes", logRequest);             
//            boolean bCustomeCodesLoaded= PricingEngine.cachedRepositoryService.loadCustomcodes();
//            lBegin = printPeriod(lBegin, "Load Package group Codes", logRequest);             
//            boolean bPackageGroupsLoaded= PricingEngine.cachedRepositoryService.loadPackageGroups();
//             lBegin = printPeriod(lBegin, "Load Package group Codes", logRequest);             
//            boolean bPackageItemLoaded= PricingEngine.cachedRepositoryService.loadPackageItems();
            lBegin = printPeriod(lBegin, "Start Load facility clinicians", logRequest);
            boolean bCliniciansLoaded = PricingEngine.cachedRepositoryService.loadFacilityClinicians(
                    claim.getProviderID());

            lBegin = printPeriod(lBegin, "DB connection", logRequest);
            dbConn = DBConnectionManager.getPriceDB();
            dao = new DAO(dbConn);

            lBegin = printPeriod(lBegin, "DAO connection", logRequest);
//            dao = new DAO(conn);

            lBegin = printPeriod(lBegin, "Checkout accountant.", logRequest);
            //we need to use bLoaded in the checked out accountant
            accountantPool.expireAndRemove(POOL_MAX_COUNT);
            accountant = accountantPool.checkOut_newest(repo);
            Logger.getLogger(PricingEngine.class
                    .getName()).log(Level.INFO, "*** Get Accountant ID={0}", accountant.getId());
            if (bCusContractsLoaded || bCliniciansLoaded || bDHA_DRGLoaded //||bCustomeCodesLoaded ||bPackageGroupsLoaded||bPackageItemLoaded 
                    ) {
                Logger.getLogger(PricingEngine.class
                        .getName()).log(Level.INFO, "Load custome contracts , clinicians , DHA DRGs, Package Groups,Package Items, Custom codes info for Accountant ID={0}",
                                claim.getReceiverID() + " "
                                + claim.getProviderID() + " "
                                + accountant.getId());
                accountantPool.setSynchronized(false);
          
                Logger.getLogger(PricingEngine.class
                        .getName()).log(Level.INFO, "priceMethod Re-synchronize accountant ID={0}", accountant.getId());
                claim.addOutcome(Severity.INFO, "Loading", "Loading Facility contracts, clinicians , DRG info , Package Groups,Package Items, Custom codes", "");
            }

            lBegin = printPeriod(lBegin, "accountant pricing.", logRequest);
            claim = accountant.price(dao, claim);

            lBegin = printPeriod(lBegin, "accountant pricing time.", logRequest);
            Logger.getLogger(PricingEngine.class
                    .getName()).log(Level.INFO, "*** Return Accountant ID=" + accountant.getId());
            accountantPool.checkIn(accountant);
            accountant = null;

            long takenTime = System.currentTimeMillis() - lStart;
            Logger.getLogger(PricingEngine.class
                    .getName()).log(Level.INFO, "Accountant Pricing Taken time=" + takenTime + ", For claim=" + claim.getIdCaller());

            PricingEngine.lastRequestTime = takenTime;
            if (bCusContractsLoaded == false && bCliniciansLoaded == false) {
                PricingEngine.totalPricedClaims++;
                PricingEngine.totalPricedClaimsTime += takenTime;
                averageClaimPricingTime = totalPricedClaimsTime / totalPricedClaims;
            }

            if (PricingEngine.minRequest == null) {
                PricingEngine.minRequest = new PricingLog(claim.getIdCaller(),
                        takenTime, lStart);
                PricingEngine.maxRequest = new PricingLog(claim.getIdCaller(),
                        takenTime, lStart);
            } else {
                if (takenTime < PricingEngine.minRequest.getPeriodInMilli()) {
                    if (claim == null) {
                        Logger.getLogger(PricingEngine.class
                                .getName()).log(Level.INFO, "Sameer*****claim is null");
                    }
                    PricingLog log = new PricingLog(claim.getIdCaller(),
                            takenTime, lStart);
                    if (log == null) {
                        Logger.getLogger(PricingEngine.class
                                .getName()).log(Level.INFO, "Sameer*****log is null");
                    }
                    PricingEngine.minRequest = log;
                }
                if (takenTime > PricingEngine.maxRequest.getPeriodInMilli()) {
                    PricingEngine.maxRequest = new PricingLog(claim.getIdCaller(),
                            takenTime, lStart);
                }
            }

            PricingEngine.pricingLogger.add(new PricingLog(claim.getIdCaller(),
                    takenTime, lStart));

            if (logRequestResponse || logRequest) {
                SaveThread saveThread = new SaveThread(claim);
                saveThread.setPriority(Thread.MIN_PRIORITY);
                saveThread.start();
            }
            Logger.getLogger(PricingEngine.class
                    .getName()).log(Level.INFO, "ret claim");
            return claim;
        } finally {
            if (accountant != null) {
                accountantPool.checkIn(accountant);
            }
            dao = null;
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, null, ex);
                }
                dbConn = null;
            }
        }

    }
//
//
//    private Connection getPriceDB() {
//        DataSource ds = null;
//        Connection con = null;
//        try {
//            Context initCtx = new InitialContext();
//            Context envCtx = (Context) initCtx.lookup("java:comp/env");
//            ds = (DataSource) envCtx.lookup("jdbc/pricingDB");
//            con = ds.getConnection();
//            if (con.getTransactionIsolation() != Connection.TRANSACTION_READ_UNCOMMITTED) {
//                Logger.getLogger(PricingEngine.class
//                        .getName()).log(Level.SEVERE, "DB connection is NOT READ_UNCOMMITTED.");
//            } else {
//                Logger.getLogger(PricingEngine.class
//                        .getName()).log(Level.INFO, "DB connection is READ_UNCOMMITTED.");
//            }
//
//        } catch (NamingException ex) {
//            Logger.getLogger(PricingEngine.class
//                    .getName()).log(Level.SEVERE, null, ex);
//        } catch (SQLException ex) {
//            Logger.getLogger(PricingEngine.class
//                    .getName()).log(Level.SEVERE, null, ex);
//        }
//        return con;
//
//    }

    private long printPeriod(long lstart, String message, boolean logRequest) {
        if (!logRequest) {
            return lstart;
        }
        long curr = System.currentTimeMillis();
        Logger.getLogger(PricingEngine.class
                .getName()).log(Level.INFO, curr - lstart + " ms "
                        + message);
        return curr;
    }

    public static final AccountantPool getAccountantPool() {
        return accountantPool;
    }

    @WebMethod(operationName = "getBaseRate")
    public Double getBaseRate(
            @WebParam(name = "insurerLicense") String insurerLicense,
            @WebParam(name = "facilityLicense") String facilityLicense,
            @WebParam(name = "packageName") String packageName,
            @WebParam(name = "date") String date)
            throws InvalidArgument {
        if (date == null || date.isEmpty() || !date.matches("^(((0[1-9]|[12]\\d|3[01])\\-(0[13578]|1[02])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|[12]\\d|30)\\-(0[13456789]|1[012])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|1\\d|2[0-8])\\-02\\-((19|[2-9]\\d)\\d{2}))|(29\\-02\\-((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))))$")) {
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }
        Date effDate;
        Integer regulatorID = new Integer(1);
        try {
            effDate = formatter.parse(date);
        } catch (ParseException ex) {
            Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, null, ex);
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }

        if (insurerLicense == null || insurerLicense.isEmpty()) {
            throw new InvalidArgument("Invalid insurer license.");
        }

        if (facilityLicense == null || facilityLicense.isEmpty()) {
            throw new InvalidArgument("Invalid facility license.");
        }

        /*if (packageName == null || packageName.isEmpty()) {
         throw new InvalidArgument("Invalid package name.");
         }*/
        if (packageName != null && packageName.trim().isEmpty()) {
            packageName = null;
        }
        List<PLContract> contracts = this.getContracts(insurerLicense, facilityLicense,
                packageName, effDate, effDate);
        if (packageName == null && !contracts.isEmpty()) {
            return contracts.get(0).getBaseRate() == null ? (double) 0 : contracts.get(0).getBaseRate();
        } else {
            for (PLContract contract : contracts) {
                if (contract.getPackageName() != null
                        && contract.getPackageName().equalsIgnoreCase(packageName)) {
                    return contract.getBaseRate() == null ? (double) 0 : contract.getBaseRate();
                }
            }
            //18 June 2018 -- if package contract is not found ... get baserate for contract with null package
            for (PLContract contract : contracts) {
                if (contract.getPackageName() == null) {
                    return contract.getBaseRate() == null ? (double) 0 : contract.getBaseRate();
                }
            }
            //End 18 June 2018
        }
        return 0d;
    }

    @WebMethod(operationName = "getGAP")
    public Double getGAP(
            @WebParam(name = "insurerLicense") String insurerLicense,
            @WebParam(name = "facilityLicense") String facilityLicense,
            @WebParam(name = "packageName") String packageName,
            @WebParam(name = "date") String date)
            throws InvalidArgument {
        if (date == null || date.isEmpty() || !date.matches("^(((0[1-9]|[12]\\d|3[01])\\-(0[13578]|1[02])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|[12]\\d|30)\\-(0[13456789]|1[012])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|1\\d|2[0-8])\\-02\\-((19|[2-9]\\d)\\d{2}))|(29\\-02\\-((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))))$")) {
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }
        Date effDate;
        Integer regulatorID = new Integer(1);
        try {
            effDate = formatter.parse(date);
        } catch (ParseException ex) {
            Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, null, ex);
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }

        if (insurerLicense == null || insurerLicense.isEmpty()) {
            throw new InvalidArgument("Invalid insurer license.");
        }

        if (facilityLicense == null || facilityLicense.isEmpty()) {
            throw new InvalidArgument("Invalid facility license.");
        }

        /*if (packageName == null || packageName.isEmpty()) {
         throw new InvalidArgument("Invalid package name.");
         }*/
        if (packageName != null && packageName.trim().isEmpty()) {
            packageName = null;
        }

        List<PLContract> contracts = this.getContracts(insurerLicense, facilityLicense,
                packageName, effDate, effDate);
        if (packageName == null && !contracts.isEmpty()) {
            return contracts.get(0).getGap() == null ? new Double(0) : contracts.get(0).getGap();
        } else {
            for (PLContract contract : contracts) {
                if (contract.getPackageName() != null
                        && contract.getPackageName().equalsIgnoreCase(packageName)) {
                    return contract.getGap() == null ? new Double(0) : contract.getGap();
                }
            }
        }
        return 0d;
    }

    @WebMethod(operationName = "getMarginal")
    public Double getMarginal(
            @WebParam(name = "insurerLicense") String insurerLicense,
            @WebParam(name = "facilityLicense") String facilityLicense,
            @WebParam(name = "packageName") String packageName,
            @WebParam(name = "date") String date)
            throws InvalidArgument {
        if (date == null || date.isEmpty() || !date.matches("^(((0[1-9]|[12]\\d|3[01])\\-(0[13578]|1[02])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|[12]\\d|30)\\-(0[13456789]|1[012])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|1\\d|2[0-8])\\-02\\-((19|[2-9]\\d)\\d{2}))|(29\\-02\\-((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))))$")) {
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }
        Date effDate;
        Integer regulatorID = new Integer(1);
        try {
            effDate = formatter.parse(date);
        } catch (ParseException ex) {
            Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, null, ex);
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }

        if (insurerLicense == null || insurerLicense.isEmpty()) {
            throw new InvalidArgument("Invalid insurer license.");
        }

        if (facilityLicense == null || facilityLicense.isEmpty()) {
            throw new InvalidArgument("Invalid facility license.");
        }

        /*if (packageName == null || packageName.isEmpty()) {
         throw new InvalidArgument("Invalid package name.");
         }*/
        if (packageName != null && packageName.trim().isEmpty()) {
            packageName = null;
        }

        List<PLContract> contracts = this.getContracts(insurerLicense, facilityLicense,
                packageName, effDate, effDate);
        if (packageName == null && !contracts.isEmpty()) {
            return contracts.get(0).getMarginal() == null ? new Double(0) : contracts.get(0).getMarginal();
        } else {
            for (PLContract contract : contracts) {
                if (contract.getPackageName() != null
                        && contract.getPackageName().equalsIgnoreCase(packageName)) {
                    return contract.getMarginal() == null ? new Double(0) : contract.getMarginal();
                }
            }
        }
        return 0d;
    }

    @WebMethod(operationName = "isContractExists")
    public boolean isContractExists(
            @WebParam(name = "insurerLicense") String insurerLicense,
            @WebParam(name = "facilityLicense") String facilityLicense,
            @WebParam(name = "packageName") String packageName,
            @WebParam(name = "date") String date,
            @WebParam(name = "ignorePackage") Boolean ignorePackage)
            throws InvalidArgument {

        if (date == null || date.isEmpty() || !date.matches("^(((0[1-9]|[12]\\d|3[01])\\-(0[13578]|1[02])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|[12]\\d|30)\\-(0[13456789]|1[012])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|1\\d|2[0-8])\\-02\\-((19|[2-9]\\d)\\d{2}))|(29\\-02\\-((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))))$")) {
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }
        Date effDate;
        try {
            effDate = formatter.parse(date);
        } catch (ParseException ex) {
            Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, null, ex);
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }

        if (insurerLicense == null || insurerLicense.isEmpty()) {
            throw new InvalidArgument("Invalid insurer license.");
        }

        if (facilityLicense == null || facilityLicense.isEmpty()) {
            throw new InvalidArgument("Invalid facility license.");
        }

        if (packageName != null && packageName.trim().isEmpty()) {
            packageName = null;
        }

        List<PLContract> contracts = this.getContracts(insurerLicense, facilityLicense,
                packageName, effDate, effDate);
        if (ignorePackage) {
            return !contracts.isEmpty();
        } else {
            for (PLContract contract : contracts) {
                if (contract.getPackageName() != null
                        && contract.getPackageName().equalsIgnoreCase(packageName)) {
                    return true;
                }
            }
        }

        return false;
    }

    @WebMethod(operationName = "getNegotiationDrgFactor")
    public Double getNegotiationDrgFactor(
            @WebParam(name = "insurerLicense") String insurerLicense,
            @WebParam(name = "facilityLicense") String facilityLicense,
            @WebParam(name = "packageName") String packageName,
            @WebParam(name = "date") String date)
            throws InvalidArgument {
        if (date == null || date.isEmpty() || !date.matches("^(((0[1-9]|[12]\\d|3[01])\\-(0[13578]|1[02])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|[12]\\d|30)\\-(0[13456789]|1[012])\\-((19|[2-9]\\d)\\d{2}))|((0[1-9]|1\\d|2[0-8])\\-02\\-((19|[2-9]\\d)\\d{2}))|(29\\-02\\-((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))))$")) {
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }
        Date effDate;
        Integer regulatorID = new Integer(1);
        try {
            effDate = formatter.parse(date);
        } catch (ParseException ex) {
            Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, null, ex);
            throw new InvalidArgument("Invalid Date, or date format is invalid.");
        }

        if (insurerLicense == null || insurerLicense.isEmpty()) {
            throw new InvalidArgument("Invalid insurer license.");
        }

        if (facilityLicense == null || facilityLicense.isEmpty()) {
            throw new InvalidArgument("Invalid facility license.");
        }

        /*if (packageName == null || packageName.isEmpty()) {
         throw new InvalidArgument("Invalid package name.");
         }*/
        if (packageName != null && packageName.trim().isEmpty()) {
            packageName = null;
        }
        List<PLContract> contracts = this.getContracts(insurerLicense, facilityLicense,
                packageName, effDate, effDate);
        if (packageName == null && !contracts.isEmpty()) {
            return contracts.get(0).getIP_DRG_Factor() == null ? (double) 0 : contracts.get(0).getIP_DRG_Factor();
        } else {
            for (PLContract contract : contracts) {
                if (contract.getPackageName() != null
                        && contract.getPackageName().equalsIgnoreCase(packageName)) {
                    return contract.getIP_DRG_Factor() == null ? (double) 0 : contract.getIP_DRG_Factor();
                }
            }
            //18 June 2018 -- if package contract is not found ... get baserate for contract with null package
            for (PLContract contract : contracts) {
                if (contract.getPackageName() == null) {
                    return contract.getIP_DRG_Factor() == null ? (double) 0 : contract.getIP_DRG_Factor();
                }
            }
            //End 18 June 2018
        }
        return 0d;
    }

    private List<PLContract> getContracts(String insurerLicense,
            String facilityLicense,
            String packageName,
            Date startDate,
            Date endDate) {

        Connection dbConn = null;
        try {
            dbConn = DBConnectionManager.getPriceDB();
            return com.accumed.pricing.cachedRepo.RepoUtils.getContracts(
                    dbConn, facilityLicense,
                    insurerLicense, startDate, endDate);

        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        } finally {
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                }
                dbConn = null;
            }
        }
        return null;
    }

    protected class SaveThread extends Thread {

        private Claim claim = null;

        public SaveThread(Claim claim1) {
            super();
            setPriority(Thread.MIN_PRIORITY);
            this.claim = claim1;
        }

        @Override
        public void run() {
            Utils.saveRequest(this.claim, true, LoggingManager.getInstance().isLogRequest());
        }
    }

    public static CachedRepositoryService getCachedRepositoryService() {
        return cachedRepositoryService;
    }

    public static PricingLog getMaxRequest() {
        return maxRequest;
    }

    public static Long getAverageClaimPricingTime() {
        return averageClaimPricingTime;
    }

    public static long getTotalRequests() {
        return totalRequests;
    }

    public static long getTotalPricedClaims() {
        return totalPricedClaims;
    }

    public static long getTotalPricedClaimsTime() {
        return totalPricedClaimsTime;
    }

    public static PricingLogger getPricingLogger() {
        return pricingLogger;
    }

    public static PricingLog getMinRequest() {
        return minRequest;
    }

    public static void setPricingLogger(PricingLogger logger) {
        PricingEngine.pricingLogger = logger;
    }

    public static Long getLastRequestTime() {
        return lastRequestTime;
    }

    public static void setLastRequestTime(Long lastRequestTime) {
        PricingEngine.lastRequestTime = lastRequestTime;
    }

    private boolean loadFilteredDrg(List<Activity> activitiesList) {
        //  lBegin = printPeriod(lBegin, "Load Drg related objects", logRequest);
        List<Activity> drgList = new ArrayList<>();
        List<Activity> cptDrugHcpcsList = new ArrayList<>();
        for (Activity a : activitiesList) {
            if (a.getType().equals(CodeType.CPT) || a.getType().equals(CodeType.HCPCS) || a.getType().equals(CodeType.TRADE_DRUG)) {
                cptDrugHcpcsList.add(a);

            }
            if (a.getType().equals(CodeType.IR_DRG)) {
                drgList.add(a);
            }
        }
        boolean bDRGExcludedCpts = false;
        boolean bDHA_DRG_HighCost = false;
        boolean bDHA_DRG_COST_PER_ACTIVITY = false;
        boolean bDHA_DRGLoaded = false;
        if (drgList.size() > 0) {
            bDHA_DRGLoaded = PricingEngine.cachedRepositoryService.loadDHA_DRG(drgList);

            if (cptDrugHcpcsList.size() > 0) {
                bDRGExcludedCpts = PricingEngine.cachedRepositoryService.loadDRGExcludedCpts(cptDrugHcpcsList);
                bDHA_DRG_HighCost = PricingEngine.cachedRepositoryService.loadDHA_DRG_HighCost(cptDrugHcpcsList);
                bDHA_DRG_COST_PER_ACTIVITY = PricingEngine.cachedRepositoryService.loadDHA_DRG_COST_PER_ACTIVITY(cptDrugHcpcsList);
            }
        }
        return (bDHA_DRGLoaded || bDRGExcludedCpts || bDHA_DRG_HighCost || bDHA_DRG_COST_PER_ACTIVITY);
    }

    private boolean loadAllDrgObjects() {
        PricingEngine.cachedRepositoryService.loadAllDHA_DRG();       
        PricingEngine.cachedRepositoryService.loadAllHAAD_DRG();
        PricingEngine.cachedRepositoryService.loadAllDRGExcludedCpts();
        PricingEngine.cachedRepositoryService.loadAllDHA_DRG_HighCost();
        PricingEngine.cachedRepositoryService.loadAllDHA_DRG_COST_PER_ACTIVITY();
        
        return true;
    }

}
