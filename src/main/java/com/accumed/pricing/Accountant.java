
package com.accumed.pricing;

import com.accumed.pricing.cachedRepo.CachedData;
import com.accumed.pricing.cachedRepo.CachedRepository;
import com.accumed.pricing.model.request.Activity;
import com.accumed.pricing.model.request.Claim;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.drools.KnowledgeBase;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.rule.FactHandle;


public class Accountant  {

    private boolean bSynchronized = false;
    private KnowledgeBase knowledgeBase = null;
    private long package_time = 0;
    private StatefulKnowledgeSession session = null;
//    private boolean valid = true;
    private boolean packageChanged = true;
    private static String package_fileName;
    private static Long loadingTime;
    private static Boolean isLoaded;
    private Date repoTimeStamp;

    static final AtomicLong NEXT_ID = new AtomicLong(0);
    final long id = NEXT_ID.getAndIncrement();

    static {
        String sRulesSource = System.getProperty("com.san.accountant_package");
        try {
            sRulesSource = (String) (new InitialContext().lookup("java:comp/env/com.san.accountant_package"));  // from Tomcat's server.xml
        } catch (NamingException ex) {
            sRulesSource = "D:/newPricePackages/accumed_accountant.pkg";
        }
        Logger.getLogger(Accountant.class
                .getName()).log(Level.INFO, "com.san.accountant_package=" + sRulesSource);
        package_fileName = sRulesSource;
    }

    public Accountant() {
        loadPackage(null);
        if (knowledgeBase != null) {
            this.session = knowledgeBase.newStatefulKnowledgeSession();
            if (this.session != null) {
                isLoaded = true;
            }
            // addSessionListener();
        } else {
            Logger.getLogger(Accountant.class.getName()).severe("Accumed Accountnat Knowledge Base is NULL");
        }
    }

    
    public long getId() {
        return id;
    }

    
    public void dispose() {
        if (this.session != null) {
            this.session.dispose();
            this.session = null;
        }
    }

    
    synchronized public boolean reInitialize(CachedRepository repo) {
        dispose();
        this.session = knowledgeBase.newStatefulKnowledgeSession();
        if (this.session != null) {
            isLoaded = true;
        }
        // addSessionListener();
        return this.initialize(repo);
    }

    
    synchronized public boolean initialize(CachedRepository repo) {
        long lBegin = System.nanoTime();
        if (session == null) {
            return false;
        }

        repoTimeStamp = repo.getTimeStamp();
        this.bSynchronized = true;
        for (Map.Entry<String, CachedData> entry : repo.getCachedDB().entrySet()) {
            CachedData cachedData = entry.getValue();
            Logger.getLogger(Accountant.class
                    .getName()).log(Level.INFO, "+++++++++++ Accountant ID=" + this.getId() + " Insert " + cachedData.getLogicalName() + " Into Working Memory, size=" + cachedData.getData().size());
            for (Object obj : cachedData.getData()) {
                try {
                    session.insert(obj);
                } catch (Exception e) {
                    Logger.getLogger(Accountant.class
                            .getName()).log(Level.SEVERE, "Exeption on load " + getId(), e.fillInStackTrace());
                }
            }
        }
        Logger.getLogger(Accountant.class
                .getName()).log(Level.INFO, "Accountant::initialize:ID=" + getId() + " " + (new Long((System.nanoTime() - lBegin) / 1000000)).toString() + "ms");
        return true;
    }
    
 
    
    public final boolean loadPackage(CachedRepository repo) {
        boolean reLoaded = false;
        Logger.getLogger(Accountant.class.getName()).entering(Accountant.class.getName(), "loadPackage");
        {
            java.io.File fFile = null;
            fFile = new java.io.File(package_fileName);
            if (fFile.exists()) {
                if (knowledgeBase == null) {
                    package_time = fFile.lastModified();
                    loadingTime = package_time;
                    knowledgeBase = Utils.createKnowledgeBase(package_fileName);
                    setPackageChanged(false);
                    reLoaded = true;
                } else if (package_time < fFile.lastModified()) {
                    knowledgeBase = null;
                    knowledgeBase = Utils.createKnowledgeBase(package_fileName);
                    package_time = fFile.lastModified();
                    loadingTime = package_time;
                    session = knowledgeBase.newStatefulKnowledgeSession();
                    if (this.session != null) {
                        isLoaded = true;
                    }
                    // addSessionListener();
                    initialize(repo);
                    reLoaded = true;
                }
            }
            fFile = null;

        }
        Logger.getLogger(Accountant.class.getName()).exiting(Accountant.class.getName(), "loadPackage");
        return reLoaded;
    }

   
    
    protected void finalize() throws Throwable {
        dispose();
        super.finalize();
    }

   
    public boolean isValid() {
        return true;
    }

//    public void setValid(boolean valid) {
//        this.valid = valid;
//    }
    
    public boolean isPackageChanged() {
        return packageChanged;
    }

    
    public void setPackageChanged(boolean packageChanged) {
        this.packageChanged = packageChanged;
    }

    protected final boolean isPackageModified() {
        Logger.getLogger(Accountant.class.getName()).entering(Accountant.class.getName(), "isPackageModified");
        boolean ret = false;
        {
            java.io.File fFile = null;
            fFile = new java.io.File(package_fileName);
            if (fFile.exists()) {
                if (knowledgeBase != null) {
                    if (package_time < fFile.lastModified()) {
                        setPackageChanged(true);
                        ret = true;
                    }
                }
            }
            fFile = null;

        }

        Logger.getLogger(Accountant.class.getName()).exiting(Accountant.class.getName(), "isPackageModified");
        return ret;
    }

    private void addSessionListener() {

       
        session.addEventListener(new org.drools.event.rule.DefaultAgendaEventListener() {

            
            @Override
            public void beforeActivationFired(org.drools.event.rule.BeforeActivationFiredEvent event) {
                Logger.getLogger(Accountant.class.getName()).info("beforeActivationFired=" + event.getActivation().getRule().getName() + "=" + System.nanoTime());
                super.beforeActivationFired(event); //To change body of generated methods, choose Tools | Templates.
            }

           
            @Override
            public void afterActivationFired(org.drools.event.rule.AfterActivationFiredEvent event) {
                Logger.getLogger(Accountant.class.getName()).info("afterActivationFired=" + event.getActivation().getRule().getName() + "=" + System.nanoTime());
                super.afterActivationFired(event); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void activationCreated(org.drools.event.rule.ActivationCreatedEvent event) {
                Logger.getLogger(Accountant.class.getName()).info("activationCreated=" + event.getActivation().getRule().getName() + "=" + System.nanoTime());
                super.activationCreated(event); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void activationCancelled(org.drools.event.rule.ActivationCancelledEvent event) {
                Logger.getLogger(Accountant.class.getName()).info("activationCancelled=" + event.getActivation().getRule().getName() + "=" + System.nanoTime());
                super.activationCancelled(event); //To change body of generated methods, choose Tools | Templates.
            }

        });

    }

    public static String getPackage_fileName() {
        return package_fileName;
    }

    public static Boolean getIsLoaded() {
        return isLoaded;
    }

    public static Long getLoadingTime() {
        return loadingTime;
    }

    private boolean executeRulesWithTimeout(final StatefulKnowledgeSession session, int timeoutSeconds, Claim claim) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(new Runnable() {
            @Override
            public void run() {
                session.fireAllRules();
            }
        });

        try {
            future.get(timeoutSeconds, TimeUnit.SECONDS); // Wait for execution
            Logger.getLogger(PricingEngine.class.getName()).log(Level.INFO, "Session Successfully executed");
            return true;  // Successfully executed
        } catch (TimeoutException e) {
            Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE,
                    "Rules execution timed out. Terminating execution for Claim : " + claim.getId().toString() + ", Claim Line ID : " + claim.getIdCaller());
            future.cancel(true);  // Cancel execution
            return false;
        } catch (Exception e) {
            Logger.getLogger(PricingEngine.class.getName()).log(Level.SEVERE, "Error executing rules", e);
            return false;
        } finally {
            executor.shutdownNow();
            Logger.getLogger(PricingEngine.class.getName()).log(Level.INFO, "Session Executor Shutdown");
        }
    }

    
    public Claim price(DAO dao,
            Claim claim) {
        Logger.getLogger(Accountant.class.getName()).log(Level.INFO, "Info::In Accountant::price");
        if (session == null) {
            return null;  //package is not loaded
        }

        Utils.deleteFacts(session, Claim.class);

        FactHandle dao_factHandle = null;
        FactHandle claim_factHandle = null;
        ArrayList<FactHandle> activites = new ArrayList();
        ArrayList<FactHandle> deductibles = new ArrayList();
        ArrayList<FactHandle> coPayments = new ArrayList();

//        Long patientInsuranceId = new Long(claim.getPatient().getPatientInsurance().getIdCaller());
        try {
            dao_factHandle = session.insert(dao);
            claim_factHandle = session.insert(claim);
            /////////////////added by  me  16 SEP to  solve  the  order of  activities      
            Integer leastValue = findMinActivityID(claim.getActivity());
            Integer counter = -1;

            if (leastValue != null && leastValue < counter) {
                counter = leastValue;
            }

            // Replace null ActivityIDs with decreasing negative values
            for (Activity act : claim.getActivity()) {
                if (act.getActivityID() == null) {
                    act.setActivityID(counter.toString());
                    counter--;
                }
            }
            List<Activity> positiveActs = new ArrayList<Activity>();
            for (Activity act : claim.getActivity()) {
                if (Integer.parseInt(act.getActivityID()) > 0) {
                    positiveActs.add(act);
                }
            }
            List<Activity> negativeActs = new ArrayList<Activity>();
            for (Activity act : claim.getActivity()) {
                if (Integer.parseInt(act.getActivityID()) < 0) {
                    negativeActs.add(act);
                }
            }
            // Sort activities by ActivityID in descending order put -1 , Ascending put 1
            Collections.sort(positiveActs, new Comparator<Activity>() {
                @Override
                public int compare(Activity p1, Activity p2) {
                    Integer id1 = Integer.parseInt(p1.getActivityID());
                    Integer id2 = Integer.parseInt(p2.getActivityID());
                    return 1 * id2.compareTo(id1);
                }
            });
            Collections.sort(negativeActs, new Comparator<Activity>() {
                @Override
                public int compare(Activity p1, Activity p2) {
                    Integer id1 = Integer.parseInt(p1.getActivityID());
                    Integer id2 = Integer.parseInt(p2.getActivityID());
                    return 1 * id2.compareTo(id1);
                }
            });
            // Combine negative acts into positive acts to  keep  order
            positiveActs.addAll(negativeActs);
///////////////// end  of  addition 

            for (com.accumed.pricing.model.request.Activity act : positiveActs) {
                activites.add(session.insert(act));
            }
            if (claim.getPatient().getPatientInsurance().getDeductible() != null) {
                for (com.accumed.pricing.model.request.Deductible ded : claim.getPatient().getPatientInsurance().getDeductible()) {
                    deductibles.add(session.insert(ded));
                }
            }
            if (claim.getPatient().getPatientInsurance().getCoPayment() != null) {
                for (com.accumed.pricing.model.request.CoPayment copay : claim.getPatient().getPatientInsurance().getCoPayment()) {
                    coPayments.add(session.insert(copay));
                }
            }

            //session.fireAllRules();
            int timeoutInSeconds = 200; // Set your desired timeout
            executeRulesWithTimeout(session, timeoutInSeconds, claim);
         

        } catch (org.drools.runtime.rule.ConsequenceException e) {
//            Statistics.addException(e, req);
            Logger.getLogger(Accountant.class.getName()).log(Level.INFO, "Info::org.drools.runtime.rule.ConsequenceException");
            Logger.getLogger(Accountant.class.getName()).log(Level.SEVERE, e.getMessage() + "In Rule:" + e.getRule(), e);
//            e.printStackTrace();
            throw e;
        } catch (org.drools.RuntimeDroolsException e) {
//            Statistics.addException(e, req);
            Logger.getLogger(Accountant.class.getName()).log(Level.INFO, "Info::org.drools.RuntimeDroolsException");
            Logger.getLogger(Accountant.class.getName()).log(Level.SEVERE, e.getMessage(), e);
            throw e;
        } //        catch (javax.ejb.EJBTransactionRolledbackException e) {
        ////            Statistics.addException(e, req);
        //            Logger.getLogger(Accountant.class.getName()).log(Level.SEVERE, e.getMessage(), e);
        //
        //            throw e;
        //        }
        catch (Exception e) {
//            Statistics.addException(e, req);

            Logger.getLogger(Accountant.class.getName()).log(Level.INFO, "Info::Exception");
            if (e instanceof org.drools.runtime.rule.ConsequenceException) {
                org.drools.runtime.rule.ConsequenceException conEx = (org.drools.runtime.rule.ConsequenceException) e;
                Logger.getLogger(Accountant.class.getName()).log(Level.INFO, "Info::org.drools.runtime.rule.ConsequenceException");
                Logger.getLogger(Accountant.class.getName()).log(Level.SEVERE, e.getMessage() + "In Rule:" + conEx.getRule(), conEx);
            }

            Logger.getLogger(Accountant.class.getName()).log(Level.SEVERE, e.getMessage(), e);

            //throw e;
        } finally {
            for (FactHandle factHandle : coPayments) {
                session.retract(factHandle);
            }
            for (FactHandle factHandle : deductibles) {
                session.retract(factHandle);
            }
            for (FactHandle factHandle : activites) {
                session.retract(factHandle);
            }
            if (claim_factHandle != null) {
                session.retract(claim_factHandle);
            }
            if (dao_factHandle != null) {
                session.retract(dao_factHandle);
            }
            Logger.getLogger(PricingEngine.class.getName()).log(Level.INFO, "Session released Successfully");

        }

        return claim;
    }

    // Method to find the minimum (non-null) ActivityID
    public static Integer findMinActivityID(List<Activity> listOfActivities) {
        Integer minActivityID = null;

        for (Activity activity : listOfActivities) {
            String activityIDStr = activity.getActivityID();

            if (activityIDStr != null) {
                Integer currentID = Integer.parseInt(activityIDStr);

                if (minActivityID == null || currentID < minActivityID) {
                    minActivityID = currentID;
                }
            }
        }

        return minActivityID;
    }

    // Method to sort and replace null ActivityIDs with negative values
    public static List<Activity> sortActivities(List<Activity> listOfActivities) {
        Integer leastValue = findMinActivityID(listOfActivities);
        Integer counter = -1;

        if (leastValue != null && leastValue < counter) {
            counter = leastValue;
        }

        // Replace null ActivityIDs with decreasing negative values
        for (Activity act : listOfActivities) {
            if (act.getActivityID() == null) {
                act.setActivityID(counter.toString());
                counter--;
            }
        }
        List<Activity> positiveActs = new ArrayList<Activity>();
        for (Activity act : listOfActivities) {
            if (Integer.parseInt(act.getActivityID()) > 0) {
                positiveActs.add(act);
            }
        }
        List<Activity> negativeActs = new ArrayList<Activity>();
        for (Activity act : listOfActivities) {
            if (Integer.parseInt(act.getActivityID()) < 0) {
                negativeActs.add(act);
            }
        }
        // Sort activities by ActivityID in descending order
        Collections.sort(positiveActs, new Comparator<Activity>() {
            @Override
            public int compare(Activity p1, Activity p2) {
                Integer id1 = Integer.parseInt(p1.getActivityID());
                Integer id2 = Integer.parseInt(p2.getActivityID());
                return -1 * id2.compareTo(id1);
            }
        });
        Collections.sort(negativeActs, new Comparator<Activity>() {
            @Override
            public int compare(Activity p1, Activity p2) {
                Integer id1 = Integer.parseInt(p1.getActivityID());
                Integer id2 = Integer.parseInt(p2.getActivityID());
                return 1 * id2.compareTo(id1);
            }
        });
        // Combine negative acts into positive acts to  keep  order
        positiveActs.addAll(negativeActs);

        // Create a new list of activities based on positive acts
        return positiveActs;
    }

//    
//    public List<ActivityPrice> getPrices(DAO dao, Integer regulator, String payerLicense,
//            String receiverLicense, String facilityLicense, String packageName,
//            String patientInsurance, Long patientInsuranceId, Integer encounterType,
//            List<ActivityPriceRequest> activities)
//            throws Exception, NoPriceException, InvalidArgument {
//
//        if (session == null) {
//            return null;  //package is not loaded
//        }
//
//        
//        List<ActivityPrice> returned_activityPrices = new ArrayList<ActivityPrice>();
//
//        FactHandle controlFact_factHandle = null;
//        List<FactHandle> requests_factHandle = null;
//        FactHandle dao_factHandle = null;
//        //FactHandle price_factHandle = null;
//        List<FactHandle> deductables_factHandles = null;
//        List<FactHandle> coPayments_factHandles = null;
//
//        List<Deductible> deductables = dao.getDeductables(patientInsuranceId);
//        List<CoPayment> coPayments = dao.getCoPayments(patientInsuranceId);
//        //activityPrice = new ActivityPrice();
//
//        dao_factHandle = session.insert(dao); //New Rule-32
//        requests_factHandle = insertArray(session, activities);
//        if (!deductables.isEmpty()) {
//            deductables_factHandles = insertArray(session, deductables);
//        }
//        if (!coPayments.isEmpty()) {
//            coPayments_factHandles = insertArray(session, coPayments);
//        }
//        //price_factHandle = session.insert(activityPrice);
//
//        /*long lBegin = System.nanoTime();
//        session.fireAllRules();
//        Logger.getLogger(Accountant.class                    .getName()).log(Level.INFO, "Accountant.getPrices Done(" + (new Long((System.nanoTime() - lBegin) / 1000000)).toString() + "ms)");*/
//        controlFact_factHandle = session.insert(new ControlFact("PREPARE_GROUPS"));
//        long lBegin = System.nanoTime();
//        session.fireAllRules();
//        session.retract(controlFact_factHandle);
//        Logger.getLogger(Accountant.class                    .getName()).log(Level.INFO, "Accountant.getPrices-PREPARE_GROUPS Done(" + (new Long((System.nanoTime() - lBegin) / 1000000)).toString() + "ms)");
//        controlFact_factHandle = session.insert(new ControlFact("FIND_CONTRACTS"));
//        session.fireAllRules();
//        session.retract(controlFact_factHandle);
//        Logger.getLogger(Accountant.class                    .getName()).log(Level.INFO, "Accountant.getPrices-FIND_CONTRACTS Done(" + (new Long((System.nanoTime() - lBegin) / 1000000)).toString() + "ms)");
//        controlFact_factHandle = session.insert(new ControlFact("GET_CUS_PRICES"));
//        session.fireAllRules();
//        session.retract(controlFact_factHandle);
//        Logger.getLogger(Accountant.class                    .getName()).log(Level.INFO, "Accountant.getPrices-GET_CUS_PRICES Done(" + (new Long((System.nanoTime() - lBegin) / 1000000)).toString() + "ms)");
//        
//        Collection<Object> objects = session.getObjects();
//        for (Object obj : objects) {
//            if (obj instanceof ActivityPrice) {
//                ActivityPrice temp = (ActivityPrice) obj;
//                //temp.getActivityprice().setSteps(null);
//                returned_activityPrices.add((ActivityPrice) temp);
//            }
//        }
//        Logger.getLogger(Accountant.class                    .getName()).log(Level.INFO, "Loop Done(" + (new Long((System.nanoTime() - lBegin) / 1000000)).toString() + "ms)");
//
//        retractArray(session, requests_factHandle);
//        session.retract(dao_factHandle);
//
//        retractTempFacts(session);
//
//        if (deductables_factHandles != null && !deductables_factHandles.isEmpty()) {
//            retractArray(session, deductables_factHandles);
//        }
//        if (coPayments_factHandles != null && !coPayments_factHandles.isEmpty()) {
//            retractArray(session, coPayments_factHandles);
//        }
//        if (returned_activityPrices == null || returned_activityPrices.isEmpty()) {
//            throw new NoPriceException("");
//        }
//        return returned_activityPrices;
//    }
//
//    public Price calcPrice(Double price, Integer priceDiscount, ActivityPriceRequest request, DAO dao, Boolean enableDebugging)
//            throws Exception, NoPriceException, InvalidArgument {
//        if (session == null) {
//            return null;  //package is not loaded
//        }
//        InternalActivityPrice internalActivityPrice = new InternalActivityPrice();
//        internalActivityPrice.setActivityID(request.getID());
//        internalActivityPrice.setContractID(-1L);
//        internalActivityPrice.setListPrice(price);
//        internalActivityPrice.setDiscountPercentage(new Double(priceDiscount));
//        internalActivityPrice.getInvolvedRules().add("User Price");
//
//        //ActivityPrice activityPrice = null;
//
//
//        ActivityPrice returned_activityPrice = null;
//        DebuggingInfo returned_debuggingInfo = null;
//
//
//        FactHandle internalActivityPrice_factHandle = null;
//        FactHandle debugging_factHandle = null;
//        FactHandle request_factHandle = null;
//        FactHandle dao_factHandle = null;
//        //FactHandle price_factHandle = null;
//        List<FactHandle> deductables_factHandles = null;
//        List<FactHandle> coPayments_factHandles = null;
//
//        List<Deductible> deductables = dao.getDeductables(request.getPatientInsuranceId());
//        List<CoPayment> coPayments = dao.getCoPayments(request.getPatientInsuranceId());
//        //activityPrice = new ActivityPrice();
//
//        internalActivityPrice_factHandle = session.insert(internalActivityPrice);
//        dao_factHandle = session.insert(dao); //New Rule-32
//        request_factHandle = session.insert(request);
//        deductables_factHandles = insertArray(session, deductables);
//        coPayments_factHandles = insertArray(session, coPayments);
//        //price_factHandle = session.insert(activityPrice);
//        if (enableDebugging) {
//            debugging_factHandle = session.insert(new DebuggingInfo());
//        }
//
//        long lBegin = System.nanoTime();
//        session.fireAllRules();
//        Logger.getLogger(Accountant.class                    .getName()).log(Level.INFO, "Accountant.getPrice Done(" + (new Long((System.nanoTime() - lBegin) / 1000000)).toString() + "ms)");
//        Collection<Object> objects = session.getObjects();
//        for (Object obj : objects) {
//            if (obj instanceof ActivityPrice) {
//                returned_activityPrice = (ActivityPrice) obj;
//                if (!enableDebugging) {
//                    break;
//                }
//            }
//            if (enableDebugging) {
//                if (obj instanceof DebuggingInfo) {
//                    returned_debuggingInfo = (DebuggingInfo) obj;
//                }
//            }
//
//        }
//        Logger.getLogger(Accountant.class                    .getName()).log(Level.INFO, "Loop Done(" + (new Long((System.nanoTime() - lBegin) / 1000000)).toString() + "ms)");
//
//        session.retract(request_factHandle);
//        session.retract(dao_factHandle);
//
//        retractTempFacts(session);
//
//        retractArray(session, deductables_factHandles);
//        retractArray(session, coPayments_factHandles);
//        if (enableDebugging) {
//            session.retract(debugging_factHandle);
//        }
//        if (returned_activityPrice == null) {
//            throw new NoPriceException(returned_debuggingInfo == null ? "" : returned_debuggingInfo.getAllAsString());
//        }
//        return returned_activityPrice.getActivityprice();
//    }
    private static List<FactHandle> insertArray(StatefulKnowledgeSession session1, List list) {
        List<FactHandle> ret = new ArrayList<FactHandle>();
        for (Object obj : list) {
            ret.add(session1.insert(obj));
        }
        return ret;
    }

    private static void retractArray(StatefulKnowledgeSession session1, List<FactHandle> list) {
        for (FactHandle factHandle : list) {
            session1.retract(factHandle);
        }
    }
//
//    private static void retractTempFacts(StatefulKnowledgeSession session1) {
//        List<FactHandle> tempHandles = new ArrayList<FactHandle>();
//        Collection<Object> objects = session1.getObjects();
//        for (Object obj : objects) {
//            if (obj instanceof ActivityPrice) {
//                tempHandles.add(session1.getFactHandle((ActivityPrice) obj));
//            }
//            if (obj instanceof InternalActivityPrice) {
//                tempHandles.add(session1.getFactHandle((InternalActivityPrice) obj));
//            }
//        }
//        retractArray(session1, tempHandles);
//    }

    
    public boolean isSynchronized() {
        return bSynchronized;
    }

    
    public void setSynchronized(boolean bSynchronized) {
        this.bSynchronized = bSynchronized;
    }

}
