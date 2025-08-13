/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.cachedRepo;

import com.accumed.pricing.DBConnectionManager;
import com.accumed.pricing.model.Status;
import com.accumed.pricing.PricingEngine;
import com.accumed.pricing.Utils;
import static com.accumed.pricing.cachedRepo.DroolsUpdaterService.cachedRepositoryService;
import com.accumed.pricing.model.request.Activity;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class CachedRepositoryService implements Runnable {

    private CachedRepository repo;
    private Date repoDay;

    private static final Logger LOGGER = Logger.getLogger(PricingEngine.class.getName());

    public CachedRepositoryService() {

    }

    public boolean refreshRepository(String receiverLicense, String facilityLicense, String packageName) {
        int tablesChangedCount = checkSynchronization(receiverLicense, facilityLicense, packageName);
        return tablesChangedCount > 0;
    }

    @Override
    public void run() {
        try {
            // Log the start of the CachedRepository task with the current date and time.
            Logger.getLogger(CachedRepositoryService.class.getName())
                    .log(Level.INFO, "CachedRepository task running at {0}", new Date());

            // Check if the repository is not yet initialized.
            if (repo == null) {
                // Instantiate a new CachedRepository.
                repo = new CachedRepository();

                // Perform any necessary initialization. Passing null might indicate no specific configuration.
                initialize(null);

                // Register this CachedRepositoryService instance with the PricingEngine.
                PricingEngine.setCachedRepositoryService(this);

                // Register this CachedRepositoryService instance with the DroolsUpdaterService.
                DroolsUpdaterService.setCachedRepositoryService(this);

                // Mark that the background task manager has been initialized.
                BackgroundTaskManager.initialized = true;
            } else {
                // If the repository is already initialized, perform synchronization.
                cachedRepositoryService.checkSynchronizationJob();

                // Update the session repository in the PricingEngine's accountant pool with the current repository.
                PricingEngine.getAccountantPool().updateSessionRepository(cachedRepositoryService.getRepo());
            }
        } catch (Throwable e) {
            // Log any errors that occur during the run execution at the SEVERE level.
            // The log includes the exception's string representation and its full stack trace.
            Logger.getLogger(CachedRepositoryService.class.getName())
                    .log(Level.SEVERE, "Exception {0}{1}", new Object[]{e.toString(), Utils.stackTraceToString(e)});
        }
    }

    private Connection getPriceDB() {
        Connection con = null;
        try {
            // Obtain the initial naming context
            Context initCtx = new InitialContext();
            // Look up the environment naming context (java:comp/env) in the JNDI tree
            Context envCtx = (Context) initCtx.lookup("java:comp/env");
            // Look up the DataSource object using its JNDI name "jdbc/pricingDB"
            DataSource ds = (DataSource) envCtx.lookup("jdbc/pricingDB");

            // Retrieve a database connection from the DataSource
            con = ds.getConnection();

            // Check if the connection's transaction isolation level is set to READ_UNCOMMITTED
            if (con.getTransactionIsolation() != Connection.TRANSACTION_READ_UNCOMMITTED) {
                // Log an error if the transaction isolation level is not as expected
                Logger.getLogger(CachedRepositoryService.class.getName())
                        .log(Level.SEVERE, "DB connection is NOT READ_UNCOMMITTED.");
            } else {
                // Log an info message confirming the correct transaction isolation level
                Logger.getLogger(CachedRepositoryService.class.getName())
                        .log(Level.INFO, "DB connection is READ_UNCOMMITTED.");
            }
        } catch (SQLException ex) {
            // Log any SQL exceptions that occur while obtaining the connection
            Logger.getLogger(CachedRepositoryService.class.getName())
                    .log(Level.SEVERE, "Failed to establish a database connection.", ex);
        } catch (NamingException ex) {
            // Log any naming exceptions that occur during the JNDI lookup process
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
        }
        // Return the established database connection (or null if unsuccessful)
        return con;
    }

    private void initialize(String logicalName) {
        Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "CachedRepositoryService.initialize...");
        Connection dbConn = null;

        try {
            dbConn = DBConnectionManager.getPriceDB(); // Establish a database connection

            if (logicalName == null) {
                // Initialize all cached data if no specific logicalName is provided
                initializeAllCachedData(dbConn);
            } else {
                // Initialize specific cached data based on the logicalName
                initializeSpecificCachedData(dbConn, logicalName);
            }
        } catch (Exception ex) {
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, "Error during initialization", ex);
        } finally {
            // Ensure the database connection is closed
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, "Error closing database connection", ex);
                }
            }
        }
    }

    /**
     * Initializes all cached data by loading it from the database.
     */
    private void initializeAllCachedData(Connection dbConn) throws Exception {
        // Populate the repository cache with facilities data
        repo.addCachedData("facilities", RepoUtils.getFacilities(dbConn, "facilities"));

        // Populate the cache with SPC master price lists
        repo.addCachedData("spcMasterLists", RepoUtils.getMasterPriceLists(dbConn, "spcMasterLists"));

        // Populate the cache with SPC master price list items
        repo.addCachedData("spcMasterListsItems", RepoUtils.getMasterPricelistItems(dbConn, "spcMasterListsItems"));

        // Populate the cache with SPC contracts
        repo.addCachedData("spcContracts", RepoUtils.getSPCContracts(dbConn, "spcContracts"));

        // Populate the cache with SPC group factors
        repo.addCachedData("spcGroupFactors", RepoUtils.getSPCGroupFactors(dbConn, "spcGroupFactors"));

        // Populate the cache with SPC code factors
        repo.addCachedData("spcCodeFactors", RepoUtils.getSPCCodeFactors(dbConn, "spcCodeFactors"));

        // Populate the cache with code groups
        repo.addCachedData("codeGroups", RepoUtils.getCodeGroups(dbConn, "codeGroups"));

        // Populate the cache with drug prices
        repo.addCachedData("drugPrices", RepoUtils.getDrugPrices(dbConn, "drugPrices"));

        // Populate the cache with facility codes mapping for RCM
        repo.addCachedData("facilityCodesMapping", RepoUtils.getRCMFacilityCodesMapping(dbConn, "facilityCodesMapping"));

        // Populate the cache with custom contracts data
        repo.addCachedData(RepoUtils.getCustomContracts(dbConn));

        // Populate the cache with custom price lists data
        repo.addCachedData(RepoUtils.getCustomPriceLists(dbConn));

        // Populate the cache with all package codes for RCM
        repo.addCachedData("RCM_Package_Group", RepoUtils.getAllPackageCodes(dbConn, "RCM_Package_Group"));

        // Populate the cache with all package item codes for RCM
        repo.addCachedData("RCM_Package_Item", RepoUtils.getAllPackageItemCodes(dbConn, "RCM_Package_Item"));

        // Populate the cache with all custom codes for RCM
        repo.addCachedData("RCM_Custom_Codes", RepoUtils.getAllCustomeCodes(dbConn, "RCM_Custom_Codes"));

        repo.addCachedData("STT_DHA_DRG_CODES", RepoUtils.getAllDHA_DRG(dbConn, "STT_DHA_DRG_CODES"));

        repo.addCachedData("STT_DHA_COSTPERACTIVITY", RepoUtils.getAllDHA_DRG_COST_PER_ACTIVITY(dbConn, "STT_DHA_COSTPERACTIVITY"));

        repo.addCachedData("STT_DHA_DRG_HIGHCOST", RepoUtils.getAllDHA_DRG_HighCost(dbConn, "STT_DHA_DRG_HIGHCOST"));
        repo.addCachedData("STT_DRG_EXCLUDED_CPTS", RepoUtils.getAllDRGExcludedCPTs(dbConn, "STT_DRG_EXCLUDED_CPTS"));
         repo.addCachedData("ACCUMED_DRG_CODES", RepoUtils.getDRGCodes(dbConn, "ACCUMED_DRG_CODES"));

        // Populate the cache with the maximum audit IDs
        repo.addCachedData(RepoUtils.getMaxAuditIds(dbConn));
    }

    /**
     * Initializes specific cached data based on the logicalName.
     */
    private void initializeSpecificCachedData(Connection dbConn, String logicalName) throws Exception {
        // Check if the logical name indicates a custom contracts cache load.
        if (logicalName.startsWith("PL_CUS_CON")) {
            // Split the logical name by the pipe symbol.
            String[] arr = logicalName.split(Pattern.quote("|"));
            // Load custom contracts with parameters from the split array.
            loadCustomContracts(arr[1], arr[2]);
        } // Check if the logical name indicates a custom price list cache load.
        else if (logicalName.startsWith("PL_CUS_PL")) {
            // Split the logical name by the pipe symbol.
            String[] arr = logicalName.split(Pattern.quote("|"));
            // Load custom price list using the database connection and a parsed long value.
            loadCustomPriceList(dbConn, Long.parseLong(arr[1]));
        } // Check if the logical name indicates a facility clinicians cache load.
        else if (logicalName.startsWith("CLINICIANS")) {
            // Split the logical name by the pipe symbol.
            String[] arr = logicalName.split(Pattern.quote("|"));
            // Load facility clinicians data using the parameter from the split array.
            loadFacilityClinicians(arr[1]);
        } // For all other logical names, use a switch statement to determine the appropriate action.
        else {
            switch (logicalName) {
                case "spcMasterLists":
                    // Cache SPC master price lists.
                    repo.addCachedData("spcMasterLists", RepoUtils.getMasterPriceLists(dbConn, "spcMasterLists"));
                    break;
                case "spcMasterListsItems":
                    // Cache SPC master price list items.
                    repo.addCachedData("spcMasterListsItems", RepoUtils.getMasterPricelistItems(dbConn, "spcMasterListsItems"));
                    break;
                case "spcContracts":
                    // Cache SPC contracts.
                    repo.addCachedData("spcContracts", RepoUtils.getSPCContracts(dbConn, "spcContracts"));
                    break;
                case "spcGroupFactors":
                    // Cache SPC group factors.
                    repo.addCachedData("spcGroupFactors", RepoUtils.getSPCGroupFactors(dbConn, "spcGroupFactors"));
                    break;
                case "spcCodeFactors":
                    // Cache SPC code factors.
                    repo.addCachedData("spcCodeFactors", RepoUtils.getSPCCodeFactors(dbConn, "spcCodeFactors"));
                    break;
                case "codeGroups":
                    // Cache code groups.
                    repo.addCachedData("codeGroups", RepoUtils.getCodeGroups(dbConn, "codeGroups"));
                    break;
                case "facilities":
                    // Cache facilities data.
                    repo.addCachedData("facilities", RepoUtils.getFacilities(dbConn, "facilities"));
                    break;
                case "drugPrices":
                    // Cache drug prices.
                    repo.addCachedData("drugPrices", RepoUtils.getDrugPrices(dbConn, "drugPrices"));
                    break;
                case "facilityCodesMapping":
                    // Cache facility codes mapping for RCM.
                    repo.addCachedData("facilityCodesMapping", RepoUtils.getRCMFacilityCodesMapping(dbConn, "facilityCodesMapping"));
                    break;
                case "RCM_Package_Group":
                    // Cache all package codes for RCM.
                    repo.addCachedData("RCM_Package_Group", RepoUtils.getAllPackageCodes(dbConn, "RCM_Package_Group"));
                    break;
                case "RCM_Package_Item":
                    // Cache all package item codes for RCM.
                    repo.addCachedData("RCM_Package_Item", RepoUtils.getAllPackageItemCodes(dbConn, "RCM_Package_Item"));
                    break;
                case "RCM_Custom_Codes":
                    // Cache all custom codes for RCM.
                    repo.addCachedData("RCM_Custom_Codes", RepoUtils.getAllCustomeCodes(dbConn, "RCM_Custom_Codes"));
                    break;
                case "STT_DHA_DRG_CODES":
                    // Cache all custom codes for RCM.
                    repo.addCachedData("STT_DHA_DRG_CODES", RepoUtils.getAllDHA_DRG(dbConn, "STT_DHA_DRG_CODES"));

                    break;
                case "STT_DHA_COSTPERACTIVITY":
                    repo.addCachedData("STT_DHA_COSTPERACTIVITY", RepoUtils.getAllDHA_DRG_COST_PER_ACTIVITY(dbConn, "STT_DHA_COSTPERACTIVITY"));
                    break;
                case "STT_DHA_DRG_HIGHCOST":
                    repo.addCachedData("STT_DHA_DRG_HIGHCOST", RepoUtils.getAllDHA_DRG_HighCost(dbConn, "STT_DHA_DRG_HIGHCOST"));
                    break;
                case "STT_DRG_EXCLUDED_CPTS":
                    repo.addCachedData("STT_DRG_EXCLUDED_CPTS", RepoUtils.getAllDRGExcludedCPTs(dbConn, "STT_DRG_EXCLUDED_CPTS"));
                    break;

                case "ACCUMED_DRG_CODES":
                    repo.addCachedData("ACCUMED_DRG_CODES", RepoUtils.getDRGCodes(dbConn, "ACCUMED_DRG_CODES"));
                      break;
                default:
                    // Log a warning for any unrecognized logical name.
                    Logger.getLogger(CachedRepositoryService.class.getName())
                            .log(Level.WARNING, "Unknown logicalName: " + logicalName);
                    break;
            }
        }
    }

    public int reSynchronize() {
        // Log the start of the re-synchronization process.
        Logger.getLogger(CachedRepositoryService.class.getName())
                .log(Level.INFO, "CachedRepositoryService.reSynchronize...");

        // Initialize a counter for the number of re-synchronized entries.
        int ret = 0;
        Connection dbConn = null;

        try {
            // Obtain a database connection.
            dbConn = DBConnectionManager.getPriceDB();

            // Iterate over each entry in the cached database map.
            for (Map.Entry<String, CachedData> entry : repo.getCachedDB().entrySet()) {
                CachedData cachedData = entry.getValue();
                // Check if the cached data has an INVALID status.
                if (Status.INVALID == cachedData.getStatus()) {
                    // Re-initialize the cached data for the given key.
                    this.initialize(entry.getKey());
                    // Increment the counter for each re-synchronization.
                    ret++;
                    // Log that the specific cache entry has been re-synchronized.
                    Logger.getLogger(CachedRepositoryService.class.getName())
                            .log(Level.INFO, "{0} reSynchronized.", entry.getKey());
                }
            }
        } catch (Exception ex) {
            // Log any exceptions encountered during the re-synchronization process.
            Logger.getLogger(CachedRepositoryService.class.getName())
                    .log(Level.SEVERE, null, ex);
        } finally {
            // Ensure the database connection is closed to free resources.
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    // Log any exceptions that occur while closing the connection.
                    Logger.getLogger(CachedRepositoryService.class.getName())
                            .log(Level.SEVERE, null, ex);
                }
                dbConn = null; // Clear the connection reference.
            }
        }

        // Return the number of cache entries that were re-synchronized.
        return ret;
    }

    private int checkSynchronization() {
        // Get the current time
        Date currentTime = new Date();

        // Create a date format to display the time in "yyyy-MM-dd HH:mm:ss" format
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Format the current time using the specified format
        String formattedTime = dateFormat.format(currentTime);

        // Print the current time to the console
        System.out.println("Current time: " + formattedTime);
        // Log the current time along with a message at the INFO level
        Logger.getLogger(CachedRepositoryService.class.getName())
                .log(Level.INFO, "CachedRepositoryService.checkSynchronization... Current time:  " + formattedTime);

        Connection dbConn = null;
        try {
            // Get a database connection for the pricing database
            dbConn = DBConnectionManager.getPriceDB();
            // Perform synchronization check on the repository using the obtained connection
            return repo.checkSynchronization(dbConn);
        } catch (Exception ex) {
            // Log any exceptions that occur during the synchronization check at the SEVERE level
            Logger.getLogger(CachedRepositoryService.class.getName())
                    .log(Level.SEVERE, null, ex);
        } finally {
            // Ensure the database connection is closed to avoid resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    // Log any exceptions that occur while closing the connection
                    Logger.getLogger(CachedRepositoryService.class.getName())
                            .log(Level.SEVERE, null, ex);
                }
                dbConn = null; // Clear the connection reference
            }
        }
        // Return 0 if the synchronization check could not be performed successfully
        return 0;
    }

    private int checkSynchronization(String receiverLicense, String facilityLicense, String packageName) {
        // Log the start of the synchronization check.
        Logger.getLogger(CachedRepositoryService.class.getName())
                .log(Level.INFO, "CachedRepositoryService.checkSynchronization...");

        Connection dbConn = null;
        try {
            // Obtain a database connection from the pricing database.
            dbConn = DBConnectionManager.getPriceDB();
            // Delegate the synchronization check to the repository,
            // passing the database connection along with the provided parameters.
            return repo.checkSynchronization(dbConn, receiverLicense, facilityLicense, packageName);
        } catch (Exception ex) {
            // Log any exception that occurs during the process.
            Logger.getLogger(CachedRepositoryService.class.getName())
                    .log(Level.SEVERE, null, ex);
        } finally {
            // Ensure the database connection is closed to free up resources.
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    // Log any SQLException encountered while closing the connection.
                    Logger.getLogger(CachedRepositoryService.class.getName())
                            .log(Level.SEVERE, null, ex);
                }
                dbConn = null; // Clear the reference to the connection.
            }
        }
        // Return 0 if the synchronization check failed or an exception was caught.
        return 0;
    }

    public CachedRepository getRepo() {
        return repo;
    }

    public boolean loadCustomContracts(String receiverLicense, String facilityLicense) {
        // Obtain a logger instance for logging messages
        Logger logger = Logger.getLogger(CachedRepositoryService.class.getName());
        logger.log(Level.INFO, "Loading Custom Contracts from Cache or DB...");

        boolean ret = false;  // Flag to track if contracts were loaded successfully
        Connection dbConn = null;  // Database connection
        CachedData customContracts; // Variable to store cached custom contracts

        // Construct the logical name used to identify cached contracts
        String sLogicalName = "PL_CUS_CON|" + receiverLicense + "|" + facilityLicense;

        try {
            // Check if the contracts are already cached
            customContracts = repo.get(sLogicalName);
            boolean contractsAlreadyCached = (customContracts != null && !customContracts.isInvalid());

            if (!contractsAlreadyCached) {
                // If not cached, retrieve from the database
                dbConn = DBConnectionManager.getPriceDB();
                customContracts = RepoUtils.getCustomContracts(dbConn, sLogicalName, receiverLicense, facilityLicense);

                // If contracts exist, load each custom price list associated with them
                if (customContracts != null && !customContracts.getData().isEmpty()) {
                    for (Object customContract : customContracts.getData()) {
                        ret |= loadCustomPriceList(dbConn,
                                ((com.accumed.pricing.model.CusContract) customContract).getPriceListId());
                    }
                    // Store the retrieved contracts in the cache
                    repo.addCachedData(sLogicalName, customContracts);
                }
            } else {
                // Log if contracts are already cached
                logger.log(Level.INFO, "Contracts already cached for: {0}", sLogicalName);
            }

        } catch (Exception ex) {
            // Log any exceptions that occur while loading custom contracts
            logger.log(Level.SEVERE, "Error loading custom contracts", ex);
        } finally {
            // Ensure the database connection is closed in the finally block
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, "Error closing database connection", ex);
                }
            }
        }
        return ret; // Return whether contracts were successfully loaded
    }

    public boolean loadCustomPriceList(Connection dbConn, Long PriceListId) {
        // Flag to indicate if the price list was successfully loaded
        boolean ret = false;
        // Flag to check if the price list is already cached
        boolean plsAlreadyCached = true;
        // Variable to store cached price list data
        CachedData customPriceList = null;

        // Construct the logical name for caching the price list
        String sLogicalName2 = "PL_CUS_PL|" + PriceListId;

        // Check if the price list is already cached
        customPriceList = repo.get(sLogicalName2);

        // If not cached or invalid, retrieve it from the database
        if (customPriceList == null || customPriceList.isInvalid()) {
            customPriceList = RepoUtils.getCustomPriceLists(dbConn, sLogicalName2, PriceListId);
            plsAlreadyCached = false;  // Mark as not cached
        }

        // If the price list was retrieved from the database, store it in the cache
        if (!plsAlreadyCached) {
            repo.addCachedData(sLogicalName2, customPriceList);
            ret = true;  // Indicate that data was loaded
        }

        return ret;  // Return whether the price list was successfully loaded
    }

    public boolean loadFacilityClinicians(String facilityLicense) {
        boolean ret = false;
        Connection dbConn = null;
        try {
            // Generate a unique logical name for the cache
            String sLogicalName = "CLINICIANS|" + facilityLicense;

            // Check if the clinicians are already cached
            CachedData clinicians = repo.get(sLogicalName);

            // If clinicians are not cached or are invalid, load them from the database
            if (clinicians == null || clinicians.isInvalid()) {
                dbConn = DBConnectionManager.getPriceDB(); // Establish a database connection only if needed
                clinicians = RepoUtils.getFacilityClinicians(dbConn, sLogicalName, facilityLicense);

                // Cache the newly loaded clinicians
                repo.addCachedData(sLogicalName, clinicians);
                ret = true; // Indicate that the clinicians were loaded and cached

                Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "FacilityClinicians are Cached for the first time.");
            } else {
                Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "FacilityClinicians are already cached. for Facility:{0}", facilityLicense);
            }

        } catch (Exception ex) {
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, "Error loading facility clinicians", ex);
        } finally {
            // Ensure the database connection is closed
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, "Error closing database connection", ex);
                }
            }
        }
        return ret;
    }

//    public boolean loadFacility(String facilityLicense) {
//        boolean ret = false;
//        Connection dbConn = DBConnectionManager.getPriceDB();
//        CachedData facility = null;
//        boolean facilityAlreadyCached = true;
//        try {
//            String sLogicalName = "FACILITY|" + facilityLicense;
//            facility = repo.get(sLogicalName);//repo.getCustomContracts(sLogicalName); //is already cached
//            if (facility == null || facility.isInvalid()) {
//                facility = RepoUtils.getFacility(dbConn, sLogicalName, facilityLicense);
//                facilityAlreadyCached = false;
//            }
//
//            if (!facilityAlreadyCached) {
//                repo.addCachedData(sLogicalName, facility);
//                ret = true;
//            }
//
//        } catch (Exception ex) {
//            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
//        } finally {
//            if (dbConn != null) {
//                try {
//                    dbConn.close();
//                } catch (SQLException ex) {
//                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
//                }
//                dbConn = null;
//            }
//        }
//        return ret;
//    }
    public boolean loadDHA_DRG_COST_PER_ACTIVITY(List<Activity> activityList) {
        // Logging the start of the method execution
        Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "CachedRepositoryService.loadDHA_DRG_COST_PER_ACTIVITY...");

        boolean ret = false; // Flag to indicate whether new data was loaded
        Connection dbConn = DBConnectionManager.getPriceDB(); // Establishing a connection to the database
        CachedData dha_DRG_COST_PER_ACTIVITY = null; // Variable to hold cached data
        boolean dha_DRG_COST_PER_ACTIVITYAlreadyCached = true; // Flag to track if data is already cached

        try {
            // Logical name used for caching
            String sLogicalName = "STT_DHA_COSTPERACTIVITY";

            // Attempt to retrieve the cached data
            dha_DRG_COST_PER_ACTIVITY = repo.get(sLogicalName);

            // If no cached data exists or the cached data is invalid, retrieve it from the database
            if (dha_DRG_COST_PER_ACTIVITY == null || dha_DRG_COST_PER_ACTIVITY.isInvalid()) {
                dha_DRG_COST_PER_ACTIVITY = RepoUtils.getDHA_DRG_COST_PER_ACTIVITY(dbConn, sLogicalName, activityList);
                dha_DRG_COST_PER_ACTIVITYAlreadyCached = false; // Mark that fresh data was retrieved
            }

            // If fresh data was retrieved, store it in the cache
            if (!dha_DRG_COST_PER_ACTIVITYAlreadyCached) {
                repo.addCachedData(sLogicalName, dha_DRG_COST_PER_ACTIVITY);
                ret = true; // Mark that new data was loaded
            }

        } catch (Exception ex) {
            // Log any exceptions that occur during the process
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            // Ensure the database connection is closed to prevent resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
                }
                dbConn = null; // Explicitly set the connection to null
            }
        }

        // Return whether new data was loaded into the cache
        return ret;
    }

    public boolean loadDHA_DRG_HighCost(List<Activity> activityList) {
        // Logging the start of the method execution
        Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "CachedRepositoryService.loadDHA_DRG_HighCost...");

        boolean ret = false; // Flag to indicate whether new data was loaded
        Connection dbConn = DBConnectionManager.getPriceDB(); // Establishing a connection to the database
        CachedData dha_DRG_HighCost = null; // Variable to hold cached data
        boolean dha_DRG_HighCostAlreadyCached = true; // Flag to track if data is already cached

        try {
            // Logical name used for caching
            String sLogicalName = "STT_DHA_DRG_HIGHCOST";

            // Attempt to retrieve the cached data
            dha_DRG_HighCost = repo.get(sLogicalName);

            // If no cached data exists or the cached data is invalid, retrieve it from the database
            if (dha_DRG_HighCost == null || dha_DRG_HighCost.isInvalid()) {
                dha_DRG_HighCost = RepoUtils.getDHA_DRG_HighCost(dbConn, sLogicalName, activityList);
                dha_DRG_HighCostAlreadyCached = false; // Mark that fresh data was retrieved
            }

            // If fresh data was retrieved, store it in the cache
            if (!dha_DRG_HighCostAlreadyCached) {
                repo.addCachedData(sLogicalName, dha_DRG_HighCost);
                ret = true; // Mark that new data was loaded
            }

        } catch (Exception ex) {
            // Log any exceptions that occur during the process
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            // Ensure the database connection is closed to prevent resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
                }
                dbConn = null; // Explicitly set the connection to null
            }
        }

        // Return whether new data was loaded into the cache
        return ret;
    }

    public boolean loadDRGExcludedCpts(List<Activity> activityList) {
        // Logging the start of the method execution
        Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "CachedRepositoryService.loadDRGExcludedCpts...");

        boolean ret = false; // Flag to indicate whether new data was loaded
        Connection dbConn = DBConnectionManager.getPriceDB(); // Establishing a connection to the database
        CachedData drgExcludedCpts = null; // Variable to hold cached data
        boolean drgExcludedCptsAlreadyCached = true; // Flag to track if data is already cached

        try {
            // Logical name used for caching
            String sLogicalName = "STT_DRG_EXCLUDED_CPTS";

            // Attempt to retrieve the cached data
            drgExcludedCpts = repo.get(sLogicalName);

            // If no cached data exists or the cached data is invalid, retrieve it from the database
            if (drgExcludedCpts == null || drgExcludedCpts.isInvalid()) {
                drgExcludedCpts = RepoUtils.getDRGExcludedCPTs(dbConn, sLogicalName, activityList);
                drgExcludedCptsAlreadyCached = false; // Mark that fresh data was retrieved
            }

            // If fresh data was retrieved, store it in the cache
            if (!drgExcludedCptsAlreadyCached) {
                repo.addCachedData(sLogicalName, drgExcludedCpts);
                ret = true; // Mark that new data was loaded
            }

        } catch (Exception ex) {
            // Log any exceptions that occur during the process
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, "Error loading DRG Excluded CPTs", ex);
        } finally {
            // Ensure the database connection is closed to prevent resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, "Error closing database connection", ex);
                }
                dbConn = null; // Explicitly set the connection to null
            }
        }

        // Return whether new data was loaded into the cache
        return ret;
    }

    public boolean loadDHA_DRG(List<Activity> activityList) {
        // Log the start of the method execution
        Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "CachedRepositoryService.loadDHA_DRG...");

        boolean ret = false; // Flag to indicate whether new data was loaded
        Connection dbConn = DBConnectionManager.getPriceDB(); // Establish a database connection
        CachedData dhaDrgs = null; // Variable to store cached data
        boolean dha_DRGsAlreadyCached = true; // Flag to check if the data is already cached

        try {
            // Define a logical name for caching
            String sLogicalName = "STT_DHA_DRG_CODES";

            // Attempt to retrieve cached data from the repository
            dhaDrgs = repo.get(sLogicalName); // Check if data is already cached

            // If data is not cached or is invalid, fetch it from the database
            if (dhaDrgs == null || dhaDrgs.isInvalid()) {
                dhaDrgs = RepoUtils.getDHA_DRG(dbConn, sLogicalName, activityList);
                dha_DRGsAlreadyCached = false; // Mark that fresh data was retrieved
            }

            // If fresh data was retrieved, store it in the cache
            if (!dha_DRGsAlreadyCached) {
                repo.addCachedData(sLogicalName, dhaDrgs);
                ret = true; // Indicate that new data was loaded
            }

        } catch (Exception ex) {
            // Log any exceptions encountered
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, "Error loading DHA DRG Codes", ex);
        } finally {
            // Ensure the database connection is closed to prevent resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, "Error closing database connection", ex);
                }
                dbConn = null; // Explicitly set the connection to null
            }
        }

        // Return whether new data was loaded into the cache
        return ret;
    }

    public boolean loadAllDHA_DRG_COST_PER_ACTIVITY() {
        // Create a logger instance
        Logger logger = Logger.getLogger(CachedRepositoryService.class.getName());
        logger.log(Level.INFO, "Loading DHA DRG Cost Per Activity data from Cache or DB...");

        boolean ret = false; // Flag to indicate whether new data was loaded
        Connection dbConn = null; // Database connection instance
        CachedData dhaDrgCostPerActivity; // Variable to store the cached data

        // Define a logical name for caching
        String sLogicalName = "STT_DHA_COSTPERACTIVITY";

        try {
            // Check if the data is already cached
            dhaDrgCostPerActivity = repo.get(sLogicalName);
            boolean dhaDrgCostPerActivityAlreadyCached = (dhaDrgCostPerActivity != null && !dhaDrgCostPerActivity.isInvalid());

            // If data is not cached or is invalid, retrieve it from the database
            if (!dhaDrgCostPerActivityAlreadyCached) {
                dbConn = DBConnectionManager.getPriceDB(); // Establish a database connection
                dhaDrgCostPerActivity = RepoUtils.getAllDHA_DRG_COST_PER_ACTIVITY(dbConn, sLogicalName);

                // If data retrieval was successful and contains valid records
                if (dhaDrgCostPerActivity != null && !dhaDrgCostPerActivity.getData().isEmpty()) {
                    // Store the new data in the cache
                    repo.addCachedData(sLogicalName, dhaDrgCostPerActivity);
                    ret = true; // Mark that new data was loaded
                }
            } else {
                // Log that the data was already cached, avoiding unnecessary database queries
                logger.log(Level.INFO, "DHA DRG Cost Per Activity data already cached.");
            }

        } catch (Exception ex) {
            // Log any errors encountered during the process
            logger.log(Level.SEVERE, "Error loading DHA DRG Cost Per Activity data", ex);
        } finally {
            // Ensure the database connection is closed to prevent resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, "Error closing database connection", ex);
                }
            }
        }

        // Return whether new data was loaded into the cache
        return ret;
    }

    public boolean loadAllDHA_DRG_HighCost() {
        // Create a logger instance to log the process of loading data
        Logger logger = Logger.getLogger(CachedRepositoryService.class.getName());
        logger.log(Level.INFO, "Loading DHA DRG High-Cost data from Cache or DB...");

        boolean ret = false; // Flag to indicate if new data was loaded
        Connection dbConn = null; // Database connection instance
        CachedData dhaDrgHighCost; // Variable to store the cached data

        // Define a logical name for the data in the cache
        String sLogicalName = "STT_DHA_DRG_HIGHCOST";

        try {
            // Check if the data is already cached
            dhaDrgHighCost = repo.get(sLogicalName);
            boolean dhaDrgHighCostAlreadyCached = (dhaDrgHighCost != null && !dhaDrgHighCost.isInvalid());

            // If data is not cached or is invalid, retrieve it from the database
            if (!dhaDrgHighCostAlreadyCached) {
                dbConn = DBConnectionManager.getPriceDB(); // Establish a connection to the database
                dhaDrgHighCost = RepoUtils.getAllDHA_DRG_HighCost(dbConn, sLogicalName);

                // If data was successfully retrieved and is not empty
                if (dhaDrgHighCost != null && !dhaDrgHighCost.getData().isEmpty()) {
                    // Cache the new data
                    repo.addCachedData(sLogicalName, dhaDrgHighCost);
                    ret = true; // Mark that new data was loaded
                }
            } else {
                // Log that the data is already cached and no database call is needed
                logger.log(Level.INFO, "DHA DRG High-Cost data already cached.");
            }

        } catch (Exception ex) {
            // Log any errors encountered during data loading
            logger.log(Level.SEVERE, "Error loading DHA DRG High-Cost data", ex);
        } finally {
            // Ensure the database connection is properly closed to avoid resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, "Error closing database connection", ex);
                }
            }
        }

        // Return whether new data was successfully loaded into the cache
        return ret;
    }

    public boolean loadAllDRGExcludedCpts() {
        // Create a logger instance to log the process of loading data
        Logger logger = Logger.getLogger(CachedRepositoryService.class.getName());
        logger.log(Level.INFO, "Loading DRG Excluded CPTs from Cache or DB...");

        boolean ret = false; // Flag to indicate if new data was loaded
        Connection dbConn = null; // Database connection instance
        CachedData drgExcludedCpts; // Variable to store the cached data

        // Define a logical name for the data in the cache
        String sLogicalName = "STT_DRG_EXCLUDED_CPTS";

        try {
            // Check if the data is already cached
            drgExcludedCpts = repo.get(sLogicalName);
            boolean drgExcludedCptsAlreadyCached = (drgExcludedCpts != null && !drgExcludedCpts.isInvalid());

            // If data is not cached or is invalid, retrieve it from the database
            if (!drgExcludedCptsAlreadyCached) {
                dbConn = DBConnectionManager.getPriceDB(); // Establish a connection to the database
                drgExcludedCpts = RepoUtils.getAllDRGExcludedCPTs(dbConn, sLogicalName);

                // If data was successfully retrieved and is not empty
                if (drgExcludedCpts != null && !drgExcludedCpts.getData().isEmpty()) {
                    // Cache the new data
                    repo.addCachedData(sLogicalName, drgExcludedCpts);
                    ret = true; // Mark that new data was loaded
                }
            } else {
                // Log that the data is already cached and no database call is needed
                logger.log(Level.INFO, "DRG Excluded CPTs already cached.");
            }

        } catch (Exception ex) {
            // Log any errors encountered during data loading
            logger.log(Level.SEVERE, "Error loading DRG Excluded CPTs", ex);
        } finally {
            // Ensure the database connection is properly closed to avoid resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, "Error closing database connection", ex);
                }
            }
        }

        // Return whether new data was successfully loaded into the cache
        return ret;
    }

    public boolean loadAllDHA_DRG() {
        // Create a logger instance to log the process of loading data
        Logger logger = Logger.getLogger(CachedRepositoryService.class.getName());
        logger.log(Level.INFO, "Loading DHA DRG Codes from Cache or DB...");

        boolean ret = false; // Flag to indicate if new data was loaded
        Connection dbConn = null; // Database connection instance
        CachedData dhaDrgs = null; // Variable to store the cached data

        // Define a logical name for the data in the cache
        String sLogicalName = "STT_DHA_DRG_CODES";

        try {

            // Check if the data is already cached
            dhaDrgs = repo.get(sLogicalName);
//            dbConn = DBConnectionManager.getPriceDB(); // Establish a connection to the database
//            dhaDrgs = RepoUtils.getAllDHA_DRG(dbConn, sLogicalName);
            boolean dha_DRGsAlreadyCached = (dhaDrgs != null && !dhaDrgs.isInvalid());

            // If data is not cached or is invalid, retrieve it from the database
            if (!dha_DRGsAlreadyCached) {
                dbConn = DBConnectionManager.getPriceDB(); // Establish a connection to the database
                dhaDrgs = RepoUtils.getAllDHA_DRG(dbConn, sLogicalName);

                // If data was successfully retrieved and is not empty
                if (dhaDrgs != null && !dhaDrgs.getData().isEmpty()) {
                    // Cache the new data
                    repo.addCachedData(sLogicalName, dhaDrgs);
                    ret = true; // Mark that new data was loaded
                }
            } else {
                // Log that the data is already cached and no database call is needed
                logger.log(Level.INFO, "DHA DRG Codes already cached.");
            }

        } catch (Exception ex) {
            // Log any errors encountered during data loading
            logger.log(Level.SEVERE, "Error loading DHA DRG Codes", ex);
        } finally {
            // Ensure the database connection is properly closed to avoid resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, "Error closing database connection", ex);
                }
            }
        }

        // Return whether new data was successfully loaded into the cache
        return ret;
    }

    public boolean loadAllHAAD_DRG() {
        // Create a logger instance to log the process of loading data
        Logger logger = Logger.getLogger(CachedRepositoryService.class.getName());
        logger.log(Level.INFO, "Loading HAAD DRG Codes from Cache or DB...");

        boolean ret = false; // Flag to indicate if new data was loaded
        Connection dbConn = null; // Database connection instance
        CachedData haadDrgs = null; // Variable to store the cached data

        // Define a logical name for the data in the cache
        String sLogicalName = "ACCUMED_DRG_CODES";

        try {
            // Check if the data is already cached
            haadDrgs = repo.get(sLogicalName);
            boolean haad_DRGsAlreadyCached = (haadDrgs != null && !haadDrgs.isInvalid());

            // If data is not cached or is invalid, retrieve it from the database
            if (!haad_DRGsAlreadyCached) {
                dbConn = DBConnectionManager.getPriceDB(); // Establish a connection to the database
                haadDrgs = RepoUtils.getDRGCodes(dbConn, sLogicalName);

                // If data was successfully retrieved and is not empty
                if (haadDrgs != null && !haadDrgs.getData().isEmpty()) {
                    // Cache the new data
                    repo.addCachedData(sLogicalName, haadDrgs);
                    ret = true; // Mark that new data was loaded
                }
            } else {
                // Log that the data is already cached and no database call is needed
                logger.log(Level.INFO, "HAAD DRG Codes already cached.");
            }

        } catch (Exception ex) {
            // Log any errors encountered during data loading
            logger.log(Level.SEVERE, "Error loading DHA DRG Codes", ex);
        } finally {
            // Ensure the database connection is properly closed to avoid resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    logger.log(Level.SEVERE, "Error closing database connection", ex);
                }
            }
        }

        // Return whether new data was successfully loaded into the cache
        return ret;
    }

    public boolean loadCustomcodes() {
        // Log the process of loading custom codes data
        Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "CachedRepositoryService.loadCustomcodes...");

        boolean ret = false; // Flag to track whether new data was loaded
        Connection dbConn = DBConnectionManager.getPriceDB(); // Database connection to retrieve custom codes
        CachedData customCodes = null; // To store the custom codes data
        boolean customeCodesCached = true; // Flag to track if the custom codes are already cached

        try {
            // Define a logical name for the custom codes cache
            String sLogicalName = "RCM_Custom_Codes";

            // Try to get custom codes from the cache
            customCodes = repo.get(sLogicalName);

            // Check if custom codes are not found or the cache is invalid
            if (customCodes == null || customCodes.isInvalid()) {
                // If not in cache or cache is invalid, fetch the data from the database
                customCodes = RepoUtils.getAllCustomeCodes(dbConn, sLogicalName);
                customeCodesCached = false; // Data is not cached
            }

            // If data was fetched from the database (not from the cache)
            if (!customeCodesCached) {
                // Cache the new custom codes data for future use
                repo.addCachedData(sLogicalName, customCodes);
                ret = true; // Mark that new data was loaded
            }

        } catch (Exception ex) {
            // Log any errors that occur during the loading process
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            // Ensure that the database connection is closed properly
            if (dbConn != null) {
                try {
                    dbConn.close(); // Close the connection to avoid resource leaks
                } catch (SQLException ex) {
                    // Log any errors that occur while closing the connection
                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
                }
                dbConn = null; // Set the connection to null to avoid using it later
            }
        }

        // Return whether new data was loaded (from the database) or not
        return ret;
    }

    public boolean loadPackageGroups() {
        // Log the process of loading package group data
        Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "CachedRepositoryService.loadPackages...");

        boolean ret = false; // Flag to track whether new data was loaded
        Connection dbConn = DBConnectionManager.getPriceDB(); // Database connection to retrieve package group data
        CachedData Packages = null; // To store the package group data
        boolean packageCodesCached = true; // Flag to track if the package group data is already cached

        try {
            // Define a logical name for the package group cache
            String sLogicalName = "RCM_Package_Group";

            // Try to get the package group data from the cache
            Packages = repo.get(sLogicalName);

            // Check if the package group data is not found or the cache is invalid
            if (Packages == null || Packages.isInvalid()) {
                // If not in cache or cache is invalid, fetch the data from the database
                Packages = RepoUtils.getAllPackageCodes(dbConn, sLogicalName);
                packageCodesCached = false; // Data is not cached
            }

            // If data was fetched from the database (not from the cache)
            if (!packageCodesCached) {
                // Cache the new package group data for future use
                repo.addCachedData(sLogicalName, Packages);
                ret = true; // Mark that new data was loaded
            }

        } catch (Exception ex) {
            // Log any errors that occur during the loading process
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            // Ensure that the database connection is closed properly
            if (dbConn != null) {
                try {
                    dbConn.close(); // Close the connection to avoid resource leaks
                } catch (SQLException ex) {
                    // Log any errors that occur while closing the connection
                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
                }
                dbConn = null; // Set the connection to null to avoid using it later
            }
        }

        // Return whether new data was loaded (from the database) or not
        return ret;
    }

    public boolean loadPackageItems() {
        // Log the process of loading package item data from Cache or DB
        Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.INFO, "CachedRepositoryService.loadPackages...");

        boolean ret = false; // Flag to track if new data was loaded
        Connection dbConn = DBConnectionManager.getPriceDB(); // Get database connection for package item data retrieval
        CachedData Packages = null; // Variable to hold the package item data
        boolean packageCodesCached = true; // Flag to check if package item data is cached

        try {
            // Define a logical name for the package item cache
            String sLogicalName = "RCM_Package_Item";

            // Try to get the package item data from the cache
            Packages = repo.get(sLogicalName);

            // Check if the package item data is not found or the cache is invalid
            if (Packages == null || Packages.isInvalid()) {
                // If data is not in the cache or the cache is invalid, fetch data from the database
                Packages = RepoUtils.getAllPackageItemCodes(dbConn, sLogicalName);
                packageCodesCached = false; // Mark that data was not cached
            }

            // If data was retrieved from the database (not from the cache)
            if (!packageCodesCached) {
                // Cache the new package item data for future use
                repo.addCachedData(sLogicalName, Packages);
                ret = true; // Set flag to true indicating data was loaded and cached
            }

        } catch (Exception ex) {
            // Log any exceptions that occur during the loading process
            Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            // Ensure the database connection is closed to avoid resource leaks
            if (dbConn != null) {
                try {
                    dbConn.close(); // Close the connection to release resources
                } catch (SQLException ex) {
                    // Log any errors while closing the connection
                    Logger.getLogger(CachedRepositoryService.class.getName()).log(Level.SEVERE, null, ex);
                }
                dbConn = null; // Set the database connection to null
            }
        }

        // Return whether new data was loaded and cached or not
        return ret;
    }

    public int checkSynchronizationJob() {
        // Log the start of the synchronization check.
        Logger.getLogger(CachedRepositoryService.class.getName())
                .log(Level.INFO, "CachedRepositoryService.checkSynchronization...");

        Connection dbConn = null;
        try {
            // Obtain a connection to the pricing database.
            dbConn = DBConnectionManager.getPriceDB();
            // Delegate the synchronization check to the repository using the database connection.
            return repo.checkSynchronizationJob(dbConn);
        } catch (Exception ex) {
            // Log any exceptions that occur during the synchronization check.
            Logger.getLogger(CachedRepositoryService.class.getName())
                    .log(Level.SEVERE, null, ex);
        } finally {
            // Ensure the database connection is closed to free resources.
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                    // Log any exceptions that occur while closing the connection.
                    Logger.getLogger(CachedRepositoryService.class.getName())
                            .log(Level.SEVERE, null, ex);
                }
                dbConn = null;  // Clear the connection reference.
            }
        }
        // Return 0 if an exception occurred or if no valid result was obtained.
        return 0;
    }

}
