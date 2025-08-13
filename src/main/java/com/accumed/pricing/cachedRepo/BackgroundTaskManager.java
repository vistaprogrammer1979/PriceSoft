/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.cachedRepo;

import com.accumed.pricing.Accountant;
import com.accumed.pricing.PricingEngine;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import com.accumed.pricing.PricingLogger;

/**
 *
 * @author smutlak
 */
public class BackgroundTaskManager implements ServletContextListener {

    private static final int MAXIMUM_CURRENT = 3;
    private static final int initialDelay = 30;//90;
    private static   int period = 3600; 
    private static final int accountantPeriod = 60;//300;//180; //5 minutes
    private static final int loggerInitialDelay = 15;//90;
    private static final int loggerPeriod = 30;//180;
    private static ScheduledThreadPoolExecutor executor = null;
    private static ScheduledFuture cachedRepositoryFuture;
    private static ScheduledFuture droolsUpdaterFuture;
    private static ScheduledFuture pricingLoggerFuture;
     public static ThreadPoolExecutor RefreshingJobPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(100));
    public static boolean initialized= false; 
    public static boolean isRunning(ScheduledFuture future) {
        return future.getDelay(TimeUnit.MILLISECONDS) <= 0;
    }

    public static boolean isRunningCachedRepositoryFuture() {
        return (cachedRepositoryFuture == null || !(cachedRepositoryFuture instanceof ScheduledFuture)) ? false
                : cachedRepositoryFuture.getDelay(TimeUnit.MILLISECONDS) > 0;
    }

    public static boolean isRunningDroolsUpdaterFuture() {
        return (droolsUpdaterFuture == null || !(droolsUpdaterFuture instanceof ScheduledFuture)) ? false
                : droolsUpdaterFuture.getDelay(TimeUnit.MILLISECONDS) > 0;
    }

    public static boolean isRunningPricingLoggerFuture() {
        return (pricingLoggerFuture == null || !(pricingLoggerFuture instanceof ScheduledFuture)) ? false
                : pricingLoggerFuture.getDelay(TimeUnit.MILLISECONDS) > 0;
    }

    synchronized public static void restartAgents() {
        if (executor != null) {
            
            executor.shutdownNow();//shutdown();
            while (!executor.isTerminated()) {
                Logger.getLogger(BackgroundTaskManager.class.getName()).log(Level.INFO, "Waiting agents to shutdown..");
                Logger.getLogger(BackgroundTaskManager.class.getName()).log(Level.SEVERE, "Exception: Waiting agents to shutdown..");
            }

            executor = null;
        }
        executor = new ScheduledThreadPoolExecutor(MAXIMUM_CURRENT);
        //long oneDayDelay = TimeUnit.HOURS.toMillis(24);
        long I1 = initialDelay;
        long I2 = initialDelay + 30;// (accountantPeriod * 2);//(period/2);

        String refreshPeriod = System.getProperty("com.san.rules.engine.reload_period");
        
        try {
            refreshPeriod = (String) (new InitialContext().lookup("java:comp/env/com.san.rules.engine.reload_period"));  // from Tomcat's server.xml
        } catch (NamingException ex) {
            Logger.getLogger(BackgroundTaskManager.class.getName()).log(Level.SEVERE, null, ex);
        }
         
        Logger.getLogger(Accountant.class
                .getName()).log(Level.INFO, "com.san.rules.engine.reload_period=" + refreshPeriod);
        if (refreshPeriod!=null)
       period = Integer.parseInt(refreshPeriod) ;
    
        cachedRepositoryFuture = executor.scheduleWithFixedDelay(new CachedRepositoryService(),
                I1, period, TimeUnit.SECONDS);
        droolsUpdaterFuture = executor.scheduleWithFixedDelay(new DroolsUpdaterService(),
                I2, accountantPeriod, TimeUnit.MINUTES);
        PricingLogger pricingLogger = new PricingLogger();
        pricingLoggerFuture = executor.scheduleWithFixedDelay(pricingLogger,
                loggerInitialDelay, loggerPeriod, TimeUnit.MINUTES);
        PricingEngine.setPricingLogger(pricingLogger);
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        restartAgents();
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        executor.shutdown();
        executor = null;
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
