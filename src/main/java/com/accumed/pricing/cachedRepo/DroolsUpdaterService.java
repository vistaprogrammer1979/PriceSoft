/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.cachedRepo;

import com.accumed.pricing.Accountant;
import com.accumed.pricing.PricingEngine;
import com.accumed.pricing.Utils;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author smutlak
 */
public class DroolsUpdaterService implements Runnable {

    protected static CachedRepositoryService cachedRepositoryService = null;   


    public DroolsUpdaterService() {

    }

    public static void setCachedRepositoryService(CachedRepositoryService cachedRepositoryService) {
        DroolsUpdaterService.cachedRepositoryService = cachedRepositoryService;
    }

     
    @Override
    public void run() {
        Logger.getLogger(DroolsUpdaterService.class.getName()).log(Level.INFO, "Running DroolsUpdaterService...");
        try {
            if (DroolsUpdaterService.cachedRepositoryService == null) {
                Logger.getLogger(DroolsUpdaterService.class.getName()).log(Level.SEVERE, "Cached Repository Service is not set...");
                return;
            }
            CachedRepository repo = DroolsUpdaterService.cachedRepositoryService.getRepo();
            if (repo == null || !repo.isValid()) {
                Logger.getLogger(DroolsUpdaterService.class.getName()).log(Level.SEVERE, "Cached Repository is not set or invalid...");
                return;
            }
            PricingEngine.getAccountantPool().expireAndRemove(9);
            //boolean expire = (PricingEngine.getAccountantPool().getAsynchronizedCount() > 3);
            Accountant accountant = PricingEngine.getAccountantPool().checkout_needsSynchronization();
            if (accountant != null) {
                Logger.getLogger(DroolsUpdaterService.class.getName()).log(Level.INFO, "sout Re-synchronize accountant ID=" + accountant.getId());
                if (!accountant.loadPackage(repo)) {
                    accountant.reInitialize(repo);
                }
                PricingEngine.getAccountantPool().checkIn(accountant);

            } else {//check if rules package is changed
                accountant = PricingEngine.getAccountantPool().checkOut_oldest(repo);
                accountant.loadPackage(repo);
                PricingEngine.getAccountantPool().checkIn(accountant);
            }

            Logger.getLogger(DroolsUpdaterService.class.getName()).log(Level.INFO, "Running DroolsUpdaterService...END");

        } catch (Throwable e) {
            Logger.getLogger(DroolsUpdaterService.class.getName()).log(Level.SEVERE, "Exception {0}{1}", new Object[]{e.toString(), Utils.stackTraceToString(e)});
        }
    }


}
