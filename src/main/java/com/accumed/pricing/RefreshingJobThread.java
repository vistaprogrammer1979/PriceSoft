/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing;

import static com.accumed.pricing.PricingEngine.accountantPool;
import static com.accumed.pricing.PricingEngine.cachedRepositoryService;

/**
 *
 * @author Jalal Issa
 */
public class RefreshingJobThread implements Runnable {

    public RefreshingJobThread() {
    }

    @Override
    public void run() {
        cachedRepositoryService.checkSynchronizationJob();
        accountantPool.refreshRepository(cachedRepositoryService.getRepo());

    }
}
