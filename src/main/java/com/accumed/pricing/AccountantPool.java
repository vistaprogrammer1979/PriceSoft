/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing;

import com.accumed.pricing.cachedRepo.CachedRepository;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author smutlak
 */
public class AccountantPool extends ObjectPool<Accountant> {

    private boolean batchInsert;

    public AccountantPool(boolean batchInsert) {
        super();
        this.batchInsert = batchInsert;
    }

    public boolean isBatchInsert() {
        return batchInsert;
    }

    public void setBatchInsert(boolean batchInsert) {
        this.batchInsert = batchInsert;
    }

    @Override
    protected Accountant create(CachedRepository repo) {
        long lBegin = System.nanoTime();
        Accountant Accountant = new Accountant();
        Logger.getLogger(AccountantPool.class.getName()).log(Level.INFO, "sout create initialize");
        Accountant.initialize(repo);
        return Accountant;
    }

    @Override
    public boolean isValid(Accountant o) {
        return o.isValid();
    }

    @Override
    public void expire(Accountant o) {

        o.dispose();
        Logger.getLogger(AccountantPool.class.getName()).log(Level.INFO, "Destroy Accountant Session");
    }

    @Override
    public boolean checkPackageModification(Accountant o) {
        return o.isPackageModified();
    }

    @Override
    public void setSynchronized(Accountant o, boolean bSynchronized) {
        o.setSynchronized(bSynchronized);
    }

    @Override
    public boolean isSynchronized(Accountant o) {
        return o.isSynchronized();
    }

    public String getRulesPackage() {
        return Accountant.getPackage_fileName();
    }

    public Long getRulesPackageTime() {
        return Accountant.getLoadingTime();
    }

   

}
