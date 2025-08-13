/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing;

import com.accumed.pricing.cachedRepo.CachedData;
import com.accumed.pricing.cachedRepo.CachedRepository;
import com.accumed.pricing.model.CusContract;
import com.accumed.pricing.model.CusPriceListItem;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author smutlak
 */
public abstract class ObjectPool<T> {

    private long expirationTime;
    private ConcurrentHashMap<T, Long> locked, unlocked;

    public ObjectPool() {

        expirationTime = 3600000; // 3600 seconds- 1hour
        locked = new ConcurrentHashMap();
        unlocked = new ConcurrentHashMap();
    }

    protected abstract T create(CachedRepository repo);

    public abstract boolean isValid(T o);

    public abstract void expire(T o);

    public abstract boolean checkPackageModification(T o);

    public abstract void setSynchronized(T t, boolean bSynchronized);

    public abstract boolean isSynchronized(T t);

    public void setSynchronized(boolean bSynchronized) {
        printPoolStatus("setSynchronized_start");
        T t;
        {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                setSynchronized(t, bSynchronized);
            }
        }
        Enumeration<T> e = locked.keys();
        while (e.hasMoreElements()) {
            t = e.nextElement();
            setSynchronized(t, bSynchronized);
        }
        printPoolStatus("setSynchronized_end");
    }
 public synchronized void updateSessionRepository(CachedRepository repo) {
        printPoolStatus("ObjectPool::refreshRepository");
        T t;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                Accountant accountant = (Accountant) t;
//                accountant.updateSession(repo);

            }
        }

        for (Map.Entry<String, CachedData> entry : repo.getCachedDB().entrySet()) {
            CachedData cachedData = entry.getValue();
            for (Object obj : cachedData.getData()) {
                if (cachedData.getLogicalName().startsWith("PL_CUS_CON")) {
                    if (((CusContract) obj).getUpdatedStatus() != null && ((CusContract) obj).getUpdatedStatus().equals(com.accumed.pricing.model.Status.STAGING)) {
                        ((CusContract) obj).setUpdatedStatus(com.accumed.pricing.model.Status.UPDATED);
                        
                    } else if (((CusContract) obj).getUpdatedStatus() != null && ((CusContract) obj).getUpdatedStatus().equals(com.accumed.pricing.model.Status.NEW)) {
                        ((CusContract) obj).setUpdatedStatus(com.accumed.pricing.model.Status.UPDATED);
                        
                    }
                }
                if (cachedData.getLogicalName().startsWith("PL_CUS_PL")) {
                    if (((CusPriceListItem) obj).getUpdatedStatus() != null && ((CusPriceListItem) obj).getUpdatedStatus().equals(com.accumed.pricing.model.Status.STAGING)) {
                        ((CusPriceListItem) obj).setUpdatedStatus(com.accumed.pricing.model.Status.UPDATED);
                        

                    }
                }
                 
            }
        }
        printPoolStatus("ObjectPool::refreshRepository end");
    }
  public synchronized void refreshRepository(CachedRepository repo) {
        printPoolStatus("ObjectPool::refreshRepository");
        T t;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                Accountant accountant = (Accountant) t;
                accountant.reInitialize(repo);

            }
        }
        printPoolStatus("ObjectPool::refreshRepository end");
    }
    public synchronized boolean checkPackagesModification() {
        printPoolStatus("checkPackagesModification_start");
        boolean ret = false;
        T t;
        {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (checkPackageModification(t)) {
                    ret = true;
                }
            }
        }
        Enumeration<T> e = locked.keys();
        while (e.hasMoreElements()) {
            t = e.nextElement();
            if (checkPackageModification(t)) {
                ret = true;
            }
        }
        printPoolStatus("checkPackagesModification_end");
        return ret;
    }

    public synchronized int getCount() {
        printPoolStatus("getCount_start");

        int ret = 0;
        if (unlocked != null) {
            ret += unlocked.keySet().size();
        }
        if (locked != null) {
            ret += locked.keySet().size();
        }

        printPoolStatus("getCount_end");

        return ret;
    }

    public synchronized int getValidCount(boolean includeCheckedOut) {
        printPoolStatus("getValidCount_start");
        int ret = 0;
        T t;
        {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (isValid(t)) {
                    ret++;
                }
            }
        }
        if (includeCheckedOut) {
            Enumeration<T> e = locked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (isValid(t)) {
                    ret++;
                }
            }
        }
        printPoolStatus("getValidCount_end");

        return ret;
    }

    public synchronized void expireAllCheckedIn()  {
        printPoolStatus("expireAllCheckedIn_start");
        /*if (locked.size() > 0) {
            Logger.getLogger(ObjectPool.class
                    .getName()).log(Level.INFO, "cannot expire all, some objectes are still in use.");
            throw new Exception("cannot expire all, some objectes are still in use.");
        }*/
        T t;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                unlocked.remove(t);
                expire(t);
                t = null;
            }
        }
        printPoolStatus("expireAllCheckedIn_end");
    }

    public synchronized void expireAll() throws Exception {
        printPoolStatus("ObjectPool::expireAll");
        if (locked.size() > 0) {
            Logger.getLogger(ObjectPool.class
                    .getName()).log(Level.INFO, "cannot expire all, some objectes are still in use.");
            throw new Exception("cannot expire all, some objectes are still in use.");
        }
        T t;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                unlocked.remove(t);
                expire(t);
                t = null;
            }
        }
        printPoolStatus("ObjectPool::expireAll end");
    }

    public synchronized void expireAndRemove(int maxPoolSize) {
        printPoolStatus("ObjectPool::expireAndRemove");
        if (getCount() > maxPoolSize) {

            int objectTobeRemovedCount = getCount() - maxPoolSize;
            T t;
            if (unlocked.size() > 0) {
                Enumeration<T> e = unlocked.keys();
                while (e.hasMoreElements()) {
                    t = e.nextElement();
                    if (!isSynchronized(t)) {
                        unlocked.remove(t);
                        expire(t);
                        t = null;
                        objectTobeRemovedCount--;
                        if (objectTobeRemovedCount <= 0) {
                            break;
                        }
                    }
                }
            }
        }
        printPoolStatus("ObjectPool::expireAndRemove end");
    }

    public synchronized T checkOut(CachedRepository repo) {
        printPoolStatus("checkOut_start");
        long now = System.currentTimeMillis();
        T t;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                /*if ((now - unlocked.get(t)) > expirationTime) {
                 // object has expired
                 unlocked.remove(t);
                 expire(t);
                 t = null;
                 } else {*/
                if (isValid(t)) {
                    unlocked.remove(t);
                    locked.put(t, now);
                    printPoolStatus("checkOut_return1");
                    Logger.getLogger(ObjectPool.class
                            .getName()).log(Level.INFO, "total lockedAndUnlocked=" + (locked.size() + unlocked.size()));
                    return (t);
                } else {
                    // object failed validation
                    unlocked.remove(t);
                    expire(t);
                    t = null;
                }
                //}
            }
        }
        // no objects available, create a new one
        t = create(repo);
        locked.put(t, now);
        Logger.getLogger(ObjectPool.class
                .getName()).log(Level.INFO, "total lockedAndUnlocked=" + (locked.size() + unlocked.size()));
        printPoolStatus("checkOut_end");
        return (t);
    }

    public synchronized T checkOut_oldest(CachedRepository repo) {
        printPoolStatus("checkOut_oldest_start");
        long now = System.currentTimeMillis();
        long oldTime = 0;
        T t;
        T ret = null;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (unlocked.get(t) < oldTime || oldTime == 0) {
                    oldTime = unlocked.get(t);
                    ret = t;
                }
            }
            /*if (!isValid(ret)) { omit because we do not handle the time
             // object failed validation
             unlocked.remove(ret);
             expire(ret);
             ret = null;
             }*/
            if (ret != null) {
                unlocked.remove(ret);
                locked.put(ret, now);
                printPoolStatus("checkOut_oldest_return1");
                return ret;
            }
        }
        // no objects available, create a new one
        t = create(repo);
        locked.put(t, now);
        printPoolStatus("checkOut_oldest_end");
        return (t);
    }

    public synchronized T checkOut_newest(CachedRepository repo) {
        printPoolStatus("checkOut_newest_start");
        long now = System.currentTimeMillis();
        long newerTime = 0;
        T t;
        T ret = null;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (unlocked.get(t) > newerTime || newerTime == 0) {
                    newerTime = unlocked.get(t);
                    ret = t;
                }
                /*if (!isValid(ret)) { omit because we do not handle the time
                 // object failed validation
                 unlocked.remove(ret);
                 expire(ret);
                 ret = null;
                 }*/
            }
            if (ret != null) {
                unlocked.remove(ret);
                locked.put(ret, now);
                printPoolStatus("checkOut_newest_return1");
                Logger.getLogger(ObjectPool.class
                        .getName()).log(Level.INFO, "total lockedAndUnlocked=" + (locked.size() + unlocked.size()));
                return ret;
            }

        }
        // no objects available, create a new one
        t = create(repo);
        locked.put(t, now);
        printPoolStatus("checkOut_newest_end");
        Logger.getLogger(ObjectPool.class
                .getName()).log(Level.INFO, "total lockedAndUnlocked=" + (locked.size() + unlocked.size()));
        return (t);
    }

    public synchronized T checkout_needsSynchronization() {
        printPoolStatus("checkout_needsSynchronization_start");
        long now = System.currentTimeMillis();
        T t;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (!isSynchronized(t)) {
                    unlocked.remove(t);
                    locked.put(t, now);
                    printPoolStatus("checkout_needsSynchronization_return1");
                    return t;

                }
            }
        }
        printPoolStatus("checkout_needsSynchronization_end");
        return (null);
    }

    public synchronized int getAsynchronizedCount() {
        printPoolStatus("getUnSynchronizedCount");
        int ret = 0;
        long now = System.currentTimeMillis();
        T t;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (!isSynchronized(t)) {
                    ret++;
                }
            }
        }

        if (locked.size() > 0) {
            Enumeration<T> e = locked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (!isSynchronized(t)) {
                    ret++;
                    printPoolStatus("getUnSynchronizedCount");
                }
            }
        }
        printPoolStatus("getUnSynchronizedCount");
        return ret;
    }

    public synchronized int getCheckedoutCount() {
        printPoolStatus("getCheckedoutCount");
//        long now = System.currentTimeMillis();
        return locked.size();
    }

    public synchronized int getSynchronizedCount() {
        printPoolStatus("getUnSynchronizedCount");
        int ret = 0;
        long now = System.currentTimeMillis();
        T t;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (isSynchronized(t)) {
                    ret++;
                }
            }
        }

        if (locked.size() > 0) {
            Enumeration<T> e = locked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if (isSynchronized(t)) {
                    ret++;
                    printPoolStatus("getUnSynchronizedCount");
                }
            }
        }
        printPoolStatus("getUnSynchronizedCount");
        return ret;
    }

    public synchronized void checkIn(T t) {
        printPoolStatus("checkIn_start");
        locked.remove(t);
        unlocked.put(t, System.currentTimeMillis());
        printPoolStatus("checkIn_end");
    }

    public void printPoolStatus(String loc) {
        Logger.getLogger(ObjectPool.class
                .getName()).log(Level.INFO, loc + " Pool Status unlocked.size()=" + unlocked.size() + " locked.size()" + locked.size());
    }
}
