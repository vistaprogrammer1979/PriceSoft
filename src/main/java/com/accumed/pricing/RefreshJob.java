/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing;

import static com.accumed.pricing.cachedRepo.BackgroundTaskManager.RefreshingJobPool;
 import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author waltasseh
 */
public class RefreshJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        RefreshingJobThread job = new RefreshingJobThread();
        RefreshingJobPool.submit(job);
    }

}
