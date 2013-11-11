package com.pannous.es.rollindex;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.core.jmx.JobDataMapSupport.newJobDataMap;

public class RollScheduler {
    private final Scheduler scheduler;
    private final static RollScheduler _instance = new RollScheduler();

    public static RollScheduler getInstance() {
        return _instance;
    }

    private RollScheduler() {
        try {
            scheduler = new StdSchedulerFactory().getScheduler();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void scheduleRollingCron(IndexRoller roller, RollRequest request) {
        final Map<String, Object> jobDataMap = new HashMap<String,Object>();
        jobDataMap.put("request", request);
        jobDataMap.put("roller", roller);

        final JobDetail job = newJob(RollSchedulerJob.class)
            .usingJobData(newJobDataMap(jobDataMap))
            .withIdentity("job1", "group1")
            .build();

        final CronTrigger trigger = newTrigger()
            .withIdentity("trigger_" + request.getIndexPrefix(), "group_" + request.getIndexPrefix())
            .withSchedule(cronSchedule(request.getCronSchedule()))
            .build();

        try {
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

}
