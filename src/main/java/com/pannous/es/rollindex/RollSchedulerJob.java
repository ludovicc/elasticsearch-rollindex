package com.pannous.es.rollindex;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.util.Map;

public class RollSchedulerJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        final Map<String, Object> jobDataMap = context.getMergedJobDataMap();

        final RollRequest request = (RollRequest) jobDataMap.get("request");
        final IndexRoller indexRoller = (IndexRoller) jobDataMap.get("roller");

        if (request != null && indexRoller != null)
            try {
                indexRoller.rollIndex(request);
            } catch (IOException e) {
                throw new JobExecutionException(e);
            }
        else
            throw new JobExecutionException("Neither 'request' nor 'roller' may be null.");
    }
}
