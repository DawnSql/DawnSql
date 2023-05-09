package org.gridgain.myservice;

import cn.myservice.MyLoadCronService;
import cn.smart.service.IMyJob;
import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import java.io.Serializable;

public class MyJobService implements IMyJob, Service, Serializable {

    private static final long serialVersionUID = -4195983952031917580L;

    @IgniteInstanceResource
    private Ignite ignite;

    @Override
    public Object myRunJob(String consistentId, Object group_id, Object job_name, Object ps) {
        if (ignite.configuration().getConsistentId().toString().equals(consistentId))
        {
            return MyLoadCronService.getInstance().getMyLoadCron().myRunJob(ignite, group_id, job_name, ps);
        }
        return null;
    }

    @Override
    public void cancel(ServiceContext serviceContext) {

    }

    @Override
    public void init(ServiceContext serviceContext) throws Exception {

    }

    @Override
    public void execute(ServiceContext serviceContext) throws Exception {

    }
}
