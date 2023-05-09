package org.tools;

import cn.smart.service.IMyJob;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class MyJobUtil {

    public static Object myRunJob(Object group_id, Object job_name, Object ps)
    {
        Ignite ignite = Ignition.ignite();
        IMyJob service = ignite.services().serviceProxy("my_schedule_job_service", IMyJob.class, false);
        return service.myRunJob(ignite.configuration().getConsistentId().toString(), group_id, job_name, ps);
    }
}
