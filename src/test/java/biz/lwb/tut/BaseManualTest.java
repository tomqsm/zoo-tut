package biz.lwb.tut;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Before;

public class BaseManualTest {
    @Before
    public void setup() {
        org.apache.log4j.BasicConfigurator.configure();
    }

    protected CuratorFramework newClient() {
        int sleepMsBetweenRetries = 500;
        int maxRetries = 3;
        RetryPolicy retryPolicy = new RetryNTimes(maxRetries, sleepMsBetweenRetries);
        return newClient(retryPolicy);
    }

    protected CuratorFramework newClient(RetryPolicy retryPolicy) {
        return CuratorFrameworkFactory.newClient("0.0.0.0:2181", retryPolicy);
    }
}
