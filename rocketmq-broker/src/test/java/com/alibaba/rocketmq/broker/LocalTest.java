package com.alibaba.rocketmq.broker;

import com.alibaba.rocketmq.store.StoreUtil;
import org.junit.Test;

/**
 * Created by lianbin.wang on 2016/7/22.
 */
public class LocalTest {

    @Test
    public void testGetTotalPhysicalMemorySize() {

        System.out.println(StoreUtil.getTotalPhysicalMemorySize());
    }
}
