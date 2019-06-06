package com.dtc.analytic.java.demo;



import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;



/**
 * Created on 2019-06-06
 *
 * @author :hao.li
 */
public class MemoryUsageExtrator {
   static OperatingSystemMXBean system = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    /**
     * Get current free memory size in bytes
     * @return  free RAM size
     */
    public static long currentFreeMemorySizeInBytes() {
        return system.getFreePhysicalMemorySize();
    }
}
