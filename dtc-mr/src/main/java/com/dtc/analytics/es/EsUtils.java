package com.dtc.analytics.es;

import com.dtc.analytics.mapreduce.HourHdfs2EsMR;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created on 2019-05-10
 *
 * @author :hao.li
 */
public class EsUtils {
    private final static Logger logger = LoggerFactory.getLogger(EsUtils.class);

    public static TransportClient getEsClient(String clusterName, String hosts, int port) {
        TransportClient client = null;
        // 获取es主机中节点的ip地址及端口号
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", clusterName)
                    .put("client.transport.sniff", true)
                    .build();
            client = new PreBuiltTransportClient(settings);
            TransportAddress transportAddress;
            for (String host : hosts.split(",")) {
                transportAddress = new TransportAddress(InetAddress.getByName(host), port);
                client.addTransportAddresses(transportAddress);
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException("Cannot create elasticsearch client,the caues is {}!", e);
        }
        return client;
    }
}
