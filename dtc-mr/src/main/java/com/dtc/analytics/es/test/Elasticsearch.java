package com.dtc.analytics.es.test;

/**
 * Created on 2019-05-10
 *
 * @author :hao.li
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Elasticsearch {
    private static final ObjectMapper objectMapper = ObjectMapperFactory.getDefaultMapper();

    TransportClient client = null;
    private IndicesAdminClient adminClient;

    /**
     * 集群配置初始化方法
     *
     * @throws Exception
     */
    private void init() throws Exception {
        Properties properties = readElasticsearchConfig();
        String clusterName = properties.getProperty("clusterName");
        String[] inetAddresses = properties.getProperty("hosts").split(",");
        int port = Integer.parseInt(properties.getProperty("port", "9300"));
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true)
                .build();
        client = new PreBuiltTransportClient(settings);
        TransportAddress transportAddress;
        for (String host : inetAddresses) {
            transportAddress = new TransportAddress(InetAddress.getByName(host), port);
            client.addTransportAddresses(transportAddress);
        }
    }

    /**
     * 构造方法
     */
    public Elasticsearch() {
        try {
            init();
        } catch (Exception e) {
            System.out.println("initHBase() exception!");
            e.printStackTrace();
        }
        adminClient = client.admin().indices();
    }

    /**
     * 判断集群中{index}是否存在
     *
     * @param index
     * @return 存在（true）、不存在（false）
     */
    public boolean indexExists(String index) {
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = adminClient.exists(request).actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }

    /**
     * 读取es配置信息
     *
     * @return
     */
    private Properties readElasticsearchConfig() {
        Properties properties = new Properties();
        try {
//            InputStream is = this.getClass().getClassLoader().getResourceAsStream("elasticsearch.properties");
            InputStream is = new FileInputStream(new File("/Users/lixuewei/workspace/private/dtcanalytics/dtc-mr/src/main/java/com/dtc/analytics/es/test/elasticsearch.properties"));
            properties.load(new InputStreamReader(is, "UTF-8"));
        } catch (IOException e) {
            System.out.println("readEsConfig exception!");
            e.printStackTrace();
        }
        return properties;
    }

    /**
     * 读取json配置文件
     *
     * @return JSONArray
     * @throws IOException
     */
    public JsonNode readJosnFile() throws IOException {
//        InputStream is = this.getClass().getClassLoader().getResourceAsStream("index.json");
        InputStream is = new FileInputStream(new File("/Users/lixuewei/workspace/private/dtcanalytics/dtc-mr/src/main/java/com/dtc/analytics/es/test/index.json"));
        JsonNode jsonNode = objectMapper.readTree(is);
        String index = jsonNode.get(0).get("index").asText();
        System.out.println(index);
        return jsonNode;
    }

    /**
     * 获取要创建的index列表
     *
     * @param client
     * @return List<Index>
     */
    public List<Index> getIndexList() {

        List<Index> result = new ArrayList<Index>();
        JsonNode jsonArray = null;

        try {
            jsonArray = readJosnFile();
        } catch (IOException e) {
            System.out.println("readJsonFile exception!");
            e.printStackTrace();
        }
        if (jsonArray == null || jsonArray.size() == 0) {
            return null;
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonNode jsonObject = jsonArray.get(i);
            Index indexObject = new Index();
            String index = jsonObject.path("index").asText();
            String type = jsonObject.path("type").asText();
            Integer number_of_shards = jsonObject.get("number_of_shards").asInt();
            Integer number_of_replicas = jsonObject.get("number_of_replicas").asInt();
            String fieldRType = jsonObject.get("fieldType").asText();
            indexObject.setIndex(index);
            indexObject.setType(type);
            indexObject.setFieldJson(fieldRType);
            indexObject.setNumber_of_shards(number_of_shards);
            indexObject.setNumber_of_replicas(number_of_replicas);
            result.add(indexObject);
        }
        return result;
    }

    /**
     * 创建Index
     *
     * @param client
     */
    public void CreateIndex() {
        int i = 0;
        List<Index> list = getIndexList();
        IndicesAdminClient adminClient = client.admin().indices();
        for (Index index : list) {
            if (indexExists(index.getIndex())) {
                continue;
            }
            adminClient.prepareCreate(index.getIndex())
                    .setSettings(Settings.builder().put("index.number_of_shards", index.getNumber_of_shards()).put("index.number_of_replicas", index.getNumber_of_replicas()))
                    .addMapping(index.getType(), index.getFieldJson())
                    .get();
            i++;
        }
        System.out.println("index creation success! create " + i + " index");

    }

    public static void main(String[] args) throws IOException {
        Elasticsearch es = new Elasticsearch();
       es.CreateIndex();
    }
}


