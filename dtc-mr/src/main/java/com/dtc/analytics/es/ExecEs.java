package com.dtc.analytics.es;//package com.demo.es;

/**
 * Copyright @ 2018 lihao.com
 * All right reserved.
 *
 * @author Li Hao
 * @since 2018/11/14  14:09
 */

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dtc.analytics.es.EsUtils.getEsClient;


/**
 * 员工增删改查的应用程序
 *
 * @author Administrator
 */
public class ExecEs {
    private static TransportClient client;
    public String ClusterName = "common_test";
    public final String HOST = "10.3.6.7,10.3.6.12,10.3.6.16";// 服务器地址，本机
    public final int PORT = 9300; // http请求的端口是9200，客户端是9300
    static Logger log = LoggerFactory.getLogger(ExecEs.class);

    public ExecEs() {
        client = getEsClient(ClusterName, HOST, PORT);
    }

    public static void main(String[] args) throws Exception {
        log.info("Hourly .... ");
        ExecEs em = new ExecEs();
        // 先构建client
//        em.createEmployee(client);
//        em.getData(client, "bank", "Hourly", "EsjqB2oBJVxvoYA5FeVH");
//        em.allquery(client, "bank", "Hourly");
        em.myMoreQuery(client, "bank", "Hourly");
//        getData(client);
//        updateEmployee(client);
//        deleteEmployee(client);

        client.close();
    }


    /**
     * 创建员工信息（创建一个document）
     *
     * @param client
     */
    private void createEmployee(TransportClient client) throws Exception {
        IndexResponse response = client.prepareIndex("company", "employee", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 10000)
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }

    /**
     * 获取员工信息
     *
     * @param client
     * @throws Exception
     */
    private void getData(TransportClient client, String Index, String type, String id) {
        GetResponse response = client.prepareGet(Index, type, id).get();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 查询所有数据,默认10条
     */

    public void allquery(TransportClient client, String index, String type) throws Exception {
        QueryBuilder qb = QueryBuilders.matchAllQuery();
//        SearchResponse response = client.prepareSearch(index).setTypes(type).setSize(100).setQuery(qb).get();
        SearchResponse response = client.prepareSearch(index).setTypes(type).setQuery(qb).get();
        System.out.println("length: "+ response.getHits().getHits().length );
        if(response.getHits().getTotalHits() != 0) {
            for (SearchHit hit : response.getHits().getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        }
//        SearchHits hits = response.getHits();
//        for (SearchHit hit : hits) {
//            //整个文档输出
//            System.out.println(hit.getSourceAsString());
//            //文档中每个字段数据
//            Map<String, Object> map = hit.getSourceAsMap();
//            for (String key : map.keySet()) {
//                System.out.println(key + "=" + map.get(key));
//            }
//        }
    }

    /**
     *
     * Match Query 查询单一条件的数据
    * */

    public void myMatchQuery(TransportClient client,String index, String type) throws Exception {
        //name:key,text:value
        QueryBuilder qb = QueryBuilders.matchQuery("gender","M");

        SearchResponse response = client.prepareSearch(index).setTypes(type).setQuery(qb).get();
        System.out.println("length: "+response.getHits().getHits().length );
        if(response.getHits().getTotalHits() != 0) {
            for (SearchHit hit : response.getHits().getHits()) {
                System.out.println(hit.getScore()+" --> "+hit.getSourceAsString());
            }

        }
    }
    /**
     *
     * 多条件查询
     *
     * */
    /**
     * 使用QueryBuilder
     * termQuery("key", obj) 完全匹配
     * termsQuery("key", obj1, obj2..)   一次匹配多个值
     * matchQuery("key", Obj) 单个匹配, field不支持通配符, 前缀具高级特性
     * multiMatchQuery("text", "field1", "field2"..);  匹配多个字段, field有通配符忒行
     * matchAllQuery();         匹配所有文件
     */

    public void myMoreQuery(TransportClient client,String index, String type) throws Exception{
        MatchPhraseQueryBuilder mpq1 = QueryBuilders
                .matchPhraseQuery("gender","M");
        MatchPhraseQueryBuilder mpq2 = QueryBuilders
                .matchPhraseQuery("state","ID");
//        QueryBuilders.

        QueryBuilder qb2 = QueryBuilders.boolQuery()
                .must(mpq1)
                .must(mpq2);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(qb2);

        //查询建立
        SearchRequestBuilder responsebuilder = client
                .prepareSearch(index).setTypes(type);
        SearchResponse myresponse=responsebuilder
                .setQuery(qb2)
                .setFrom(0).setSize(50)
//                .addSort("inputtime", SortOrder.ASC)
                //.addSort("inputtime", SortOrder.DESC)
                .setExplain(true).execute().actionGet();
        SearchHits hits = myresponse.getHits();
        for(int i = 0; i < hits.getHits().length; i++) {
            System.out.println(hits.getHits()[i].getSourceAsString());

        }
    }



    /**
     * 修改员工信息
     *
     * @param client
     * @throws Exception
     */
    private static void updateEmployee(TransportClient client) throws Exception {
        UpdateResponse response = client.prepareUpdate("company", "employee", "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("position", "technique manager")
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }

    /**
     * 删除 员工信息
     *
     * @param client*
     * @throws Exception
     */
    private static void deleteEmployee(TransportClient client) throws Exception {
        DeleteResponse response = client.prepareDelete("company", "employee", "1").get();
        System.out.println(response.getResult());
    }

}
