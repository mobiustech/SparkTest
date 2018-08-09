package com.levi.cdh;

import org.apache.http.HttpHost;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;


public class ProductIndexerBatchJob {

    public static void main(String args[]) throws Exception{

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("ProductIndexer").master("local[*]")
                .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider").getOrCreate();

        //Dataset<Row> products = session.read().option("header","true").csv("s3a://levi-cdh-prod/notifications/inbound/products.txt");
        Dataset<Row> products = session.read().option("header","true").csv("in/products.txt");

        products.show(20);

        products.foreach( p -> {
            //System.out.println((String)x.getAs("pc9"));
            XContentBuilder builder = XContentFactory.jsonBuilder();

            //pc9,product_link,price,smdp,hmdp,price_start_tm,price_end_tm,title
            float price = Float.parseFloat(p.getAs("PRICE"));
            float smdp = p.getAs("SOFT_MARKUP") == null ? 0.0f : Float.parseFloat(p.getAs("SOFT_MARKUP"));
            float hmdp = p.getAs("HARD_MARKUP") == null ? 0.0f : Float.parseFloat(p.getAs("HARD_MARKUP"));

            builder.startObject()
                    .field("pc9", (String)p.getAs("PC9"))
                    .field("category", (String)p.getAs("CATEGORY"))
                    .field("product_link", (String)p.getAs("PC9_PAGE_URL"))
                    .field("thumbnail", (String)p.getAs("THUMBNAIL_IMAGE"))
                    .field("price", price)
                    .field("smdp", smdp)
                    .field("hmdp", hmdp)
                    .field("title", (String)p.getAs("TITLE"))
                    .endObject();

            IndexRequest indexRequest = new IndexRequest("products", "product", p.getAs("PC9"))
                    .source(builder.string(), XContentType.JSON);

            bulkProcessor.add(indexRequest);
        });

        bulkProcessor.awaitClose(100, TimeUnit.SECONDS);
        session.close();
    }

    protected static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
//                    new HttpHost("52.38.154.60", 80, "http"),
//                    new HttpHost("52.38.154.60", 81, "http")));
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9200, "http")));

    protected static BulkProcessor bulkProcessor = BulkProcessor.builder(
            client::bulkAsync,
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId,
                                       BulkRequest request) {
                    System.out.println("Request # of actions: " + request.numberOfActions());
                }

                @Override
                public void afterBulk(long executionId,
                                      BulkRequest request,
                                      BulkResponse response) {
                    System.out.println("Bulk execution completed ["+  executionId + "].\n" +
                            "Took (ms): " + response.getTook() + "\n" +
                            "Failures: " + response.hasFailures() + "\n" +
                            "Count: " + response.getItems().length);

                    Iterator<BulkItemResponse> it = response.iterator();
                    while(it.hasNext()) {
                        //System.out.println(it.next().getFailureMessage());
                        System.out.println(it.next().getFailure().getCause());
                    }

                }

                @Override
                public void afterBulk(long executionId,
                                      BulkRequest request,
                                      Throwable failure) {
                    System.out.println("Bulk execution failed ["+  executionId + "].\n" +
                            failure.toString());

                    System.out.println(failure.getMessage());
                }
            })
            .setBulkActions(500)
            .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setConcurrentRequests(1)
            .setBackoffPolicy(
                    BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
            .build();
}
