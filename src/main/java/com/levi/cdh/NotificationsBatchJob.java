package com.levi.cdh;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.levi.model.notifications.Notification;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Encoder;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

import com.levi.model.notifications.Product;

public class NotificationsBatchJob {

    private static final Logger LOGGER = Logger.getLogger(NotificationsBatchJob.class);

    public static void main (String args[]){

        Logger.getLogger("org").setLevel(Level.INFO);

//        SparkConf conf = new SparkConf().setAppName("NotifyOnSale")
//                .setMaster("spark://ip-10-248-255-75.us-west-2.compute.internal:7077").getOrCreate();
//
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        LOGGER.info("Context: " + sc.appName());
//        System.out.println("Context: " + sc.appName());
        SparkSession session = SparkSession.builder().appName("NotifyOnSale")
                .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
                //.master("spark://ip-10-248-255-75.us-west-2.compute.internal:7077")
                .master("spark://10.248.255.75:7077")
                //.master("local[*]")
                .getOrCreate();

        Dataset<Row> products = session.read().option("header","true").csv("s3a://levi-cdh-prod/notifications/inbound/products.txt");
        products.show(20);

        //Dataset<Row> products = session.read().option("header","true").csv("in/products.txt");
//        Dataset<Row> consumers = session.read().option("header","true").csv("in/consumers.txt");
//
//        Dataset<Row> joinObjectDS = consumers.join(products, "PC9");
//
//        LOGGER.debug("============= joinObjectDS =============");
//        joinObjectDS.show(20);
//
//        //save notifications
//        Encoder<Notification> noteEncoder = Encoders.bean(Notification.class);
//
//        Dataset<Notification> noteDs = joinObjectDS.map((MapFunction<Row, Notification>) row -> {
//
//            ObjectMapper om = new ObjectMapper();
//
//            //File header
//            //"PC9","CATEGORY","TITLE","PRICE","SOFT_MARKUP","HARD_MARKUP","THUMBNAIL_IMAGE","PC9_PAGE_URL"
//            Product p = Product.builder()
//                    .pc9(row.getAs("PC9"))
//                    .category(row.getAs("CATEGORY"))
//                    .product_title(row.getAs("TITLE"))
//                    .price(new BigDecimal(row.getAs("PRICE").toString()))
//                    .hmdp(row.getAs("HARD_MARKUP") == null ? new BigDecimal(0) : new BigDecimal(row.getAs("HARD_MARKUP").toString()))
//                    .smdp(row.getAs("SOFT_MARKUP") == null ? new BigDecimal(0) : new BigDecimal(row.getAs("SOFT_MARKUP").toString()))
//                    .small_image_link(row.getAs("THUMBNAIL_IMAGE"))
//                    .product_link("http://www.levi.com/US/en_US/levi/p/" + row.getAs("PC9"))
//                    .build();
//
//            Notification note = new Notification();
//            note.setCdh_consumer_id(row.getAs("MCVISID"));
//            note.setNotification_type("SALE");
//            note.setNotification_id(Integer.toString(Math.abs((row.getAs("MCVISID") + "SALE" + row.getAs("PC9")).hashCode())));
//            note.setNotification_date_time(CDHDateUtil.CDH_NANO_DATE_FORMATTER.format(ZonedDateTime.now()));
//
//            note.setNotification_object_str(om.writeValueAsString(p));
//            note.setNotification_object_id(row.getAs("PC9"));
//            return note;
//
//        }, noteEncoder);
//
//        System.out.println("=== NoteDS Dataset ===");
//        noteDs.show(20);
//
//        List<Dataset<Notification>> notesList = noteDs.randomSplitAsList(new double[]{.2, .2, .2, .2, .2}, 100);
//        for(Dataset<Notification> notes : notesList){
//            DynamoDBBatchWriteDelegate dynamoDelegate = new DynamoDBBatchWriteDelegate();
//            List<Object> objList = new LinkedList<>(notes.collectAsList());
//            dynamoDelegate.writeMultipleObjectBatchWrite(objList);
//        }

        session.stop();

    }
}