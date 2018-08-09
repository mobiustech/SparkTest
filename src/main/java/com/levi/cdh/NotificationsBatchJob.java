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

        SparkSession session = SparkSession.builder().appName("NotifyOnSale")
                .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
                .master("spark://10.248.255.75:7077")
                //.master("local[*]")
                .getOrCreate();

        Dataset<Row> products = session.read().option("header","true").csv("s3a://levi-cdh-prod/notifications/inbound/products.txt");
        products.show(20);

        session.stop();

    }
}