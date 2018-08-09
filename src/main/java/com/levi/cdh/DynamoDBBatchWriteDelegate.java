package com.levi.cdh;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.levi.model.consumer.Profile;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

public class DynamoDBBatchWriteDelegate {

    private static final Logger LOGGER = Logger.getLogger(com.levi.cdh.dao.DynamoDBBatchWriteDelegate.class);

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion("us-west-2").build();

    static DynamoDBMapper mapper = new DynamoDBMapper(client);

    public static void main(String[] args) {

        Profile profile = Profile.builder()
                .cdh_profile_id("1234")
                .profile_type("Experian")
                .cdh_consumer_id("1234")
                .female(true)
                .first_name("Jane")
                .last_name("smith")
                .build();

        List<Object> profiles = new LinkedList<>();
        profiles.add(profile);

        com.levi.cdh.dao.DynamoDBBatchWriteDelegate del = new com.levi.cdh.dao.DynamoDBBatchWriteDelegate();
        del.writeMultipleObjectBatchWrite(profiles);
    }

    public void writeMultipleObjectBatchWrite(List<Object> objectList) {

        System.out.println("Making the request.");
        List<DynamoDBMapper.FailedBatch> failedBatches = mapper.batchSave(objectList);

        for (DynamoDBMapper.FailedBatch failedBatch : failedBatches) {

            if (failedBatch.getUnprocessedItems().size() == 0) {
                LOGGER.debug("No unprocessed items found");
            } else {
                LOGGER.error("Unprocessed items: " + failedBatch.getUnprocessedItems());
            }
        }
    }
}