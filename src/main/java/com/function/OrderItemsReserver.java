package com.function;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;

import java.io.IOException;
import java.util.Optional;
import java.util.logging.Logger;


public class OrderItemsReserver {
    private static final String STORAGE_CONNECTION_STRING = System.getenv("STORAGE_CONNECTION_STRING");
    private static final String BLOB_CONTAINER_NAME = System.getenv("BLOB_CONTAINER_NAME");

    private final BlobStorageService blobStorageService;

    public OrderItemsReserver() {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(STORAGE_CONNECTION_STRING)
                .buildClient();
        this.blobStorageService = new BlobStorageService(blobServiceClient);
    }

    @FunctionName("OrderItemsReserver")
    public void run(
            @ServiceBusQueueTrigger(name = "message",
                    queueName = "queue1",
                    connection = "ServiceBusConnectionString") String message,
            final ExecutionContext context) {

        Logger logger = context.getLogger();
        logger.info("Service Bus messages received");

        try {
            // Deserialize the received message to extract order details
            Order order = new ObjectMapper().readValue(message, Order.class);
            logger.info("Deserialize the received message to extract order details : " + order);
            // Upload the order details as a JSON file to Blob Storage using BlobStorageService
//            BlobStorageService blobStorageService = new BlobStorageService(blobServiceClient);
            boolean updated = blobStorageService.updateBlobForSession(BLOB_CONTAINER_NAME, order.getId(), message, context);

            logger.info("OrderItemsReserver: Order details uploaded to Blob Storage for session ID: " + order.getId()
                    + (updated ? " (Updated existing blob)" : " (Created a new blob)"));
        } catch (IOException e) {
            logger.warning("OrderItemsReserver: Error processing the message: " + e.getMessage());
            // Implement retry logic here if needed
        }
    }
}
