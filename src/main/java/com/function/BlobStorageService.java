package com.function;

import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.microsoft.azure.functions.ExecutionContext;

import java.time.Duration;
import java.util.logging.Logger;

public class BlobStorageService {
    private final BlobServiceClient blobServiceClient;

    public BlobStorageService(BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }

    public boolean updateBlobIfExist(String containerName, String blobName, String data, ExecutionContext context) {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        containerClient.createIfNotExists();

        BlobClient blobClient = containerClient.getBlobClient(blobName);

        Logger logger = context.getLogger();
        logger.info("Client Blob initialized");
        // Check if the blob already exists
        if (blobClient.exists()) {
            // If it exists, update the blob with new data
            blobClient.upload(BinaryData.fromString(data), true); // Overwrite the existing blob
            logger.info("Indicate that the update was successful");
            return true; // Indicate that the update was successful
        } else {
            // Blob doesn't exist, hence create it
            blobClient.upload(BinaryData.fromString(data));
            logger.info("Indicate that a new blob was created");
            return false; // Indicate that a new blob was created
        }
    }

    public boolean updateBlobForSession(String containerName, String sessionId, String data, ExecutionContext context) {
        // Generate a unique filename based on customer's session ID
        Logger logger = context.getLogger();

        String blobName = sessionId + ".json";
        logger.info("unique filename based " + blobName);
        // Use the updateBlobIfExist method to update or create the blob
        return updateBlobIfExist(containerName, blobName, data, context);
    }
    public boolean updateBlobForSessionWithRetry(String containerName, String sessionId, String data, ExecutionContext context) {
        // Set up exponential backoff retry options for blob storage upload (customize according to your needs)
        ExponentialBackoffOptions exponentialBackoffOptions = new ExponentialBackoffOptions();

        exponentialBackoffOptions.setMaxDelay(Duration.ofMinutes(5));
        exponentialBackoffOptions.setMaxRetries(3);

        RetryOptions retryOptions = new RetryOptions(exponentialBackoffOptions);

        Logger logger = context.getLogger();
        int retryCount = 0;
        boolean uploadSuccessful = false;

        while (retryCount < retryOptions.getExponentialBackoffOptions().getMaxRetries() && !uploadSuccessful) {
            try {
                uploadSuccessful = updateBlobForSession(containerName, sessionId, data, context);

                if (!uploadSuccessful) {
                    // Increment retry count
                    retryCount++;

                    // Apply exponential backoff delay
                    try {
                        Thread.sleep(exponentialBackoffOptions.getMaxDelay().toMillis());
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }

                    logger.warning("Blob Storage upload retry attempt: " + retryCount);
                }
            } catch (Exception e) {
                // Log any exceptions during retry attempts
                logger.warning("Error during Blob Storage upload retry attempt: " + e.getMessage());
            }
        }

        return uploadSuccessful;
    }
}
