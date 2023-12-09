package com.function;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.microsoft.azure.functions.ExecutionContext;

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

}
