package com.example.fn;

import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.PutMessagesResult;
import com.oracle.bmc.streaming.model.PutMessagesResultEntry;
import com.oracle.bmc.streaming.model.StreamSummary;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Producer {
    private StreamClient streamClient = null;
    final ResourcePrincipalAuthenticationDetailsProvider provider
            = ResourcePrincipalAuthenticationDetailsProvider.builder().build();

    public Producer() {
    }

    public String produce(Message msg) {
        String result = null;

        try {

            String streamOCID = "ocid1.stream.oc1.iad.amaaaaaanamaaaaaanamaaaaaanamaaaaaanamaaaaaanamaaaaaan";
            System.out.println("Found stream with OCID -- " + streamOCID);

            String streamClientEndpoint = "https://xxxxxxxxxxxx.streaming.us-ashburn-1.oci.oraclecloud.com";
            System.out.println("Stream client endpoint " + streamClientEndpoint);

            streamClient = new StreamClient(provider);
            streamClient.setEndpoint(streamClientEndpoint);

            PutMessagesDetails putMessagesDetails
                    = PutMessagesDetails.builder()
                    .messages(Arrays.asList(PutMessagesDetailsEntry.builder().key(msg.key.getBytes()).value(msg.value.getBytes()).build()))
                    .build();

            PutMessagesRequest putMessagesRequest
                    = PutMessagesRequest.builder()
                    .putMessagesDetails(putMessagesDetails)
                    .streamId(streamOCID)
                    .build();

            PutMessagesResult putMessagesResult = streamClient.putMessages(putMessagesRequest).getPutMessagesResult();
            System.out.println("pushed messages...");

            for (PutMessagesResultEntry entry : putMessagesResult.getEntries()) {
                if (entry.getError() != null) {
                    result = "Put message error " + entry.getErrorMessage();
                    System.out.println(result);
                } else {
                    result = "Message pushed to offset " + entry.getOffset() + " in partition " + entry.getPartition();
                    System.out.println(result);
                }
            }
        } catch (Exception e) {
            result = "Error occurred - " + e.getMessage();

            System.out.println(result);
        }

        return result;
    }

    public static class Message {

        private String key;
        private String value;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

    }

}