package it.gov.pagopa.nodoverifykototablestorage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.azure.core.util.BinaryData;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import it.gov.pagopa.nodoverifykototablestorage.exception.AppException;
import it.gov.pagopa.nodoverifykototablestorage.util.LogHandler;
import it.gov.pagopa.nodoverifykototablestorage.util.TestUtil;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NodoVerifyKOEventToTableStorageTest {

    @SuppressWarnings("unchecked")
    @Test
    @SneakyThrows
    void runOk() {

        TableServiceClient tableServiceClient = mock(TableServiceClient.class);
        BlobServiceClient blobServiceClient = mock(BlobServiceClient.class);
        BlobContainerClient blobContainerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        TableClient tableClient = mock(TableClient.class);
        try (
                MockedConstruction<BlobServiceClientBuilder> blobServiceClientBuilder = Mockito.mockConstruction(BlobServiceClientBuilder.class, (mock, context) -> {
                    when(mock.connectionString(any())).thenReturn(mock);
                    when(mock.buildClient()).thenReturn(blobServiceClient);
                });
                MockedConstruction<TableServiceClientBuilder> tableServiceClientBuilder = Mockito.mockConstruction(TableServiceClientBuilder.class, (mock, context) -> {
                    when(mock.connectionString(any())).thenReturn(mock);
                    when(mock.buildClient()).thenReturn(tableServiceClient);
                });
        ) {

            // mocking objects
            ExecutionContext context = mock(ExecutionContext.class);
            Logger logger = Logger.getLogger("NodoVerifyKOEventToDataStore-test-logger");
            LogHandler logHandler = new LogHandler();
            logger.addHandler(logHandler);
            when(context.getLogger()).thenReturn(logger);
            String storageAccount = "mockstorageaccount";
            when(blobServiceClient.createBlobContainerIfNotExists(any())).thenReturn(blobContainerClient);
            when(blobContainerClient.getBlobClient(anyString())).thenReturn(blobClient);
            when(tableServiceClient.getTableClient(any())).thenReturn(tableClient);
            when(blobContainerClient.getAccountName()).thenReturn(storageAccount);
            when(tableClient.submitTransaction(anyList())).thenReturn(null);
            ArgumentCaptor<BinaryData> blobCaptor = ArgumentCaptor.forClass(BinaryData.class);
            ArgumentCaptor<List<TableTransactionAction>> transactionCaptor = ArgumentCaptor.forClass(List.class);

            // generating input
            String eventInStringForm1 = TestUtil.readStringFromFile("events/event_ok_1.json");
            List<String> events = new ArrayList<>();
            events.add(eventInStringForm1);
            String eventInStringForm2 = TestUtil.readStringFromFile("events/event_ok_2.json");
            events.add(eventInStringForm2);
            Map<String, Object>[] properties = new HashMap[2];
            properties[0] = new HashMap<>();
            properties[0].put("prop1_without_dash", true);
            properties[0].put("prop1-with-dash", "1");
            properties[1] = new HashMap<>();
            properties[1].put("prop2_without_dash", false);
            properties[1].put("prop2-with-dash", "2");

            // generating expected output on event 1
            String partitionKey1 = "2023-12-12";
            String rowKey1 = "1702406079-uuid-001";
            int size1 = BinaryData.fromStream(new ByteArrayInputStream(eventInStringForm1.getBytes(StandardCharsets.UTF_8))).toString().length();
            Map<String, Object> expectedEvent1 = new HashMap<>();
            expectedEvent1.put("PartitionKey", partitionKey1);
            expectedEvent1.put("RowKey", rowKey1);
            expectedEvent1.put("timestamp", 1702406079L);
            expectedEvent1.put("dateTime", "2023-12-12T18:34:39.860654");
            expectedEvent1.put("noticeNumber", "302040000090000000");
            expectedEvent1.put("idPA", "77777777777");
            expectedEvent1.put("idPsp", "88888888888");
            expectedEvent1.put("idStation", "77777777777_01");
            expectedEvent1.put("idChannel", "88888888888_01");
            expectedEvent1.put("prop1_without_dash", true);
            expectedEvent1.put("prop1WithDash", "1");
            expectedEvent1.put("blobBodyRef", "{\"storageAccount\":\"" + storageAccount + "\",\"containerName\":\"null\",\"fileName\":\"" + rowKey1 + "\",\"fileLength\":" + size1 + "}");

            // generating expected output on event 2
            String partitionKey2 = "2023-12-13";
            String rowKey2 = "1702483842-uuid-002";
            int size2 = BinaryData.fromStream(new ByteArrayInputStream(eventInStringForm2.getBytes(StandardCharsets.UTF_8))).toString().length();
            Map<String, Object> expectedEvent2 = new HashMap<>();
            expectedEvent2.put("PartitionKey", partitionKey2);
            expectedEvent2.put("RowKey", rowKey2);
            expectedEvent2.put("timestamp", 1702483842L);
            expectedEvent2.put("dateTime", "2023-12-13T16:10:42.906415");
            expectedEvent2.put("noticeNumber", "302040000090000001");
            expectedEvent2.put("idPA", "77777777777");
            expectedEvent2.put("idPsp", "88888888888");
            expectedEvent2.put("idStation", "77777777777_01");
            expectedEvent2.put("idChannel", "88888888888_01");
            expectedEvent2.put("prop2_without_dash", false);
            expectedEvent2.put("prop2WithDash", "2");
            expectedEvent2.put("blobBodyRef", "{\"storageAccount\":\"" + storageAccount + "\",\"containerName\":\"null\",\"fileName\":\"" + rowKey2 + "\",\"fileLength\":" + size2 + "}");

            // merging events
            List<Object> expectedEventsToPersist = List.of(expectedEvent1, expectedEvent2);

            // execute logic
            NodoVerifyKOEventToTableStorage function = new NodoVerifyKOEventToTableStorage();
            function.processNodoVerifyKOEvent(events, properties, context);

            // test assertion for data persistence execution
            verify(blobClient, times(2)).upload(blobCaptor.capture(), anyBoolean());
            verify(tableClient, times(2)).submitTransaction(transactionCaptor.capture());

            // test assertion for blob storing
            List<BinaryData> binaryData = blobCaptor.getAllValues();
            assertNotNull(binaryData);
            assertEquals(2, binaryData.size());
            assertEquals(eventInStringForm1, binaryData.get(0).toString());
            assertEquals(eventInStringForm2, binaryData.get(1).toString());

            // test assertion for transaction storing
            List<List<TableTransactionAction>> transactions = transactionCaptor.getAllValues();
            assertNotNull(transactions);
            assertFalse(transactions.isEmpty());
            assertEquals(expectedEventsToPersist.size(), transactions.size());
            assertEquals(partitionKey1, transactions.get(0).get(0).getEntity().getPartitionKey());
            assertEquals(rowKey1, transactions.get(0).get(0).getEntity().getRowKey());
            assertEquals(convertWithStream(expectedEvent1), convertWithStream(transactions.get(0).get(0).getEntity().getProperties()));
            assertEquals(partitionKey2, transactions.get(1).get(0).getEntity().getPartitionKey());
            assertEquals(rowKey2, transactions.get(1).get(0).getEntity().getRowKey());
            assertEquals(convertWithStream(expectedEvent2), convertWithStream(transactions.get(1).get(0).getEntity().getProperties()));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    @SneakyThrows
    void runKo_invalidNumberOfProperties() {
        // mocking objects
        ExecutionContext context = mock(ExecutionContext.class);
        Logger logger = Logger.getLogger("NodoVerifyKOEventToTableStorage-test-logger");
        LogHandler logHandler = new LogHandler();
        logger.addHandler(logHandler);
        when(context.getLogger()).thenReturn(logger);

        // generating input
        String eventInStringForm = TestUtil.readStringFromFile("events/event_ok_1.json");
        List<String> events = new ArrayList<>();
        events.add(eventInStringForm);
        Map<String, Object>[] properties = new HashMap[2];
        properties[0] = new HashMap<>();
        properties[0].put("prop1_without_dash", true);
        properties[0].put("prop1-with-dash", "1");
        properties[1] = new HashMap<>();
        properties[1].put("prop1_without_dash", false);
        properties[1].put("prop1-with-dash", "2");

        // execute logic
        NodoVerifyKOEventToTableStorage function = new NodoVerifyKOEventToTableStorage();
        assertThrows(AppException.class, () -> function.processNodoVerifyKOEvent(events, properties, context));

        // test assertion
        assertTrue(logHandler.getLogs().contains("Error processing events, lengths do not match: [events: 1 - properties: 2]"));
    }

    @SuppressWarnings("unchecked")
    @Test
    @SneakyThrows
    void runKo_missingFaultBeanTimestamp() {
        // mocking objects
        ExecutionContext context = mock(ExecutionContext.class);
        Logger logger = Logger.getLogger("NodoVerifyKOEventToTableStorage-test-logger");
        LogHandler logHandler = new LogHandler();
        logger.addHandler(logHandler);
        when(context.getLogger()).thenReturn(logger);

        // generating input
        String eventInStringForm = TestUtil.readStringFromFile("events/event_ko_1.json");
        List<String> events = new ArrayList<>();
        events.add(eventInStringForm);
        Map<String, Object>[] properties = new HashMap[1];
        properties[0] = new HashMap<>();
        properties[0].put("prop1_without_dash", true);
        properties[0].put("prop1-with-dash", "1");

        // execute logic
        NodoVerifyKOEventToTableStorage function = new NodoVerifyKOEventToTableStorage();
        assertThrows(AppException.class, () -> function.processNodoVerifyKOEvent(events, properties, context));

        // test assertion
        assertTrue(logHandler.getLogs().contains("java.lang.IllegalStateException"));
    }

    @Test
    @SneakyThrows
    void runKo_genericError() {
        // mocking objects
        ExecutionContext context = mock(ExecutionContext.class);
        Logger logger = Logger.getLogger("NodoVerifyKOEventToDataStore-test-logger");
        LogHandler logHandler = new LogHandler();
        logger.addHandler(logHandler);
        when(context.getLogger()).thenReturn(logger);

        // generating input
        String eventInStringForm = TestUtil.readStringFromFile("events/event_ok_1.json");
        List<String> events = new ArrayList<>();
        events.add(eventInStringForm);

        // execute logic
        NodoVerifyKOEventToTableStorage function = spy(NodoVerifyKOEventToTableStorage.class);
        assertThrows(AppException.class, () -> function.processNodoVerifyKOEvent(events, null, context));

        // test assertion
        assertTrue(logHandler.getLogs().contains("[ALERT][VerifyKOToTS] AppException - Generic exception on table storage nodo-verify-ko-events msg ingestion"));
    }

    public String convertWithStream(Map<String, Object> mapToConvert) {
        return new TreeMap<>(mapToConvert).entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
