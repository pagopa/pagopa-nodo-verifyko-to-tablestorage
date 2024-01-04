package it.gov.pagopa.nodoverifykototablestorage;

import com.azure.core.util.BinaryData;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.data.tables.models.TableTransactionActionType;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import it.gov.pagopa.nodoverifykototablestorage.exception.BlobStorageUploadException;
import it.gov.pagopa.nodoverifykototablestorage.model.BlobBodyReference;
import it.gov.pagopa.nodoverifykototablestorage.util.Constants;
import it.gov.pagopa.nodoverifykototablestorage.util.ObjectMapperUtils;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;

/**
 * Azure Functions with Azure Event Hub trigger.
 * This function will be invoked when an Event Hub trigger occurs
 */
public class NodoVerifyKOEventToTableStorage {

	private static TableServiceClient tableServiceClient = null;

	private static BlobContainerClient blobContainerClient = null;

	@FunctionName("EventHubNodoVerifyKOEventToTSProcessor")
    public void processNodoVerifyKOEvent (
			@EventHubTrigger(
					name = "NodoVerifyKOEvent",
					eventHubName = "", // blank because the value is included in the connection string
					connection = "EVENTHUB_CONN_STRING",
					cardinality = Cardinality.MANY)
			List<String> events,
			@BindingName(value = "PropertiesArray") Map<String, Object>[] properties,
			final ExecutionContext context) {

		Logger logger = context.getLogger();
		logger.log(Level.INFO, () -> String.format("Persisting [%d] events...", events.size()));

		try {
			if (events.size() == properties.length) {
				Map<String, List<TableTransactionAction>> partitionedEvents = new HashMap<>();

				for (int index = 0; index < properties.length; index++) {
					String eventInStringForm = events.get(index);
					final Map<String, Object> event = ObjectMapperUtils.readValue(eventInStringForm, Map.class);

					final Map<String, Object> eventToBeStored = new HashMap<>();

					// update event with the required parameters and other needed fields
					properties[index].forEach((property, value) -> eventToBeStored.put(replaceDashWithUppercase(property), value));

					Map<String, Object> faultBeanMap = (Map) event.getOrDefault(Constants.FAULTBEAN_EVENT_FIELD, new HashMap<>());
					String faultBeanTimestamp = (String) faultBeanMap.getOrDefault(Constants.TIMESTAMP_EVENT_FIELD, "ERROR");

					// sometimes faultBeanTimestamp has less than 6 digits regarding microseconds
					faultBeanTimestamp = fixDateTime(faultBeanTimestamp);

					if (faultBeanTimestamp.contains("ERROR")) {
						throw new IllegalStateException("Missing " + Constants.FAULTBEAN_EVENT_FIELD + " or " + Constants.FAULTBEAN_TIMESTAMP_EVENT_FIELD);
					}

					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
					LocalDateTime dateTime = LocalDateTime.parse(faultBeanTimestamp, formatter);
					long timestamp = dateTime.toEpochSecond(ZoneOffset.UTC);
					faultBeanMap.put(Constants.TIMESTAMP_EVENT_FIELD, timestamp);
					faultBeanMap.put(Constants.DATE_TIME_EVENT_FIELD, faultBeanTimestamp);

					String insertedDateValue = dateTime.getYear() + "-" + dateTime.getMonthValue() + "-" + dateTime.getDayOfMonth();

					// inserting the identification columns on event saved in Table Storage
					String rowKey = generateRowKey(event, String.valueOf(timestamp));
					eventToBeStored.put(Constants.PARTITION_KEY_TABLESTORAGE_EVENT_FIELD, insertedDateValue);
					eventToBeStored.put(Constants.ROW_KEY_TABLESTORAGE_EVENT_FIELD, rowKey);

					// inserting the additional columns on event saved in Table Storage
					eventToBeStored.put(Constants.TIMESTAMP_TABLESTORAGE_EVENT_FIELD, timestamp);
					eventToBeStored.put(Constants.DATE_TIME_EVENT_FIELD, dateTime);
					eventToBeStored.put(Constants.NOTICE_NUMBER_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.NOTICE_NUMBER_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.ID_PA_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.ID_PA_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.ID_PSP_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.ID_PSP_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.ID_STATION_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.ID_STATION_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.ID_CHANNEL_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.ID_CHANNEL_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.BLOB_BODY_REFERENCE_TABLESTORAGE_EVENT_FIELD, storeBodyInBlobAndGetReference(eventInStringForm, rowKey));

					addToBatch(partitionedEvents, eventToBeStored);
				}

				// save all events in the retrieved batch in the storage
				persistEventBatch(logger, partitionedEvents);
			} else {
				logger.log(Level.SEVERE, () -> String.format("[ALERT][VerifyKOToTS] AppException - Error processing events, lengths do not match: [events: %d - properties: %d]", events.size(), properties.length));
			}
		} catch (BlobStorageUploadException e) {
			logger.log(Level.SEVERE, () -> "[ALERT][VerifyKOToTS] Persistence Exception - Could not save event body on Azure Blob Storage, error: " + e);
		} catch (IllegalArgumentException e) {
			logger.log(Level.SEVERE, () -> "[ALERT][VerifyKOToDS] AppException - Illegal argument exception on table storage nodo-verify-ko-events msg ingestion at " + LocalDateTime.now() + " : " + e);
		} catch (IllegalStateException e) {
			logger.log(Level.SEVERE, () -> "[ALERT][VerifyKOToDS] AppException - Missing argument exception on nodo-verify-ko-events msg ingestion at " + LocalDateTime.now() + " : " + e);
		} catch (Exception e) {
			logger.log(Level.SEVERE, () -> "[ALERT][VerifyKOToTS] AppException - Generic exception on table storage nodo-verify-ko-events msg ingestion at " + LocalDateTime.now() + " : " + e.getMessage());
		}
    }

	private String fixDateTime(String faultBeanTimestamp) {
		int dotIndex = faultBeanTimestamp.indexOf('.');
		if (dotIndex != -1) {
			int fractionLength = faultBeanTimestamp.length() - dotIndex - 1;
			faultBeanTimestamp = fractionLength < 6 ? String.format("%s%s", faultBeanTimestamp, "0".repeat(6 - fractionLength)) : faultBeanTimestamp;
		}
		else {
			faultBeanTimestamp = String.format("%s.000000", faultBeanTimestamp);
		}

		return faultBeanTimestamp;
	}

	private <T> T getEventField(Map<String, Object> event, String name, Class<T> clazz, T defaultValue) {
		T field = null;
		List<String> splitPath = List.of(name.split("\\."));
		Map eventSubset = event;
		Iterator<String> it = splitPath.listIterator();
		while(it.hasNext()) {
			Object retrievedEventField = eventSubset.get(it.next());
			if (!it.hasNext()) {
				field = clazz.cast(retrievedEventField);
			} else {
				eventSubset = (Map) retrievedEventField;
				if (eventSubset == null) {
					throw new IllegalArgumentException("The field [" + name + "] does not exists in the passed event.");
				}
			}
		}
		return field == null ? defaultValue : field;
	}

	private String replaceDashWithUppercase(String input) {
		if (!input.contains("-")){
			return input;
		}
		Matcher matcher = Constants.REPLACE_DASH_PATTERN.matcher(input);
		StringBuilder builder = new StringBuilder();
		while (matcher.find()) {
			matcher.appendReplacement(builder, matcher.group(1).toUpperCase());
		}
		matcher.appendTail(builder);
		return builder.toString();
	}

	public static TableServiceClient getTableServiceClient(){
		if (tableServiceClient == null) {
			tableServiceClient = new TableServiceClientBuilder().connectionString(System.getenv("TABLE_STORAGE_CONN_STRING")).buildClient();
			tableServiceClient.createTableIfNotExists(Constants.TABLE_NAME);
		}
		return tableServiceClient;
	}

	public static BlobContainerClient getBlobContainerClient(){
		if (blobContainerClient == null) {
			BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(System.getenv("BLOB_STORAGE_CONN_STRING")).buildClient();
			blobContainerClient = blobServiceClient.createBlobContainerIfNotExists(Constants.BLOB_NAME);
		}
		return blobContainerClient;
	}

	private void addToBatch(Map<String,List<TableTransactionAction>> partitionEvents, Map<String, Object> event) {
		if (event.get(Constants.ROW_KEY_TABLESTORAGE_EVENT_FIELD) != null) {
			TableEntity entity = new TableEntity((String) event.get(Constants.PARTITION_KEY_TABLESTORAGE_EVENT_FIELD), (String) event.get(Constants.ROW_KEY_TABLESTORAGE_EVENT_FIELD));
			entity.setProperties(event);
			if (!partitionEvents.containsKey(entity.getPartitionKey())){
				partitionEvents.put(entity.getPartitionKey(), new ArrayList<>());
			}
			partitionEvents.get(entity.getPartitionKey()).add(new TableTransactionAction(TableTransactionActionType.UPSERT_REPLACE, entity));
		}
	}

	private String storeBodyInBlobAndGetReference(String eventBody, String fileName) throws BlobStorageUploadException {
		String blobBodyReference = null;
		try {
			BlobClient blobClient = getBlobContainerClient().getBlobClient(fileName);
			BinaryData body = BinaryData.fromStream(new ByteArrayInputStream(eventBody.getBytes(StandardCharsets.UTF_8)));
			blobClient.upload(body);
			blobBodyReference = BlobBodyReference.builder()
					.storageAccount(getBlobContainerClient().getAccountName())
					.containerName(Constants.BLOB_NAME)
					.fileName(fileName)
					.fileLength(body.toString().length())
					.build().toString();
		} catch (Exception e) {
			throw new BlobStorageUploadException(e);
		}
		return blobBodyReference;
	}

	private String generateRowKey(Map<String, Object> event, String insertedTimestampValue) {
		return insertedTimestampValue +
				"-" +
				getEventField(event, Constants.ID_EVENT_FIELD, String.class, Constants.NA);
	}

	private void persistEventBatch(Logger logger, Map<String, List<TableTransactionAction>> partitionedEvents) {
		TableClient tableClient = getTableServiceClient().getTableClient(Constants.TABLE_NAME);
		partitionedEvents.forEach((partition, values) -> {
			try {
				tableClient.submitTransaction(values);
			} catch (Exception e) {
				logger.log(Level.SEVERE, () -> "[ALERT][VerifyKOToTS] Persistence Exception - Could not save " + values.size() + " events (partition [" + partition + "]) on Azure Table Storage, error: " + e);
			}
		});
		logger.info("Done processing events");
	}
}
