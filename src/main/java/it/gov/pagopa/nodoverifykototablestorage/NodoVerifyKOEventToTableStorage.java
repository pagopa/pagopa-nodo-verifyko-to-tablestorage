package it.gov.pagopa.nodoverifykototablestorage;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.data.tables.models.TableTransactionActionType;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import it.gov.pagopa.nodoverifykototablestorage.util.Constants;
import it.gov.pagopa.nodoverifykototablestorage.util.ObjectMapperUtils;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Azure Functions with Azure Event Hub trigger.
 * This function will be invoked when an Event Hub trigger occurs
 */
public class NodoVerifyKOEventToTableStorage {

	private static TableServiceClient tableServiceClient = null;

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

					String insertedTimestampValue = getEventField(event, Constants.INSERTED_TIMESTAMP_EVENT_FIELD, String.class, Constants.NA);
					String insertedDateValue = Constants.NA.equals(insertedTimestampValue) ? Constants.NA : insertedTimestampValue.substring(0, 10);

					// inserting the identification columns on event saved in Table Storage
					eventToBeStored.put(Constants.PARTITION_KEY_TABLESTORAGE_EVENT_FIELD, insertedDateValue);
					eventToBeStored.put(Constants.ROW_KEY_TABLESTORAGE_EVENT_FIELD, generateRowKey(event, insertedTimestampValue));

					// inserting the additional columns on event saved in Table Storage
					eventToBeStored.put(Constants.UNIQUE_ID_TABLESTORAGE_EVENT_FIELD, event.get(Constants.ID_EVENT_FIELD));
					eventToBeStored.put(Constants.TIMESTAMP_TABLESTORAGE_EVENT_FIELD, insertedTimestampValue);
					eventToBeStored.put(Constants.NOTICE_NUMBER_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.NOTICE_NUMBER_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.ID_PA_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.ID_PA_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.ID_PSP_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.ID_PSP_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.ID_STATION_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.ID_STATION_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.ID_CHANNEL_TABLESTORAGE_EVENT_FIELD, getEventField(event, Constants.ID_CHANNEL_EVENT_FIELD, String.class, Constants.NA));
					eventToBeStored.put(Constants.PAYLOAD_TABLESTORAGE_EVENT_FIELD, new String(Base64.getEncoder().encode(eventInStringForm.getBytes()))); // TODO to be refactored

					addToBatch(partitionedEvents, eventToBeStored);
				}

				// save all events in the retrieved batch in the storage
				persistEventBatch(logger, partitionedEvents);
			}
		} catch (NullPointerException e) {
			logger.log(Level.SEVERE, () -> "[ALERT][VerifyKOToTS] AppException - NullPointerException exception on table storage nodo-verify-ko-events msg ingestion at " + LocalDateTime.now() + " : " + e.getMessage());
		} catch (Throwable e) {
			logger.log(Level.SEVERE, () -> "[ALERT][VerifyKOToTS] AppException - Generic exception on table storage nodo-verify-ko-events msg ingestion at " + LocalDateTime.now() + " : " + e.getMessage());
		}
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

	private static TableServiceClient getTableServiceClient(){
		if (tableServiceClient == null) {
			tableServiceClient = new TableServiceClientBuilder().connectionString(System.getenv("TABLE_STORAGE_CONN_STRING")).buildClient();
			tableServiceClient.createTableIfNotExists(Constants.TABLE_NAME);
		}
		return tableServiceClient;
	}

	private void addToBatch(Map<String,List<TableTransactionAction>> partitionEvents, Map<String, Object> event) {
		if (event.get(Constants.UNIQUE_ID_TABLESTORAGE_EVENT_FIELD) != null) {
			TableEntity entity = new TableEntity((String) event.get(Constants.PARTITION_KEY_TABLESTORAGE_EVENT_FIELD), (String) event.get(Constants.UNIQUE_ID_TABLESTORAGE_EVENT_FIELD));
			entity.setProperties(event);
			if (!partitionEvents.containsKey(entity.getPartitionKey())){
				partitionEvents.put(entity.getPartitionKey(), new ArrayList<>());
			}
			partitionEvents.get(entity.getPartitionKey()).add(new TableTransactionAction(TableTransactionActionType.UPSERT_REPLACE, entity));
		}
	}

	private byte[] getPayload(Logger logger, Map<String,Object> event) {
		ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
		String payload = (String) event.get(Constants.PAYLOAD_TABLESTORAGE_EVENT_FIELD);
		byte[] data = null;
		if (payload != null) {
			try {
				data = payload.getBytes(StandardCharsets.UTF_8);
				DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayStream, new Deflater());
				deflaterOutputStream.write(data);
				deflaterOutputStream.close();
			} catch (Exception e) {
				logger.log(Level.SEVERE, () -> "[ALERT][VerifyKOToTS] AppException - Error while generating payload for event: " + e.getMessage());
			}
		}
		return data != null ? byteArrayStream.toByteArray() : null;
	}

	private String generateRowKey(Map<String, Object> event, String insertedDateValue) {
		return insertedDateValue.replace(":", "").replace(".", "").replace("T", "").replace("-", "") +
				"-" +
				getEventField(event, Constants.CREDITOR_ID_EVENT_FIELD, String.class, Constants.NA) +
				"-" +
				getEventField(event, Constants.PSP_ID_EVENT_FIELD, String.class, Constants.NA);
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
