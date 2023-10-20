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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
		logger.log(Level.INFO, "Persisting {0} events...", events.size());

		try {
			if (events.size() == properties.length) {
				Map<String, List<TableTransactionAction>> partitionedEvents = new HashMap<>();

				for (int index = 0; index < properties.length; index++) {
					final Map<String, Object> event = ObjectMapperUtils.readValue(events.get(index), Map.class);

					// update event with the required parameters and other needed fields
					properties[index].forEach((property, value) -> event.put(replaceDashWithUppercase(property), value));
					String insertedDateValue = event.get(Constants.INSERTED_TIMESTAMP_EVENT_FIELD) != null ? ((String)event.get(Constants.INSERTED_TIMESTAMP_EVENT_FIELD)).substring(0, 10) : Constants.NA;
					event.put(Constants.INSERTED_DATE_EVENT_FIELD, insertedDateValue);
					event.put(Constants.PAYLOAD_EVENT_FIELD, getPayload(logger, event));
					event.put(Constants.PARTITION_KEY_EVENT_FIELD, generatePartitionKey(event, insertedDateValue));
					addToBatch(partitionedEvents, event);
				}

				// save all events in the retrieved batch in the storage
				persistEventBatch(logger, partitionedEvents);
			}
		} catch (NullPointerException e) {
			logger.severe("NullPointerException exception on table storage nodo-verify-ko-events msg ingestion at "+ LocalDateTime.now()+ " : " + e.getMessage());
		} catch (Throwable e) {
			logger.severe("Generic exception on table storage nodo-verify-ko-events msg ingestion at "+ LocalDateTime.now()+ " : " + e.getMessage());
		}
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
		if (event.get(Constants.UNIQUE_ID_EVENT_FIELD) != null) {
			TableEntity entity = new TableEntity((String) event.get(Constants.PARTITION_KEY_EVENT_FIELD), (String) event.get(Constants.UNIQUE_ID_EVENT_FIELD));
			entity.setProperties(event);
			if (!partitionEvents.containsKey(entity.getPartitionKey())){
				partitionEvents.put(entity.getPartitionKey(), new ArrayList<>());
			}
			partitionEvents.get(entity.getPartitionKey()).add(new TableTransactionAction(TableTransactionActionType.UPSERT_REPLACE, entity));
		}
	}

	private byte[] getPayload(Logger logger, Map<String,Object> event) {
		ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
		String payload = (String) event.get(Constants.PAYLOAD_EVENT_FIELD);
		byte[] data = null;
		if (payload != null) {
			try {
				data = payload.getBytes(StandardCharsets.UTF_8);
				DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayStream, new Deflater());
				deflaterOutputStream.write(data);
				deflaterOutputStream.close();
			} catch (Exception e) {
				logger.severe(e.getMessage());
			}
		}
		return data != null ? byteArrayStream.toByteArray() : null;
	}

	private String generatePartitionKey(Map<String, Object> event, String insertedDateValue) {
		return new StringBuilder().append(insertedDateValue)
				.append("-")
				.append(event.get(Constants.ID_DOMINIO_EVENT_FIELD) != null ? event.get(Constants.ID_DOMINIO_EVENT_FIELD).toString() : Constants.NA)
				.append("-")
				.append(event.get(Constants.PSP_EVENT_FIELD) != null ? event.get(Constants.PSP_EVENT_FIELD).toString() : Constants.NA)
				.toString();
	}

	private void persistEventBatch(Logger logger, Map<String, List<TableTransactionAction>> partitionedEvents) {
		TableClient tableClient = getTableServiceClient().getTableClient(Constants.TABLE_NAME);
		partitionedEvents.forEach((partition, values) -> {
			try {
				tableClient.submitTransaction(values);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Could not save {0} events (partition [{1}]) on Azure Table Storage, error: [{2}]", new Object[]{values.size(), partition, e});
			}
		});
		logger.info("Done processing events");
	}
}
