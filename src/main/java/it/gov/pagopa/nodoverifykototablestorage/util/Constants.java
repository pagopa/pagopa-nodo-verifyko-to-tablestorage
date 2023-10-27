package it.gov.pagopa.nodoverifykototablestorage.util;

import java.util.regex.Pattern;

public class Constants {


    private Constants() {}

    public static final String POM_PROPERTIES_PATH = "/META-INF/maven/it.gov.pagopa/nodoverifykotodatastore/pom.properties";
    public static final Pattern REPLACE_DASH_PATTERN = Pattern.compile("-([a-zA-Z])");
    public static final String NA = "NA";


    public static final String ID_EVENT_FIELD = "id";
    public static final String FAULTBEAN_TIMESTAMP_EVENT_FIELD = "faultBean.timestamp";
     public static final String CREDITOR_ID_EVENT_FIELD = "creditor.idPA";
    public static final String PSP_ID_EVENT_FIELD = "psp.idPsp";
    public static final String NOTICE_NUMBER_EVENT_FIELD = "debtorPosition.noticeNumber";
    public static final String ID_PA_EVENT_FIELD = "creditor.idPA";
    public static final String ID_PSP_EVENT_FIELD = "psp.idPsp";
    public static final String ID_STATION_EVENT_FIELD = "creditor.idStation";
    public static final String ID_CHANNEL_EVENT_FIELD = "psp.idChannel";

    public static final String PARTITION_KEY_TABLESTORAGE_EVENT_FIELD = "PartitionKey";
    public static final String ROW_KEY_TABLESTORAGE_EVENT_FIELD = "RowKey";
    public static final String NOTICE_NUMBER_TABLESTORAGE_EVENT_FIELD = "noticeNumber";
    public static final String ID_PA_TABLESTORAGE_EVENT_FIELD = "idPA";
    public static final String ID_PSP_TABLESTORAGE_EVENT_FIELD = "idPsp";
    public static final String ID_STATION_TABLESTORAGE_EVENT_FIELD = "idStation";
    public static final String ID_CHANNEL_TABLESTORAGE_EVENT_FIELD = "idChannel";
    public static final String TIMESTAMP_TABLESTORAGE_EVENT_FIELD = "timestamp";
    public static final String BLOB_BODY_REFERENCE_TABLESTORAGE_EVENT_FIELD = "blobBodyRef";
    public static final String TABLE_NAME = System.getenv("TABLE_STORAGE_TABLE_NAME");
    public static final String BLOB_NAME = System.getenv("BLOB_STORAGE_CONTAINER_NAME");
}
