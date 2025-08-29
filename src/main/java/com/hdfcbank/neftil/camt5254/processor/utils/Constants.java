package com.hdfcbank.neftil.camt5254.processor.utils;

public class Constants {


    public static final String KAFKA_RESPONSE_TOPIC_DAPR_BINDING = "kafka-nilrouter-pubsub-component";
    public static final String INWARD = "INWARD";
    public static final String SFMS = "SFMS";
    public static final String DISPATCHER_FC = "FC_DISPATCHER";
    public static final String DISPATCHER_EPH = "EPH_DISPATCHER";
    public static final String SENT_TO_DISPATCHER = "SENT_TO_DISPATCHER";
    public static final String MSGDEFIDR_XPATH = "//*[local-name()='AppHdr']/*[local-name()='MsgDefIdr']";
    public static final String MSGID_XPATH = "//*[local-name()='AppHdr']/*[local-name()='BizMsgIdr']";
    public static final String BATCH_ID_XPATH = "//*[local-name()='GrpHdr']/*[local-name()='AddtlInf']";
    public static final String BATCH_CREDT_XPATH = "//*[local-name()='AppHdr']/*[local-name()='CreDt']";
}
