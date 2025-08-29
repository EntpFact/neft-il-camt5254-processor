package com.hdfcbank.neftil.camt5254.processor.service;

import com.hdfcbank.neftil.camt5254.processor.dao.NilRepository;
import com.hdfcbank.neftil.camt5254.processor.kafkaproducer.KafkaUtils;
import com.hdfcbank.neftil.camt5254.processor.model.Body;
import com.hdfcbank.neftil.camt5254.processor.model.Header;
import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import com.hdfcbank.neftil.camt5254.processor.model.ReqPayload;
import com.hdfcbank.neftil.camt5254.processor.utils.UtilityMethods;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class CamtXmlProcessorTest {

    @Mock
    private KafkaUtils kafkaUtils;

    @Mock
    private NilRepository nilRepository;

    @Mock
    private UtilityMethods utilityMethods;

    @InjectMocks
    private CamtXmlProcessor camtXmlProcessor;

    private ReqPayload reqPayload;

    private static final String SAMPLE_XML =
            "<RequestPayload xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.052.001.06\">" +
                    "<AppHdr>" +
                    "<BizMsgIdr>RBIP202204016000000001</BizMsgIdr>" +
                    "<MsgDefIdr>camt.052.001.06</MsgDefIdr>" +
                    "<CreDt>2025-07-23T17:01:47Z</CreDt>" +
                    "</AppHdr>" +
                    "<Document>" +
                    "<BkToCstmrStmt>" +
                    "<GrpHdr>" +
                    "<AddtlInf>BATCH123</AddtlInf>" +
                    "</GrpHdr>" +
                    "</BkToCstmrStmt>" +
                    "</Document>" +
                    "</RequestPayload>";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        Header header = Header.builder()
                .source("SFMS")
                .prefix("<Prefix>")
                .build();

        Body body = Body.builder()
                .payload(SAMPLE_XML)
                .build();

        reqPayload = ReqPayload.builder()
                .header(header)
                .body(body)
                .build();
    }

    @Test
    void testParseMessage_SuccessfulProcessing() throws Exception {
        // Mock duplicateExists to return false so kafka publishing happens
        when(utilityMethods.duplicateExists(anyString())).thenReturn(false);

        camtXmlProcessor.parseMessage(reqPayload);

        // Verify that repository is called twice (for FC & EPH)
        verify(nilRepository, times(2)).saveDataInMsgEventTracker(any(MsgEventTracker.class), anyBoolean());

        // Capture the payloads published to Kafka
        ArgumentCaptor<String> kafkaCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaUtils, times(2)).publishToResponseTopic(kafkaCaptor.capture());

        String jsonPublished1 = kafkaCaptor.getAllValues().get(0);
        String jsonPublished2 = kafkaCaptor.getAllValues().get(1);

        assertTrue(jsonPublished1.contains("camt.052.001.06"));
        assertTrue(jsonPublished2.contains("camt.052.001.06"));
    }

    @Test
    void testParseMessage_DuplicateExists_NoKafkaPublish() {
        // Mock duplicateExists to return true
        when(utilityMethods.duplicateExists(anyString())).thenReturn(true);

        camtXmlProcessor.parseMessage(reqPayload);

        // Verify repository save still happens
        verify(nilRepository, times(2)).saveDataInMsgEventTracker(any(MsgEventTracker.class), anyBoolean());

        // Kafka publish should NOT happen
        verify(kafkaUtils, never()).publishToResponseTopic(anyString());
    }

    @Test
    void testParseMessage_DateParsing() throws Exception {
        when(utilityMethods.duplicateExists(anyString())).thenReturn(true);

        camtXmlProcessor.parseMessage(reqPayload);

        // Capture MsgEventTracker
        ArgumentCaptor<MsgEventTracker> trackerCaptor = ArgumentCaptor.forClass(MsgEventTracker.class);
        verify(nilRepository, atLeastOnce()).saveDataInMsgEventTracker(trackerCaptor.capture(), anyBoolean());

        MsgEventTracker tracker = trackerCaptor.getValue();

        // Verify date part is parsed correctly
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date expectedDate = sdf.parse("2025-07-23");
        assertEquals(expectedDate, tracker.getBatchCreationDate());

        // Verify timestamp
        LocalDateTime expectedLdt = LocalDateTime.of(2025, 7, 23, 17, 1, 47);
        assertEquals(expectedLdt, tracker.getBatchCreationTimestamp());
    }
}