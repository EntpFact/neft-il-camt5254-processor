package com.hdfcbank.neftil.camt5254.processor.service;

import com.hdfcbank.neftil.camt5254.processor.dao.NilRepository;
import com.hdfcbank.neftil.camt5254.processor.kafkaproducer.KafkaUtils;
import com.hdfcbank.neftil.camt5254.processor.model.Body;
import com.hdfcbank.neftil.camt5254.processor.model.Header;
import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import com.hdfcbank.neftil.camt5254.processor.model.ReqPayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class ErrorMsgHandlingTest {

    @Mock
    private NilRepository nilRepository;

    @Mock
    private KafkaUtils kafkaUtils;

    @InjectMocks
    private ErrorMsgHandling errorMsgHandling;

    private ReqPayload reqPayload;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        Header header = Header.builder()
                .msgId("MSG123")
                .msgType("camt.052.001.06")
                .source("SFMS")
                .prefix("<Prefix>")
                .build();

        Body body = Body.builder()
                .payload("<RequestPayload><Dummy>test</Dummy></RequestPayload>")
                .build();

        reqPayload = ReqPayload.builder()
                .header(header)
                .body(body)
                .build();
    }

    @Test
    void testErrorMessageAudit_Success() {
        errorMsgHandling.errorMessageAudit(reqPayload);

        // Verify NilRepository is called twice (FC & EPH)
        verify(nilRepository, times(2)).saveDataInMsgEventTracker(any(MsgEventTracker.class), anyBoolean());

        // Verify KafkaUtils is called twice (FC & EPH)
        verify(kafkaUtils, times(2)).publishToResponseTopic(anyString());

        // Capture one MsgEventTracker and check values
        ArgumentCaptor<MsgEventTracker> trackerCaptor = ArgumentCaptor.forClass(MsgEventTracker.class);
        verify(nilRepository, atLeastOnce()).saveDataInMsgEventTracker(trackerCaptor.capture(), anyBoolean());

        MsgEventTracker tracker = trackerCaptor.getValue();
        assertEquals("MSG123", tracker.getMsgId());
        assertEquals("camt.052.001.06", tracker.getMsgType());
        assertTrue(tracker.getInvalidReq());
        assertEquals("SFMS", tracker.getSource());
        assertEquals(reqPayload.getBody().getPayload(), tracker.getOrgnlReq());
    }

    @Test
    void testErrorMessageAudit_KafkaJsonProduced() throws Exception {
        errorMsgHandling.errorMessageAudit(reqPayload);

        // Capture JSON messages published
        ArgumentCaptor<String> kafkaCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaUtils, times(2)).publishToResponseTopic(kafkaCaptor.capture());

    }


}
