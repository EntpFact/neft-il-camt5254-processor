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

import static org.assertj.core.api.Assertions.assertThat;
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
        // when
        errorMsgHandling.errorMessageAudit(reqPayload);

        // then
        verify(nilRepository, times(1)).updateMsgEventTracker(any(MsgEventTracker.class));
        verify(nilRepository, times(1)).insertMsgEventTracker(any(MsgEventTracker.class));
        verify(kafkaUtils, times(2)).publishToResponseTopic(anyString());

        // Capture FC tracker
        ArgumentCaptor<MsgEventTracker> fcCaptor = ArgumentCaptor.forClass(MsgEventTracker.class);
        verify(nilRepository).updateMsgEventTracker(fcCaptor.capture());

        MsgEventTracker fcTracker = fcCaptor.getValue();
        assertThat(fcTracker.getMsgId()).isEqualTo("MSG123");
        assertThat(fcTracker.getTarget()).isEqualTo("EPH_DISPATCHER");
        assertThat(fcTracker.getTransformedJsonReq()).isNotBlank();

        // Capture EPH tracker
        ArgumentCaptor<MsgEventTracker> ephCaptor = ArgumentCaptor.forClass(MsgEventTracker.class);
        verify(nilRepository).insertMsgEventTracker(ephCaptor.capture());

        MsgEventTracker ephTracker = ephCaptor.getValue();
        assertThat(ephTracker.getMsgId()).isEqualTo("MSG123");
        assertThat(ephTracker.getTarget()).isEqualTo("EPH_DISPATCHER");
        assertThat(ephTracker.getTransformedJsonReq()).isNotBlank();
    }
}
