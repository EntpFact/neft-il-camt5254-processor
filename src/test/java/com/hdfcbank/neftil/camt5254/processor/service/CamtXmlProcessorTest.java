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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CamtXmlProcessorTest {

    @Mock
    KafkaUtils kafkaUtils;

    @Mock
    NilRepository nilRepository;

    @Mock
    UtilityMethods utilityMethods;

    @Mock
    NILPACS8PACS2VeriftnService nilpacs8PACS2VeriftnService;

    @InjectMocks
    CamtXmlProcessor camtXmlProcessor;

    private ReqPayload reqPayload;
    private static final String VALID_XML =
            "<RequestPayload>" +
                    "<AppHdr>" +
                    "<BizMsgIdr>RBIP202204016000000001</BizMsgIdr>\n" +
                    " <MsgDefIdr>camt.054.001.08</MsgDefIdr>" +
                    " <CreDt>2022-04-03T13:45:01Z</CreDt>" +
                    "<BtchId>B123</BtchId>" +
                    "</AppHdr>" +
                    "      <GrpHdr>\n" +
                    "         <MsgId>RBIP202204016000000001</MsgId>\n" +
                    "         <CreDtTm>2022-04-03T13:45:01</CreDtTm>\n" +
                    "\t\t <AddtlInf>BatchId:0007</AddtlInf></GrpHdr>" +
                    "<MsgDefIdr>camt.052.001.06</MsgDefIdr>" +
                    "</RequestPayload>";

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
                .payload(VALID_XML)
                .build();

        reqPayload = ReqPayload.builder()
                .header(header)
                .body(body)
                .build();
    }

    @Test
    void testParseMessage_Camt54_PublishesToKafka() throws Exception {
        // Arrange
        when(utilityMethods.duplicateExists(anyString())).thenReturn(false);
        when(nilpacs8PACS2VeriftnService.checkPacs8Pacs2StatusForBatchID(anyString(), anyString(), anyString())).thenReturn(true);
        // Act
        camtXmlProcessor.parseMessage(reqPayload);

        // Assert
        verify(nilRepository).updateMsgEventTracker(any(MsgEventTracker.class));
        verify(nilRepository).insertMsgEventTracker(any(MsgEventTracker.class));
        verify(kafkaUtils, times(2)).publishToResponseTopic(any());
    }

    @Test
    void testParseMessage_DuplicateExists_NoKafkaPublish() throws Exception {
        // Arrange
        when(utilityMethods.duplicateExists(anyString())).thenReturn(true);
        when(nilpacs8PACS2VeriftnService.checkPacs8Pacs2StatusForBatchID(anyString(), anyString(), anyString())).thenReturn(true);

        // Act
        camtXmlProcessor.parseMessage(reqPayload);

        // Assert
        verify(nilRepository).updateMsgEventTracker(any(MsgEventTracker.class));
        verify(nilRepository).insertMsgEventTracker(any(MsgEventTracker.class));
        verify(kafkaUtils, never()).publishToResponseTopic(any());
    }

    @Test
    void testParseMessage_Camt52_VerificationProcessing() throws Exception {
        // Arrange
        String xml =
                "<RequestPayload>" +
                        "<AppHdr>" +
                        "<BizMsgIdr>RBIP202204016000000001</BizMsgIdr>\n" +
                        " <MsgDefIdr>camt.052.001.08</MsgDefIdr>" +
                        " <CreDt>2022-04-03T13:45:01Z</CreDt>" +
                        "<BtchId>B123</BtchId>" +
                        "</AppHdr>" +
                        "      <GrpHdr>\n" +
                        "         <MsgId>RBIP202204016000000001</MsgId>\n" +
                        "         <CreDtTm>2022-04-03T13:45:01</CreDtTm>\n" +
                        "\t\t <AddtlInf>BatchId:0007</AddtlInf></GrpHdr>" +
                        "<MsgDefIdr>camt.052.001.06</MsgDefIdr>" +
                        "</RequestPayload>";
        reqPayload.getBody().setPayload(xml);

        // Act
        try {
            camtXmlProcessor.parseMessage(reqPayload);
        } catch (RuntimeException e) {
            // Expected due to @Retryable in service
        }

        // Assert
        verify(nilRepository).updateMsgEventTracker(any());
        verify(nilRepository).insertMsgEventTracker(any());
        verify(kafkaUtils, times(2)).publishToResponseTopic(any());
    }


    @Test
    void testParseMessage_InvalidXml_ThrowsRuntimeException() {
        // Arrange
        reqPayload.getBody().setPayload("<InvalidXML>");

        // Act + Assert
        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> camtXmlProcessor.parseMessage(reqPayload));
    }
}