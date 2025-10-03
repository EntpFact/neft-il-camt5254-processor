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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

import static org.mockito.ArgumentMatchers.any;

class CamtXmlProcessorTest {

    @InjectMocks
    private CamtXmlProcessor camtXmlProcessor;

    @Mock
    private KafkaUtils kafkaUtils;

    @Mock
    private NilRepository nilRepository;

    @Mock
    private UtilityMethods utilityMethods;

    @Mock
    private NILPACS8PACS2VeriftnService nilpacs8PACS2VeriftnService;

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

    private ReqPayload buildReqPayload(String msgType) {
        String xml = "<RequestPayload>" +
                "<AppHdr><BizMsgIdr>RBIP202204016000000001</BizMsgIdr><MsgDefIdr>camt.054.001.08</MsgDefIdr><CreDt>2025-01-13T19:24:20Z</CreDt></AppHdr>" +
                "<Document><GrpHdr>" +
                "<MsgId>MSG123</MsgId><MsgDefIdr>camt.054.001.08</MsgDefIdr>" +
                "<BatchId>BATCH789</BatchId>" +
                "<CreDt>2025-09-29T10:15:30Z</CreDt>" +
                "</GrpHdr></Document></RequestPayload>";

        Header header = Header.builder()
                .msgId("MSG123")
                .msgType(msgType)
                .prefix("")
                .source("SFMS")
                .target("DISPATCHER_FC")
                .flowType("INWARD")
                .replayInd(false)
                .build();

        Body body = Body.builder()
                .payload(xml)
                .build();

        return ReqPayload.builder()
                .header(header)
                .body(body)
                .build();
    }

    @Test
    void testParseMessage_normalFlow() throws Exception {
        ReqPayload reqPayload = buildReqPayload("camt.052.001.08");

        when(utilityMethods.duplicateExists(anyString())).thenReturn(false);

        camtXmlProcessor.parseMessage(reqPayload);

        verify(nilRepository, times(1)).updateMsgEventTracker(any(MsgEventTracker.class));
        verify(nilRepository, times(1)).insertMsgEventTracker(any(MsgEventTracker.class));
        verify(kafkaUtils, times(2)).publishToResponseTopic(anyString(), anyString());
    }

/*    @Test
    void testParseMessage_camt054_canProceedTrue() throws Exception {
        ReqPayload reqPayload = buildReqPayload("camt.054.001.08");

        when(nilpacs8PACS2VeriftnService.checkPacs8Pacs2StatusForBatchID(anyString(), anyString(), anyString()))
                .thenReturn(true);

        camtXmlProcessor.parseMessage(reqPayload);

        // Should return early → no insert/update/publish
        verify(nilRepository, never()).updateMsgEventTracker(any());
        verify(nilRepository, never()).insertMsgEventTracker(any());
        verify(kafkaUtils, never()).publishToResponseTopic(anyString());
    }*/

    @Test
    void testParseMessage_duplicateExistsTrue() throws Exception {
        ReqPayload reqPayload = buildReqPayload("camt.052.001.08");

        when(utilityMethods.duplicateExists(anyString())).thenReturn(true);

        camtXmlProcessor.parseMessage(reqPayload);

        // Repo is called but Kafka should NOT publish
        verify(nilRepository, times(1)).updateMsgEventTracker(any(MsgEventTracker.class));
        verify(nilRepository, times(1)).insertMsgEventTracker(any(MsgEventTracker.class));
        verify(kafkaUtils, never()).publishToResponseTopic(anyString(), anyString());
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
        verify(kafkaUtils, times(2)).publishToResponseTopic(any(), anyString());
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