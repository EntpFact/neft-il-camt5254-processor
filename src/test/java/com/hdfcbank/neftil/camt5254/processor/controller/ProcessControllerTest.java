package com.hdfcbank.neftil.camt5254.processor.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hdfcbank.neftil.camt5254.processor.model.Header;
import com.hdfcbank.neftil.camt5254.processor.model.ReqPayload;
import com.hdfcbank.neftil.camt5254.processor.model.Response;
import com.hdfcbank.neftil.camt5254.processor.service.CamtXmlProcessor;
import com.hdfcbank.neftil.camt5254.processor.service.ErrorMsgHandling;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class ProcessControllerTest {

    @Mock
    private CamtXmlProcessor camtXmlProcessor;

    @Mock
    private ErrorMsgHandling errorMsgHandling;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private ProcessController processController;

    private ReqPayload validPayload;
    private ReqPayload invalidPayload;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Create mock payloads
        Header validHeader = new Header();
        validHeader.setInvalidPayload(false);

        Header invalidHeader = new Header();
        invalidHeader.setInvalidPayload(true);

        validPayload = new ReqPayload();
        validPayload.setHeader(validHeader);

        invalidPayload = new ReqPayload();
        invalidPayload.setHeader(invalidHeader);
    }

    @Test
    void testHealthz() {
        ResponseEntity<?> response = processController.healthz();
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Success", response.getBody());
    }

    @Test
    void testReady() {
        ResponseEntity<?> response = processController.ready();
        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Success", response.getBody());
    }

    /*@Test
    void testProcess_withValidPayload_callsCamtXmlProcessor() throws Exception {
        // Prepare base64 request
        String xml = "<Request>Test</Request>";
        String base64Xml = Base64.getEncoder().encodeToString(xml.getBytes(StandardCharsets.UTF_8));

        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("data_base64", base64Xml);

        when(objectMapper.readValue(anyString(), eq(ReqPayload.class))).thenReturn(validPayload);

        ResponseEntity<Response> response = processController.process(requestMap).block();

        assertNotNull(response);
        assertEquals("SUCCESS", response.getBody().getStatus());
        verify(camtXmlProcessor, times(1)).parseMessage(validPayload);
        verify(errorMsgHandling, never()).errorMessageAudit(any());
    }

    @Test
    void testProcess_withInvalidPayload_callsErrorMsgHandling() throws Exception {
        String xml = "<Request>Invalid</Request>";
        String base64Xml = Base64.getEncoder().encodeToString(xml.getBytes(StandardCharsets.UTF_8));

        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("data_base64", base64Xml);

        when(objectMapper.readValue(anyString(), eq(ReqPayload.class))).thenReturn(invalidPayload);

        ResponseEntity<Response> response = processController.process(requestMap).block();

        assertNotNull(response);
        assertEquals("SUCCESS", response.getBody().getStatus());
        verify(errorMsgHandling, times(1)).errorMessageAudit(invalidPayload);
        verify(camtXmlProcessor, never()).parseMessage(any());
    }*/

    @Test
    void testProcess_withException_returnsErrorResponse() throws Exception {
        String xml = "<Request>Error</Request>";
        String base64Xml = Base64.getEncoder().encodeToString(xml.getBytes(StandardCharsets.UTF_8));

        when(objectMapper.readValue(anyString(), eq(ReqPayload.class))).thenThrow(new RuntimeException("Parsing error"));

        ResponseEntity<Response> response = processController.process(xml).block();

        assertNotNull(response);
        assertEquals(500, response.getStatusCodeValue());
        assertEquals("ERROR", response.getBody().getStatus());
    }

    @Test
    void testTestProcess_withValidPayload_callsCamtXmlProcessor() throws Exception {
        String jsonRequest = "{\"header\":{\"invalidPayload\":false}}";

        when(objectMapper.readValue(anyString(), eq(ReqPayload.class))).thenReturn(validPayload);

        ResponseEntity<Response> response = processController.testProcess(jsonRequest).block();

        assertNotNull(response);
        assertEquals("SUCCESS", response.getBody().getStatus());
        verify(camtXmlProcessor, times(1)).parseMessage(validPayload);
        verify(errorMsgHandling, never()).errorMessageAudit(any());
    }

    @Test
    void testTestProcess_withException_returnsErrorResponse() throws Exception {
        String jsonRequest = "{\"header\":{\"invalidPayload\":false}}";

        when(objectMapper.readValue(anyString(), eq(ReqPayload.class))).thenThrow(new RuntimeException("Bad JSON"));

        ResponseEntity<Response> response = processController.testProcess(jsonRequest).block();

        assertNotNull(response);
        assertEquals(500, response.getStatusCodeValue());
        assertEquals("ERROR", response.getBody().getStatus());
    }
}
