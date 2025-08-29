package com.hdfcbank.neftil.camt5254.processor.utils;

import com.hdfcbank.neftil.camt5254.processor.dao.NilRepository;
import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathExpressionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class UtilityMethodsTest {

    @Mock
    private NilRepository nilRepository;

    @InjectMocks
    private UtilityMethods utilityMethods;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    // ---------- Tests for getValueByXPath (static method) ----------

    @Test
    void testGetValueByXPath_validXPath_returnsValue() throws Exception {
        String xml = "<root><msgId>12345</msgId></root>";
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = builder.parse(new java.io.ByteArrayInputStream(xml.getBytes()));

        String result = UtilityMethods.getValueByXPath(doc, "/root/msgId");

        assertEquals("12345", result);
    }

    @Test
    void testGetValueByXPath_invalidXPath_returnsNull() throws Exception {
        String xml = "<root><msgId>12345</msgId></root>";
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = builder.parse(new java.io.ByteArrayInputStream(xml.getBytes()));

        String result = UtilityMethods.getValueByXPath(doc, "/root/invalid");

        assertNull(result);
    }

    @Test
    void testGetValueByXPath_throwsExceptionForBadXPath() {
        assertThrows(XPathExpressionException.class, () -> {
            String xml = "<root><msgId>12345</msgId></root>";
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = builder.parse(new java.io.ByteArrayInputStream(xml.getBytes()));

            UtilityMethods.getValueByXPath(doc, "////");
        });
    }

    // ---------- Tests for duplicateExists (instance method) ----------

    @Test
    void testDuplicateExists_whenDuplicateFound_returnsTrueAndSaves() {
        String msgId = "msg123";
        MsgEventTracker tracker = new MsgEventTracker();
        tracker.setMsgId(msgId);

        when(nilRepository.findByMsgId(msgId)).thenReturn(tracker);

        boolean result = utilityMethods.duplicateExists(msgId);

        assertTrue(result);
        verify(nilRepository, times(1)).saveDuplicateEntry(tracker);
    }

    @Test
    void testDuplicateExists_whenNoDuplicateFound_returnsFalse() {
        String msgId = "msg123";

        when(nilRepository.findByMsgId(msgId)).thenReturn(null);

        boolean result = utilityMethods.duplicateExists(msgId);

        assertFalse(result);
        verify(nilRepository, never()).saveDuplicateEntry(any());
    }
}
