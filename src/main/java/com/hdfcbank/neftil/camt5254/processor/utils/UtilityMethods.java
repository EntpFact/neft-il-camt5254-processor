package com.hdfcbank.neftil.camt5254.processor.utils;

import com.hdfcbank.neftil.camt5254.processor.dao.NilRepository;
import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

@Slf4j
@Component
public class UtilityMethods {

    @Autowired
    private NilRepository nilRepository;

    public static String getValueByXPath(Document document, String xpathExpression) throws XPathExpressionException {
        XPath xpath = XPathFactory.newInstance().newXPath();
        Node resultNode = (Node) xpath.evaluate(xpathExpression, document, XPathConstants.NODE);
        return resultNode != null ? resultNode.getTextContent().trim() : null;
    }

    public boolean duplicateExists(String msgId) {
        MsgEventTracker duplicateEntry = nilRepository.findByMsgId(msgId);
        if (duplicateEntry != null) {
            nilRepository.saveDuplicateEntry(duplicateEntry);
            return true; // Skip processing if duplicate found
        }
        return false;
    }


}
