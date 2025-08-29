package com.hdfcbank.neftil.camt5254.processor.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.hdfcbank.neftil.camt5254.processor.dao.NilRepository;
import com.hdfcbank.neftil.camt5254.processor.kafkaproducer.KafkaUtils;
import com.hdfcbank.neftil.camt5254.processor.model.Body;
import com.hdfcbank.neftil.camt5254.processor.model.Header;
import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import com.hdfcbank.neftil.camt5254.processor.model.ReqPayload;
import com.hdfcbank.neftil.camt5254.processor.utils.UtilityMethods;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import static com.hdfcbank.neftil.camt5254.processor.utils.Constants.*;


@Slf4j
@Service
public class CamtXmlProcessor {

    @Autowired
    KafkaUtils kafkaUtils;

    @Autowired
    NilRepository nilRepository;

    @Autowired
    UtilityMethods utilityMethods;

    public void parseMessage(ReqPayload reqPayload) {

        try {

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();

            Document document = builder.parse(new InputSource(new StringReader(reqPayload.getBody().getPayload())));


            String msgId = UtilityMethods.getValueByXPath(document, MSGID_XPATH);
            String msgType = UtilityMethods.getValueByXPath(document, MSGDEFIDR_XPATH);
            String batchId = UtilityMethods.getValueByXPath(document, BATCH_ID_XPATH);
            String batchDateTime = UtilityMethods.getValueByXPath(document, BATCH_CREDT_XPATH);

            // Extract first 10 chars (yyyy-MM-dd)
            String datePart = batchDateTime.substring(0, 10);

            // Parse into java.util.Date
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(datePart);


            Instant instant = Instant.parse(batchDateTime);
            // Convert to LocalDateTime in UTC (or your Zone)
            LocalDateTime localDateTime = instant.atZone(ZoneId.of("UTC")).toLocalDateTime();


            MsgEventTracker tracker = new MsgEventTracker();

            tracker.setMsgId(msgId);
            tracker.setSource(reqPayload.getHeader().getSource());
            tracker.setTarget(DISPATCHER_FC);
            tracker.setFlowType(INWARD);
            tracker.setMsgType(msgType);
            tracker.setBatchCreationDate(date);
            tracker.setBatchCreationTimestamp(localDateTime);
            tracker.setOrgnlReq(reqPayload.getHeader().getPrefix() + reqPayload.getBody().getPayload());
            tracker.setBatchId(batchId);

            // Save to msg_event_tracker table : FC
            nilRepository.saveDataInMsgEventTracker(tracker, false);
            // Save to msg_event_tracker table : EPH
            tracker.setTarget(DISPATCHER_EPH);
            nilRepository.saveDataInMsgEventTracker(tracker, true);

            ReqPayload reqPayldFC = getReqPayload(reqPayload, msgId, msgType, DISPATCHER_FC);
            ReqPayload reqPayldEPH = getReqPayload(reqPayload, msgId, msgType, DISPATCHER_EPH);

            ObjectMapper mapper = new ObjectMapper();
            String jsonFC = mapper.writeValueAsString(reqPayldFC);
            String jsonEPH = mapper.writeValueAsString(reqPayldEPH);

            if (!utilityMethods.duplicateExists(msgId)) {
                // Send to FC and EPH topic
                kafkaUtils.publishToResponseTopic(jsonFC);
                kafkaUtils.publishToResponseTopic(jsonEPH);

                log.info("CAMT52 | CAMT54 Json FC: {}", jsonFC);
                log.info("CAMT52 | CAMT54 Json EPH: {}", jsonEPH);
            }

        } catch (ParserConfigurationException | IOException | SAXException | XPathExpressionException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }


    }

    private static ReqPayload getReqPayload(ReqPayload reqPayload, String msgId, String msgType, String target) {
        Header header = Header.builder()
                .msgId(msgId)
                .msgType(msgType)
                .source(SFMS)
                .target(target)
                .flowType(INWARD)
                .replayInd(false)
                .prefix(reqPayload.getHeader().getPrefix())
                .build();

        Body body = Body.builder()
                .payload(reqPayload.getHeader().getPrefix() + reqPayload.getBody().getPayload())
                .build();

        return ReqPayload.builder()
                .header(header)
                .body(body)
                .build();

    }
}




