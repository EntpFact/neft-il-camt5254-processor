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
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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

    @Autowired
    NILPACS8PACS2VeriftnService nilpacs8PACS2VeriftnService;

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

            if (msgType.equals("camt.054.001.08")) {
                Boolean canProceed = nilpacs8PACS2VeriftnService.checkPacs8Pacs2StatusForBatchID(batchId, batchDateTime, msgId);
                if (Boolean.TRUE.equals(!canProceed)) {
                    return;
                }
            }

            // Extract first 10 chars (yyyy-MM-dd)
            String datePart = batchDateTime.substring(0, 10);
            LocalDate localDate = LocalDate.parse(datePart, DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            // Parse into java.util.Date
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(datePart);


            Instant instant = Instant.parse(batchDateTime);
            // Convert to LocalDateTime in UTC (or your Zone)
            LocalDateTime localDateTime = instant.atZone(ZoneId.of("UTC")).toLocalDateTime();

            ReqPayload reqPayldFC = getReqPayload(reqPayload, msgId, msgType, DISPATCHER_FC);
            ReqPayload reqPayldEPH = getReqPayload(reqPayload, msgId, msgType, DISPATCHER_EPH);
            ObjectMapper mapper = new ObjectMapper();
            String jsonFC = mapper.writeValueAsString(reqPayldFC);
            String jsonEPH = mapper.writeValueAsString(reqPayldEPH);

            MsgEventTracker messageEventTrackerFC = buildMsgEventTracker(msgId, msgType, batchId, localDate, localDateTime, DISPATCHER_FC, reqPayload.getHeader().getPrefix() + reqPayload.getBody().getPayload(), jsonFC);

            MsgEventTracker messageEventTrackerEPH = buildMsgEventTracker(msgId, msgType, batchId, localDate, localDateTime, DISPATCHER_EPH, reqPayload.getHeader().getPrefix() + reqPayload.getBody().getPayload(), jsonEPH);


            nilRepository.updateMsgEventTracker(messageEventTrackerFC);
            nilRepository.insertMsgEventTracker(messageEventTrackerEPH);

            if (!utilityMethods.duplicateExists(msgId)) {
                // Send to FC and EPH topic
                kafkaUtils.publishToResponseTopic(jsonFC);
                kafkaUtils.publishToResponseTopic(jsonEPH);

                log.info("CAMT52 | CAMT54 Json FC: {}", jsonFC);
                log.info("CAMT52 | CAMT54 Json EPH: {}", jsonEPH);
            }

        } catch (ParserConfigurationException | IOException | SAXException | XPathExpressionException e) {
            log.error(e.toString());
            throw new RuntimeException(e);
        } catch (ParseException e) {
            log.error(e.toString());
            throw new RuntimeException(e);
        }


    }

    private MsgEventTracker buildMsgEventTracker(String msgId, String msgType, String batchId, LocalDate date, LocalDateTime batchDateTime, String target, String reqPayload, String transformedJson) {
        return MsgEventTracker.builder().msgId(msgId).source(SFMS).msgType(msgType).
                flowType(INWARD).batchId(batchId).batchCreationTimestamp(batchDateTime).
                batchCreationDate(java.sql.Date.valueOf(date)).status(SENT_TO_DISPATCHER)
                .consolidateAmt(null).orgnlReq(reqPayload).orgnlReqCount(null).transformedJsonReq(transformedJson)
                .intermediateReq(null).intermediateCount(null).target(target)
                .invalidReq(false).replayCount(0).version(BigDecimal.ONE).build();
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




