package com.hdfcbank.neftil.camt5254.processor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hdfcbank.neftil.camt5254.processor.dao.NilRepository;
import com.hdfcbank.neftil.camt5254.processor.kafkaproducer.KafkaUtils;
import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import com.hdfcbank.neftil.camt5254.processor.model.ReqPayload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.hdfcbank.neftil.camt5254.processor.utils.Constants.*;

@Slf4j
@Service
public class ErrorMsgHandling {

    @Autowired
    private NilRepository nilRepository;

    @Autowired
    private KafkaUtils kafkaUtils;

    public void errorMessageAudit(ReqPayload reqPayload) {


        try {
            MsgEventTracker tracker = new MsgEventTracker();
            tracker.setMsgId(reqPayload.getHeader().getMsgId());
            tracker.setSource(reqPayload.getHeader().getSource());
            tracker.setFlowType(INWARD);
            tracker.setInvalidReq(true);
            tracker.setMsgType(reqPayload.getHeader().getMsgType());
            tracker.setOrgnlReq(reqPayload.getBody().getPayload());

            ObjectMapper mapper = new ObjectMapper();

            reqPayload.getHeader().setTarget(DISPATCHER_FC);
            String jsonFC = mapper.writeValueAsString(reqPayload);
            // Save to error msg_event_tracker table
            tracker.setTarget(DISPATCHER_FC);
            tracker.setTransformedJsonReq(jsonFC);
            nilRepository.updateMsgEventTracker(tracker);
            //nilRepository.saveDataInMsgEventTracker(tracker, false);

            reqPayload.getHeader().setTarget(DISPATCHER_EPH);
            String jsonEPH = mapper.writeValueAsString(reqPayload);
            // Save to error msg_event_tracker table
            tracker.setTarget(DISPATCHER_EPH);
            tracker.setTransformedJsonReq(jsonEPH);
            nilRepository.insertMsgEventTracker(tracker);
            //nilRepository.saveDataInMsgEventTracker(tracker, true);


            // Send to dispatcher topic
            kafkaUtils.publishToResponseTopic(jsonFC);
            kafkaUtils.publishToResponseTopic(jsonEPH);

            log.info("CAMT52 | CAMT54 Json FC: {}", jsonFC);
            log.info("CAMT52 | CAMT54 Json EPH: {}", jsonEPH);


        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}
