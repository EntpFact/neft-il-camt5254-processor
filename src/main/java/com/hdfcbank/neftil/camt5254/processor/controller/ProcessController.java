package com.hdfcbank.neftil.camt5254.processor.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hdfcbank.neftil.camt5254.processor.exception.Camt5254ProcessorException;
import com.hdfcbank.neftil.camt5254.processor.model.ReqPayload;
import com.hdfcbank.neftil.camt5254.processor.model.Response;
import com.hdfcbank.neftil.camt5254.processor.service.CamtXmlProcessor;
import com.hdfcbank.neftil.camt5254.processor.service.ErrorMsgHandling;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

@Slf4j
@RestController
public class ProcessController {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CamtXmlProcessor camtXmlProcessor;

    @Autowired
    private ErrorMsgHandling errorMsgHandling;

    @CrossOrigin
    @GetMapping(path = "/healthz")
    public ResponseEntity<?> healthz() {
        return new ResponseEntity<>("Success", HttpStatus.OK);
    }

    @CrossOrigin
    @GetMapping(path = "/ready")
    public ResponseEntity<?> ready() {
        return new ResponseEntity<>("Success", HttpStatus.OK);
    }


    @CrossOrigin
    @PostMapping("/process")
    public Mono<ResponseEntity<Response>> process(@RequestBody String request) throws JsonProcessingException {
        log.info("....Processing Started.... ");
        return Mono.fromCallable(() -> {
            try {

                log.info("Incoming XML: {}", request);
                // Get base64 encoded data
                ReqPayload reqPayload  = decodeMessage(request);
                if (reqPayload != null && reqPayload.getHeader().isInvalidPayload()) {
                    errorMsgHandling.errorMessageAudit(reqPayload);
                } else {
                    camtXmlProcessor.parseMessage(reqPayload);
                }

                return ResponseEntity.ok(new Response("SUCCESS", "Message Processed."));
            } catch (Exception ex) {
                log.error("Failed in consuming the message: {}", ex);
                throw new Camt5254ProcessorException("Failed in consuming the message", ex);
            } finally {
                log.info("....Processing Completed.... ");
            }
        }).onErrorResume(ex -> {
            return Mono.just(new ResponseEntity<>(new Response("ERROR", "Message Processing Failed"), HttpStatus.INTERNAL_SERVER_ERROR));
        });
    }

    private ReqPayload decodeMessage(String request) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(request);

        String base64Data = rootNode.get("data_base64").asText();
        String reqPayloadString = new String(Base64.getDecoder().decode(base64Data), StandardCharsets.UTF_8);
        reqPayloadString = objectMapper.readValue(reqPayloadString, String.class);
        log.info("reqPayloadString: {}", reqPayloadString);
        ReqPayload reqpayload = objectMapper.readValue(reqPayloadString, ReqPayload.class);
        log.info("Decoded XML: {}", reqpayload);

        return reqpayload;
    }


    private String removeBOM(String xml) {
        // Check for UTF-8 BOM (EF BB BF)
        if (xml != null && xml.startsWith("\uFEFF")) {
            return xml.substring(1);
        }
        // Sometimes BOM shows up as a junk character like �, remove non-printables too
        return xml.replaceAll("^[^\\x20-\\x7E<]+", "");  // Removes any non-XML leading junk
    }


    @CrossOrigin
    @PostMapping("/testProcess")
    public Mono<ResponseEntity<Response>> testProcess(@RequestBody String request) throws JsonProcessingException {

        log.info("....Processing Started.... ");

        return Mono.fromCallable(() -> {

            try {
                // boolean isJson = isJsonMesage(request);

                log.info("Incoming XML: {}", request);
                // Get base64 encoded data
                ReqPayload reqPayload = objectMapper.readValue(request, ReqPayload.class);
                if (reqPayload.getHeader().isInvalidPayload()) {
                    errorMsgHandling.errorMessageAudit(reqPayload);
                } else {
                    camtXmlProcessor.parseMessage(reqPayload);
                }

                return ResponseEntity.ok(new Response("SUCCESS", "Message Processed."));
            } catch (Exception ex) {
                log.error("Failed in consuming the message: {}", ex);

                throw new Camt5254ProcessorException("Failed in consuming the message", ex);
            } finally {
                log.info("....Processing Completed.... ");
            }
        }).onErrorResume(ex -> {
            return Mono.just(new ResponseEntity<>(new Response("ERROR", "Message Processing Failed"), HttpStatus.INTERNAL_SERVER_ERROR));
        });
    }
}
