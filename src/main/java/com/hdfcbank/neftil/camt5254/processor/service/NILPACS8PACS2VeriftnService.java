package com.hdfcbank.neftil.camt5254.processor.service;

import com.hdfcbank.neftil.camt5254.processor.config.RetryProperties;
import com.hdfcbank.neftil.camt5254.processor.dao.NilRepository;
import com.hdfcbank.neftil.camt5254.processor.exception.Camt5254ProcessorException;
import com.hdfcbank.neftil.camt5254.processor.exception.PACS8PACS2NotCompletedException;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

@Service
@Slf4j
@EnableConfigurationProperties(RetryProperties.class)
public class NILPACS8PACS2VeriftnService {

    @Autowired
    private NilRepository nilRepository;

    // Inject RetryProperties bean
    private final RetryProperties retryProperties;

    public NILPACS8PACS2VeriftnService(NilRepository nilRepository, RetryProperties retryProperties) {
        this.nilRepository = nilRepository;
        this.retryProperties = retryProperties;
    }

    @PostConstruct
    public void logProps() {
        log.info("Loaded retry config: maxAttempts={}, delay={}, multiplier={}",
                retryProperties.getMaxAttempts(),
                retryProperties.getDelay(),
                retryProperties.getMultiplier());
    }

    @Retryable(
            retryFor = PACS8PACS2NotCompletedException.class,
            maxAttemptsExpression = "${retry.transaction-audit.max-attempts}",
            backoff = @Backoff(
                    delayExpression = "${retry.transaction-audit.backoff.delay}",
                    multiplierExpression = "${retry.transaction-audit.backoff.multiplier}"
            )
    )
    public Boolean checkPacs8Pacs2StatusForBatchID(String batchId, String batchCreationDate, String msgId) {
        Boolean isPACS008OnHold = nilRepository.validatePacs8Pacs2Status(batchId, OffsetDateTime.parse(batchCreationDate).toLocalDate());
        if (isPACS008OnHold) {
            throw new PACS8PACS2NotCompletedException("Target not found for batchId=" + batchId);
        }
        return isPACS008OnHold;
    }

    @Recover
    public Boolean recover(PACS8PACS2NotCompletedException ex, String batchId, String batchCreationDate, String msgId) {
        try {
            log.info("In recover");
            nilRepository.updateBatchTrackerStatusToHoldByMsgId(msgId);
        } catch (Exception e) {
            throw new Camt5254ProcessorException("error while updating batch id tracker" + e.getMessage());
        }
        return Boolean.TRUE;
    }
} 