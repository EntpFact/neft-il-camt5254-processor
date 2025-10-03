package com.hdfcbank.neftil.camt5254.processor.service;

import com.hdfcbank.neftil.camt5254.processor.dao.NilRepository;
import com.hdfcbank.neftil.camt5254.processor.exception.Camt5254ProcessorException;
import com.hdfcbank.neftil.camt5254.processor.exception.PACS8PACS2NotCompletedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;


@SpringJUnitConfig
@EnableRetry
class NILPACS8PACS2VeriftnServiceTest {

    @Mock
    private NilRepository nilRepository;

    @InjectMocks
    private NILPACS8PACS2VeriftnService service;

    private String batchId = "B123";
    private String msgId = "M456";
    private String batchCreationDate = OffsetDateTime.now().toString();

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testCheckPacs8Pacs2Status_NoHold_ReturnsFalse() {
        when(nilRepository.validatePacs8Pacs2Status(anyString(), any()))
                .thenReturn(false);

        Boolean result = service.checkPacs8Pacs2StatusForBatchID(batchId, batchCreationDate, msgId);

        assertFalse(result);
    }

    @Test
    void testRecover_UpdatesBatchTrackerAndReturnsTrue() {
        PACS8PACS2NotCompletedException ex = new PACS8PACS2NotCompletedException("Target not found for batchId=B123");

        Boolean result = service.recover(ex, "B123", "2025-09-11T10:00:00Z", "MSG123");

        assertThat(result).isTrue();
        verify(nilRepository).updateBatchTrackerStatusToHoldByMsgId("MSG123");
    }

    @Test
    void testRecover_WhenUpdateFails_ThrowsCamtException() {
        doThrow(new RuntimeException("DB error"))
                .when(nilRepository).updateBatchTrackerStatusToHoldByMsgId(anyString());

        Camt5254ProcessorException ex = assertThrows(
                Camt5254ProcessorException.class,
                () -> service.recover(new PACS8PACS2NotCompletedException("fail"), batchId, batchCreationDate, msgId)
        );

        assertTrue(ex.getMessage().contains("error while updating batch id tracker"));
    }
}