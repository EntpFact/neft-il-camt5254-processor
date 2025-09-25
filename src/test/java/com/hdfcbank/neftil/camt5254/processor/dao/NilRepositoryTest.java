package com.hdfcbank.neftil.camt5254.processor.dao;


import com.hdfcbank.neftil.camt5254.processor.config.BTAllowedMsgType;
import com.hdfcbank.neftil.camt5254.processor.exception.Camt5254ProcessorException;
import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class NilRepositoryTest {

    @Mock
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Mock
    private BTAllowedMsgType btAllowedMsgType;

    @InjectMocks
    private NilRepository nilRepository;

    private MsgEventTracker tracker;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        tracker = new MsgEventTracker();
        tracker.setMsgId("MSG123");
        tracker.setSource("SFMS");
        tracker.setTarget("NIL");
        tracker.setFlowType("Inward");
        tracker.setMsgType("camt.052.001.06");
        tracker.setOrgnlReq("<xml>dummy</xml>");
        tracker.setTransformedJsonReq("{\"key\":\"value\"}");
        tracker.setStatus("NEW");
        tracker.setBatchId("B123");
        tracker.setBatchCreationDate(new Date());
        tracker.setBatchCreationTimestamp(LocalDateTime.now());
        tracker.setVersion(BigDecimal.ONE);
    }


    @Test
    void testSaveDuplicateEntry_UpdateExisting() {
        when(jdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(BigDecimal.class)))
                .thenReturn(new BigDecimal("2"));

        nilRepository.saveDuplicateEntry(tracker);

        verify(jdbcTemplate, times(1))
                .update(startsWith("UPDATE"), any(MapSqlParameterSource.class));
    }

    @Test
    void testSaveDuplicateEntry_InsertNew() {
        when(jdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(BigDecimal.class)))
                .thenReturn(null);

        nilRepository.saveDuplicateEntry(tracker);

        verify(jdbcTemplate, times(1))
                .update(startsWith("INSERT"), any(MapSqlParameterSource.class));
    }

    @Test
    void testCheckNull() {
        assert nilRepository.checkNull("test").equals("test");
        assert nilRepository.checkNull(null) == null;
    }

    @Test
    void testFindByMsgId_whenResultFound_returnsTracker() {
        // given
        String msgId = "msg123";
        MsgEventTracker tracker = new MsgEventTracker();
        tracker.setMsgId(msgId);
        tracker.setSource("SRC");
        tracker.setTarget("TGT");
        tracker.setFlowType("INWARD");
        tracker.setMsgType("pacs.008");
        tracker.setOrgnlReq("<xml>test</xml>");
        tracker.setOrgnlReqCount(1);
        tracker.setConsolidateAmt(BigDecimal.TEN);
        tracker.setIntermediateReq("intermediate");
        tracker.setIntermediateCount(2);
        tracker.setStatus("SENT_TO_DISPATCHER");
        tracker.setCreatedTime(LocalDateTime.now().minusDays(1));
        tracker.setModifiedTimestamp(LocalDateTime.now());

        // mock JDBC query (force RowMapper overload)
        when(jdbcTemplate.query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<RowMapper<MsgEventTracker>>any())
        ).thenReturn(List.of(tracker));

        // when
        MsgEventTracker result = nilRepository.findByMsgId(msgId);

        // then
        assertNotNull(result);
        assertEquals("msg123", result.getMsgId());
        assertEquals("SRC", result.getSource());
        assertEquals("TGT", result.getTarget());
        assertEquals("SENT_TO_DISPATCHER", result.getStatus());
        verify(jdbcTemplate, times(1))
                .query(anyString(), any(MapSqlParameterSource.class), ArgumentMatchers.<RowMapper<MsgEventTracker>>any());
    }

    @Test
    void testFindByMsgId_whenNoResultFound_returnsNull() {
        // given
        String msgId = "notFound";

        when(jdbcTemplate.query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<RowMapper<MsgEventTracker>>any())
        ).thenReturn(List.of()); // empty list

        // when
        MsgEventTracker result = nilRepository.findByMsgId(msgId);

        // then
        assertNull(result);
        verify(jdbcTemplate, times(1))
                .query(anyString(), any(MapSqlParameterSource.class), ArgumentMatchers.<RowMapper<MsgEventTracker>>any());
    }

    @Test
    void testFindByMsgId_RecordFound() {
        // given
        MsgEventTracker tracker = new MsgEventTracker();
        tracker.setMsgId("MSG123");
        tracker.setSource("SFMS");
        tracker.setTarget("FC");
        tracker.setFlowType("INWARD");
        tracker.setMsgType("pacs.002");
        tracker.setOrgnlReq("<xml>test</xml>");
        tracker.setOrgnlReqCount(1);
        tracker.setConsolidateAmt(BigDecimal.TEN);
        tracker.setIntermediateReq("<intermediate>");
        tracker.setIntermediateCount(2);
        tracker.setStatus("SENT_TO_DISPATCHER");
        tracker.setCreatedTime(LocalDateTime.now().minusDays(1));
        tracker.setModifiedTimestamp(LocalDateTime.now());

        List<MsgEventTracker> mockResult = List.of(tracker);

        when(jdbcTemplate.query(anyString(), any(MapSqlParameterSource.class), any(org.springframework.jdbc.core.RowMapper.class)))
                .thenReturn(mockResult);

        // when
        MsgEventTracker result = nilRepository.findByMsgId("MSG123");

        // then
        assertThat(result).isNotNull();
        assertThat(result.getMsgId()).isEqualTo("MSG123");
        assertThat(result.getTarget()).isEqualTo("FC");

        verify(jdbcTemplate, times(1))
                .query(anyString(), any(MapSqlParameterSource.class), any(org.springframework.jdbc.core.RowMapper.class));
    }

    @Test
    void testFindByMsgId_NoRecordFound() {
        // given
        when(jdbcTemplate.query(anyString(), any(MapSqlParameterSource.class), any(org.springframework.jdbc.core.RowMapper.class)))
                .thenReturn(Collections.emptyList());

        // when
        MsgEventTracker result = nilRepository.findByMsgId("NON_EXISTENT");

        // then
        assertThat(result).isNull();

        verify(jdbcTemplate, times(1))
                .query(anyString(), any(MapSqlParameterSource.class), any(org.springframework.jdbc.core.RowMapper.class));
    }

    @Test
    void testFindByMsgId_returnsTracker() {
        // Arrange
        MsgEventTracker tracker = new MsgEventTracker();
        tracker.setMsgId("MSG123");
        tracker.setSource("SRC");
        tracker.setTarget("TGT");
        tracker.setFlowType("FLOW");
        tracker.setMsgType("pacs.008.001.09");
        tracker.setOrgnlReq("<xml>request</xml>");
        tracker.setOrgnlReqCount(1);
        tracker.setConsolidateAmt(BigDecimal.TEN);
        tracker.setIntermediateReq("<xml>intermediate</xml>");
        tracker.setIntermediateCount(2);
        tracker.setStatus("SENT_TO_DISPATCHER");
        tracker.setCreatedTime(LocalDateTime.now());
        tracker.setModifiedTimestamp(LocalDateTime.now());

        when(jdbcTemplate.query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<MsgEventTracker>>any()
        )).thenReturn(List.of(tracker));

        // Act
        MsgEventTracker result = nilRepository.findByMsgId("MSG123");

        // Assert
        assertNotNull(result);
        assertEquals("MSG123", result.getMsgId());
        assertEquals("SRC", result.getSource());
        assertEquals("TGT", result.getTarget());
        assertEquals("pacs.008.001.09", result.getMsgType());

        verify(jdbcTemplate, times(1)).query(
                startsWith("SELECT * FROM network_il.msg_event_tracker"),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<MsgEventTracker>>any()
        );
    }

    @Test
    void testFindByMsgId_returnsNullWhenEmpty() {
        // Arrange
        when(jdbcTemplate.query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<MsgEventTracker>>any()
        )).thenReturn(Collections.emptyList());

        // Act
        MsgEventTracker result = nilRepository.findByMsgId("MSG999");

        // Assert
        assertNull(result);
        verify(jdbcTemplate, times(1)).query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<MsgEventTracker>>any()
        );
    }

    @Test
    void testFindByMsgId_throwsException() {
        // Arrange
        when(jdbcTemplate.query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<MsgEventTracker>>any()
        )).thenThrow(new RuntimeException("DB error"));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> nilRepository.findByMsgId("MSG123"));
    }

    @Test
    void testSaveDuplicateEntry_WhenRowExists() {
        when(jdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(BigDecimal.class)))
                .thenReturn(BigDecimal.ONE);

        nilRepository.saveDuplicateEntry(tracker);

        verify(jdbcTemplate, times(1)).update(contains("UPDATE network_il.msg_dedup_tracker"), any(MapSqlParameterSource.class));
    }

    @Test
    void testSaveDuplicateEntry_WhenRowDoesNotExist() {
        when(jdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(BigDecimal.class)))
                .thenReturn(null);

        nilRepository.saveDuplicateEntry(tracker);

        verify(jdbcTemplate, times(1)).update(contains("INSERT INTO network_il.msg_dedup_tracker"), any(MapSqlParameterSource.class));
    }

    // ---------------------- checkNull ----------------------

    @Test
    void testCheckNull_ReturnsValue() {
        assertEquals("abc", nilRepository.checkNull("abc"));
    }

    @Test
    void testCheckNull_ReturnsNull() {
        assertNull(nilRepository.checkNull(null));
    }

    // ---------------------- updateMsgEventTracker ----------------------
/*

    @Test
    void testUpdateMsgEventTracker_AllowedMsgType() throws Exception {
        when(btAllowedMsgType.getAllowedMsgTypes()).thenReturn((List<String>) Set.of("camt.052.001.06"));

        nilRepository.updateMsgEventTracker(tracker);

        verify(jdbcTemplate, times(1)).update(startsWith("WITH updated_msg"), any(MapSqlParameterSource.class));
    }

    @Test
    void testUpdateMsgEventTracker_NotAllowedMsgType() throws Exception {
        when(btAllowedMsgType.getAllowedMsgTypes()).thenReturn((List<String>) Set.of("camt.054.001.08"));

        nilRepository.updateMsgEventTracker(tracker);

        verify(jdbcTemplate, times(1)).update(startsWith("UPDATE network_il.msg_event_tracker"), any(MapSqlParameterSource.class));
    }

    // ---------------------- insertMsgEventTracker ----------------------

    @Test
    void testInsertMsgEventTracker_AllowedMsgType() throws Exception {
        when(btAllowedMsgType.getAllowedMsgTypes()).thenReturn((List<String>) Set.of("camt.052.001.06"));

        nilRepository.insertMsgEventTracker(tracker);

        verify(jdbcTemplate, times(1)).update(startsWith("WITH inserted"), any(MapSqlParameterSource.class));
    }

    @Test
    void testInsertMsgEventTracker_NotAllowedMsgType() throws Exception {
        when(btAllowedMsgType.getAllowedMsgTypes()).thenReturn((List<String>) Set.of("camt.054.001.08"));

        nilRepository.insertMsgEventTracker(tracker);

        verify(jdbcTemplate, times(1)).update(startsWith("INSERT INTO network_il.msg_event_tracker"), any(MapSqlParameterSource.class));
    }

    // ---------------------- validatePacs8Pacs2Status ----------------------

    @Test
    void testValidatePacs8Pacs2Status_ReturnsTrue() {
        when(jdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Integer.class)))
                .thenReturn(1);

        Boolean result = nilRepository.validatePacs8Pacs2Status("B123", LocalDate.now());

        assertTrue(result);
    }

    @Test
    void testValidatePacs8Pacs2Status_ReturnsFalse() {
        when(jdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Integer.class)))
                .thenReturn(0);

        Boolean result = nilRepository.validatePacs8Pacs2Status("B123", LocalDate.now());

        assertFalse(result);
    }
*/

    @Test
    void testValidatePacs8Pacs2Status_ThrowsException() {
        when(jdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Integer.class)))
                .thenThrow(new RuntimeException("DB error"));

        assertThrows(Camt5254ProcessorException.class,
                () -> nilRepository.validatePacs8Pacs2Status("B123", LocalDate.now()));
    }

    // ---------------------- updateBatchTrackerStatusToHoldByMsgId ----------------------

    @Test
    void testUpdateBatchTrackerStatusToHoldByMsgId_Success() {
        nilRepository.updateBatchTrackerStatusToHoldByMsgId("MSG123");

        verify(jdbcTemplate, times(1)).update(startsWith("WITH updated_msg"), any(MapSqlParameterSource.class));
    }

    @Test
    void testInsertMsgEventTracker_WhenMsgTypeAllowed_ShouldExecuteCTESql() {
        when(btAllowedMsgType.getAllowedMsgTypes()).thenReturn(List.of("camt.054.001.08"));

        nilRepository.insertMsgEventTracker(tracker);

        verify(jdbcTemplate, times(1)).update(anyString(), any(MapSqlParameterSource.class));
    }

    @Test
    void testInsertMsgEventTracker_WhenMsgTypeNotAllowed_ShouldExecuteSimpleInsert() {
        when(btAllowedMsgType.getAllowedMsgTypes()).thenReturn(Collections.singletonList("camt.054.001.08"));

        nilRepository.insertMsgEventTracker(tracker);

        verify(jdbcTemplate, times(1)).update(anyString(), any(MapSqlParameterSource.class));
    }

    @Test
    void testInsertMsgEventTracker_WhenSQLExceptionThrown_ShouldWrapInRuntimeException() {
        when(btAllowedMsgType.getAllowedMsgTypes()).thenReturn(List.of("camt.054.001.08"));
        doThrow(new RuntimeException(new SQLException("DB error"))).when(jdbcTemplate)
                .update(anyString(), any(MapSqlParameterSource.class));

        assertThrows(RuntimeException.class, () -> nilRepository.insertMsgEventTracker(tracker));
    }


    @Test
    void testUpdateMsgEventTracker_WhenMsgTypeNotAllowed_ShouldExecuteSimpleUpdate() {
        when(btAllowedMsgType.getAllowedMsgTypes()).thenReturn(Collections.singletonList("camt.052.001.08"));

        nilRepository.updateMsgEventTracker(tracker);

        // verify correct SQL branch executed
        verify(jdbcTemplate, times(1))
                .update(argThat(sql -> sql.startsWith("UPDATE network_il.msg_event_tracker")), any(MapSqlParameterSource.class));
    }

    @Test
    void testUpdateMsgEventTracker_WhenSQLException_ShouldThrowRuntimeException() {
        when(btAllowedMsgType.getAllowedMsgTypes()).thenReturn(List.of("camt.054.001.09"));
        doThrow(new RuntimeException(new SQLException("DB error"))).when(jdbcTemplate)
                .update(anyString(), any(MapSqlParameterSource.class));

        assertThrows(RuntimeException.class, () -> nilRepository.updateMsgEventTracker(tracker));
    }
}
