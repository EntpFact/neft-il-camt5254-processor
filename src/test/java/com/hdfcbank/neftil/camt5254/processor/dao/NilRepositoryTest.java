package com.hdfcbank.neftil.camt5254.processor.dao;

import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class NilRepositoryTest {

    @InjectMocks
    private NilRepository nilRepository;

    @Mock
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private MsgEventTracker tracker;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        tracker = new MsgEventTracker();
        tracker.setMsgId("MSG123");
        tracker.setSource("SFMS");
        tracker.setTarget("FC");
        tracker.setFlowType("INWARD");
        tracker.setMsgType("pacs008");
        tracker.setOrgnlReq("<xml/>");
        tracker.setBatchId("BATCH1");
        tracker.setBatchCreationDate(new Date());
        tracker.setBatchCreationTimestamp(LocalDateTime.now());
        tracker.setInvalidReq(false);
    }

    @Test
    void testSaveDataInMsgEventTracker_SendToBothFcEph() {
        // mock created_time from DB
        when(namedParameterJdbcTemplate.queryForObject(anyString(), any(Map.class), eq(String.class)))
                .thenReturn("2025-08-19 12:34:56.123456");

        nilRepository.saveDataInMsgEventTracker(tracker, true);

        verify(namedParameterJdbcTemplate, times(1))
                .update(anyString(), any(MapSqlParameterSource.class));
    }

    @Test
    void testSaveDataInMsgEventTracker_UpdateIfRowExists() {
        when(namedParameterJdbcTemplate.queryForObject(anyString(), any(Map.class), eq(String.class)))
                .thenReturn("MSG123"); // simulate row exists

        when(namedParameterJdbcTemplate.update(anyString(), any(MapSqlParameterSource.class)))
                .thenReturn(1);

        nilRepository.saveDataInMsgEventTracker(tracker, false);

        verify(namedParameterJdbcTemplate, times(1))
                .update(anyString(), any(MapSqlParameterSource.class));
    }

    @Test
    void testSaveDataInMsgEventTracker_NoRowFound() {
        when(namedParameterJdbcTemplate.queryForObject(anyString(), any(Map.class), eq(String.class)))
                .thenThrow(new EmptyResultDataAccessException(1));

        nilRepository.saveDataInMsgEventTracker(tracker, false);

        // should not throw exception
        verify(namedParameterJdbcTemplate, never()).update(startsWith("UPDATE"), any(MapSqlParameterSource.class));
    }


    @Test
    void testSaveDuplicateEntry_UpdateExisting() {
        when(namedParameterJdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(BigDecimal.class)))
                .thenReturn(new BigDecimal("2"));

        nilRepository.saveDuplicateEntry(tracker);

        verify(namedParameterJdbcTemplate, times(1))
                .update(startsWith("UPDATE"), any(MapSqlParameterSource.class));
    }

    @Test
    void testSaveDuplicateEntry_InsertNew() {
        when(namedParameterJdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(BigDecimal.class)))
                .thenReturn(null);

        nilRepository.saveDuplicateEntry(tracker);

        verify(namedParameterJdbcTemplate, times(1))
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
        when(namedParameterJdbcTemplate.query(
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
        verify(namedParameterJdbcTemplate, times(1))
                .query(anyString(), any(MapSqlParameterSource.class), ArgumentMatchers.<RowMapper<MsgEventTracker>>any());
    }

    @Test
    void testFindByMsgId_whenNoResultFound_returnsNull() {
        // given
        String msgId = "notFound";

        when(namedParameterJdbcTemplate.query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<RowMapper<MsgEventTracker>>any())
        ).thenReturn(List.of()); // empty list

        // when
        MsgEventTracker result = nilRepository.findByMsgId(msgId);

        // then
        assertNull(result);
        verify(namedParameterJdbcTemplate, times(1))
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

        when(namedParameterJdbcTemplate.query(anyString(), any(MapSqlParameterSource.class), any(org.springframework.jdbc.core.RowMapper.class)))
                .thenReturn(mockResult);

        // when
        MsgEventTracker result = nilRepository.findByMsgId("MSG123");

        // then
        assertThat(result).isNotNull();
        assertThat(result.getMsgId()).isEqualTo("MSG123");
        assertThat(result.getTarget()).isEqualTo("FC");

        verify(namedParameterJdbcTemplate, times(1))
                .query(anyString(), any(MapSqlParameterSource.class), any(org.springframework.jdbc.core.RowMapper.class));
    }

    @Test
    void testFindByMsgId_NoRecordFound() {
        // given
        when(namedParameterJdbcTemplate.query(anyString(), any(MapSqlParameterSource.class), any(org.springframework.jdbc.core.RowMapper.class)))
                .thenReturn(Collections.emptyList());

        // when
        MsgEventTracker result = nilRepository.findByMsgId("NON_EXISTENT");

        // then
        assertThat(result).isNull();

        verify(namedParameterJdbcTemplate, times(1))
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

        when(namedParameterJdbcTemplate.query(
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

        verify(namedParameterJdbcTemplate, times(1)).query(
                startsWith("SELECT * FROM network_il.msg_event_tracker"),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<MsgEventTracker>>any()
        );
    }

    @Test
    void testFindByMsgId_returnsNullWhenEmpty() {
        // Arrange
        when(namedParameterJdbcTemplate.query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<MsgEventTracker>>any()
        )).thenReturn(Collections.emptyList());

        // Act
        MsgEventTracker result = nilRepository.findByMsgId("MSG999");

        // Assert
        assertNull(result);
        verify(namedParameterJdbcTemplate, times(1)).query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<MsgEventTracker>>any()
        );
    }

    @Test
    void testFindByMsgId_throwsException() {
        // Arrange
        when(namedParameterJdbcTemplate.query(
                anyString(),
                any(MapSqlParameterSource.class),
                ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<MsgEventTracker>>any()
        )).thenThrow(new RuntimeException("DB error"));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> nilRepository.findByMsgId("MSG123"));
    }
}
