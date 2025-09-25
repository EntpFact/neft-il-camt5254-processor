package com.hdfcbank.neftil.camt5254.processor.dao;


import com.hdfcbank.neftil.camt5254.processor.config.BTAllowedMsgType;
import com.hdfcbank.neftil.camt5254.processor.exception.Camt5254ProcessorException;
import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;


@Slf4j
@Repository
@EnableCaching
public class NilRepository {

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private final BTAllowedMsgType btAllowedMsgType;

    @Value("${camt54.db.validate_pacs008_pacs002_query}")
    private String validatePacs08Pacs02Query;

    @Value("${camt54.db.update_batch_tracker_hold_by_msgid_query}")
    private String updateBatchTrackerHoldByMsgIdQuery;

    public NilRepository(
            NamedParameterJdbcTemplate namedParameterJdbcTemplate,
            BTAllowedMsgType btAllowedMsgType) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.btAllowedMsgType = btAllowedMsgType;
    }

    public MsgEventTracker findByMsgId(String msgId) {
        String sql = "SELECT * FROM network_il.msg_event_tracker " +
                "WHERE msg_id = :msgId AND status = 'SENT_TO_DISPATCHER' AND version > 1.0 ";

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("msgId", msgId);

        List<MsgEventTracker> result = namedParameterJdbcTemplate.query(sql, params, (rs, rowNum) -> {
            MsgEventTracker tracker = new MsgEventTracker();
            tracker.setMsgId(rs.getString("msg_id"));
            tracker.setSource(rs.getString("source"));
            tracker.setTarget(rs.getString("target"));
            tracker.setFlowType(rs.getString("flow_type"));
            tracker.setMsgType(rs.getString("msg_type"));
            tracker.setOrgnlReq(rs.getString("original_req"));
            tracker.setOrgnlReqCount(rs.getInt("original_req_count"));
            tracker.setConsolidateAmt(rs.getBigDecimal("consolidate_amt"));
            tracker.setIntermediateReq(rs.getString("intermediate_req"));
            tracker.setIntermediateCount(rs.getInt("intemdiate_count"));
            tracker.setStatus(rs.getString("status"));
            tracker.setCreatedTime(rs.getObject("created_time", LocalDateTime.class));
            tracker.setModifiedTimestamp(rs.getObject("modified_timestamp", LocalDateTime.class));
            return tracker;
        });

        return result.isEmpty() ? null : result.get(0);
    }

    public void saveDuplicateEntry(MsgEventTracker tracker) {
        String selectSql = "SELECT MAX(version) FROM network_il.msg_dedup_tracker " +
                "WHERE msg_id = :msgId";

        MapSqlParameterSource baseParams = new MapSqlParameterSource();
        baseParams.addValue("msgId", tracker.getMsgId());


        BigDecimal currentVersion = namedParameterJdbcTemplate.queryForObject(
                selectSql, baseParams, BigDecimal.class);

        if (currentVersion != null) {
            // Row exists → update version
            BigDecimal nextVersion = currentVersion.add(BigDecimal.ONE);

            String updateSql = "UPDATE network_il.msg_dedup_tracker SET " +
                    "flow_type = :flowType, msg_type = :msgType, original_req = (XMLPARSE(CONTENT :originalReq)), " +
                    "version = :version, modified_timestamp = CURRENT_TIMESTAMP " +
                    "WHERE msg_id = :msgId AND source = :source AND target = :target";

            MapSqlParameterSource updateParams = new MapSqlParameterSource();
            updateParams.addValue("msgId", tracker.getMsgId());
            updateParams.addValue("source", tracker.getSource());
            updateParams.addValue("target", tracker.getTarget());
            updateParams.addValue("flowType", tracker.getFlowType());
            updateParams.addValue("msgType", tracker.getMsgType());
            updateParams.addValue("originalReq", tracker.getOrgnlReq());
            updateParams.addValue("version", nextVersion);

            namedParameterJdbcTemplate.update(updateSql, updateParams);

        } else {
            // Row does not exist → insert with version = 1
            String insertSql = "INSERT INTO network_il.msg_dedup_tracker " +
                    "(msg_id, source, target, flow_type, msg_type, original_req, version, created_time, modified_timestamp) " +
                    "VALUES (:msgId, :source, :target, :flowType, :msgType, (XMLPARSE(CONTENT :originalReq)), 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";

            MapSqlParameterSource insertParams = new MapSqlParameterSource();
            insertParams.addValue("msgId", tracker.getMsgId());
            insertParams.addValue("source", tracker.getSource());
            insertParams.addValue("target", tracker.getTarget());
            insertParams.addValue("flowType", tracker.getFlowType());
            insertParams.addValue("msgType", tracker.getMsgType());
            insertParams.addValue("originalReq", tracker.getOrgnlReq());

            namedParameterJdbcTemplate.update(insertSql, insertParams);
        }
    }

    public String checkNull(String req) {
        if (req != null)
            return req;

        return null;
    }

    public void updateMsgEventTracker(MsgEventTracker tracker) {
        try {
            String sql = "WITH updated_msg AS ( " +
                    "  UPDATE network_il.msg_event_tracker " +
                    "  SET source = :source, target = :target, batch_id = :batchId, flow_type = :flowType, " +
                    "      msg_type = :msgType, original_req = :originalReq, invalid_msg = :invalidMsg, transformed_json_req= :transformedJsonReq " +
                    "      replay_count = :replayCount, original_req_count = :originalReqCount, " +
                    "      consolidate_amt = :consolidateAmt, intermediate_req = :intermediateReq, " +
                    "      intemdiate_count = :intemdiateCount, status = :status, " +
                    "      batch_creation_date = :batchCreationDate, batch_timestamp = :batchTimestamp, " +
                    "      modified_timestamp = :modifiedTimestamp, version = :version " +
                    "  WHERE msg_id = :msgId " +
                    "  RETURNING batch_id, status " +
                    ") " +
                    "UPDATE network_il.batch_tracker b " +
                    "SET status = (SELECT status FROM updated_msg), modified_timestamp = NOW() " +
                    "WHERE b.batch_id = (SELECT batch_id FROM updated_msg); ";

            String updateSql = "UPDATE network_il.msg_event_tracker SET " +
                    "target = :target, batch_id = :batchId, flow_type = :flowType, msg_type = :msgType, " +
                    "transformed_json_req = :transformedJsonReq, original_req_count = :originalReqCount, " +
                    "consolidate_amt = :consolidateAmt, intermediate_req = :intermediateReq, " +
                    "intemdiate_count = :intemdiateCount, status = :status, batch_creation_date = :batchCreationDate, " +
                    "batch_timestamp = :batchTimestamp, modified_timestamp = :modifiedTimestamp, version = :version " +
                    "WHERE msg_id = :msgId";

            PGobject jsonObject = new PGobject();
            jsonObject.setType("json");
            jsonObject.setValue(tracker.getTransformedJsonReq());

            MapSqlParameterSource params = new MapSqlParameterSource();
            params.addValue("msgId", tracker.getMsgId());
            params.addValue("target", tracker.getTarget());
            params.addValue("batchId", tracker.getBatchId());
            params.addValue("flowType", tracker.getFlowType());
            params.addValue("msgType", tracker.getMsgType());
            params.addValue("transformedJsonReq", jsonObject);
            params.addValue("originalReqCount", null);
            params.addValue("consolidateAmt", null);
            params.addValue("intermediateReq", null);
            params.addValue("intemdiateCount", null);
            params.addValue("status", tracker.getStatus());
            params.addValue("batchCreationDate", tracker.getBatchCreationDate());
            params.addValue("batchTimestamp", tracker.getBatchCreationTimestamp());
            params.addValue("modifiedTimestamp", LocalDateTime.now());
            params.addValue("version", tracker.getVersion());

            // Check against allowed list from application.yml
            if (btAllowedMsgType.getAllowedMsgTypes().contains(tracker.getMsgType())) {
                namedParameterJdbcTemplate.update(sql, params);
            } else {
                namedParameterJdbcTemplate.update(updateSql, params);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void insertMsgEventTracker(MsgEventTracker tracker) {
        try {
            String insertInMETSql = "INSERT INTO network_il.msg_event_tracker (msg_id, source, target, batch_id, flow_type, msg_type, original_req, transformed_json_req, invalid_msg, replay_count, original_req_count, consolidate_amt, intermediate_req, intemdiate_count, status, batch_creation_date, batch_timestamp, created_time, modified_timestamp, version) " +
                    "VALUES (:msgId, :source, :target, :batchId, :flowType, :msgType, :originalReq, :transformed_json_req, :invalidMsg, :replayCount, :originalReqCount, :consolidateAmt, :intermediateReq, :intemdiateCount, :status, :batchCreationDate, :batchTimestamp, :createdTime, :modifiedTimestamp,:version )";

            String sql = "WITH inserted AS ( " +
                    "    INSERT INTO network_il.msg_event_tracker ( " +
                    "        msg_id, source, target, batch_id, flow_type, msg_type, " +
                    "        original_req, invalid_msg, replay_count, original_req_count, transformed_json_req " +
                    "        consolidate_amt, intermediate_req, intemdiate_count, status, " +
                    "        batch_creation_date, batch_timestamp, created_time, modified_timestamp, version " +
                    "    ) " +
                    "    VALUES ( " +
                    "        :msgId, :source, :target, :batchId, :flowType, :msgType, " +
                    "        :originalReq, :invalidMsg, :replayCount, :originalReqCount, :transformed_json_req " +
                    "        :consolidateAmt, :intermediateReq, :intemdiateCount, :status, " +
                    "        :batchCreationDate, :batchTimestamp, :createdTime, :modifiedTimestamp, :version " +
                    "    ) " +
                    "    RETURNING batch_id, status " +
                    ") " +
                    "UPDATE network_il.batch_tracker bt " +
                    "SET  " +
                    "    status = (SELECT status FROM inserted), " +
                    "    modified_timestamp = NOW() " +
                    "WHERE bt.batch_id = (SELECT batch_id FROM inserted); ";

            PGobject jsonObject = new PGobject();
            jsonObject.setType("json");

            jsonObject.setValue(tracker.getTransformedJsonReq());


            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("msgId", tracker.getMsgId())
                    .addValue("source", tracker.getSource())
                    .addValue("target", tracker.getTarget())
                    .addValue("batchId", tracker.getBatchId())
                    .addValue("flowType", tracker.getFlowType())
                    .addValue("msgType", tracker.getMsgType())
                    .addValue("originalReq", tracker.getOrgnlReq())
                    .addValue("transformed_json_req", jsonObject)
                    .addValue("invalidMsg", tracker.getInvalidReq())
                    .addValue("replayCount", tracker.getReplayCount())
                    .addValue("originalReqCount", null)
                    .addValue("consolidateAmt", null)
                    .addValue("intermediateReq", null)
                    .addValue("intemdiateCount", null)
                    .addValue("status", tracker.getStatus())
                    .addValue("batchCreationDate", tracker.getBatchCreationDate())
                    .addValue("batchTimestamp", tracker.getBatchCreationTimestamp())
                    .addValue("createdTime", LocalDateTime.now())
                    .addValue("modifiedTimestamp", LocalDateTime.now())
                    .addValue("version", tracker.getVersion());

            // Check against allowed list from application.yml
            if (btAllowedMsgType.getAllowedMsgTypes().contains(tracker.getMsgType())) {
                namedParameterJdbcTemplate.update(sql, params);
            } else {
                namedParameterJdbcTemplate.update(insertInMETSql, params);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


    public Boolean validatePacs8Pacs2Status(String batchId,
                                            LocalDate batchCreationDate) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("batchId", batchId);
        //params.addValue("batch_creation_date", batchCreationDate);
        try {
            Integer count = namedParameterJdbcTemplate.queryForObject(validatePacs08Pacs02Query, params, Integer.class);
            return (count != 0 ? Boolean.TRUE : Boolean.FALSE);
        } catch (Exception e) {
            throw new Camt5254ProcessorException("Issue while checking pacs008 status");
        }
    }

    public void updateBatchTrackerStatusToHoldByMsgId(String msgId) {
        MapSqlParameterSource params = new MapSqlParameterSource();


        String updateStatus = "WITH updated_msg AS ( " +
                "  UPDATE network_il.msg_event_tracker " +
                "  SET status = 'Hold', modified_timestamp = NOW() " +
                "  WHERE msg_id = :msgId " +
                "  RETURNING status, msg_id " +
                ") " +
                "UPDATE network_il.batch_tracker b " +
                "SET status = (SELECT status FROM updated_msg), modified_timestamp = NOW() " +
                "WHERE b.msg_id = (SELECT msg_id FROM updated_msg)";

        params.addValue("msgId", msgId);
        try {
            namedParameterJdbcTemplate.update(updateStatus, params);
        } catch (Exception e) {
            throw new Camt5254ProcessorException("Issue while updating batch tracker to HOLD for msgId:" + msgId);
        }
    }

}
