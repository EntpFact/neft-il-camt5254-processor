package com.hdfcbank.neftil.camt5254.processor.dao;


import com.hdfcbank.neftil.camt5254.processor.model.MsgEventTracker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hdfcbank.neftil.camt5254.processor.utils.Constants.*;


@Slf4j
@Repository
@EnableCaching
public class NilRepository {

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public void saveDataInMsgEventTracker(MsgEventTracker msgEventTracker, boolean sendtoBothFcEph) {
        LocalDateTime timestamp = LocalDateTime.now();
        if (sendtoBothFcEph) {
            String createdtimestamp = "SELECT created_time FROM network_il.msg_event_tracker WHERE msg_id = :msgId";

            String sql = "INSERT INTO network_il.msg_event_tracker (msg_id, source, target, batch_id, flow_type, msg_type, original_req, invalid_msg, original_req_count, consolidate_amt, intermediate_req, intemdiate_count, status,batch_creation_date, batch_timestamp, created_time, modified_timestamp, version) " +
                    "VALUES (:msg_id, :source, :target, :batch_id, :flow_type, :msg_type, :original_req, :invalid_msg, :original_req_count, :consolidate_amt, :intermediate_req, :intemdiate_count, :status, :batch_creation_date, :batch_timestamp, :created_time, :modified_timestamp,:version )";


            Map<String, Object> params1 = new HashMap<>();
            params1.put("msgId", msgEventTracker.getMsgId());


            String foundId = namedParameterJdbcTemplate.queryForObject(createdtimestamp, params1, String.class);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            LocalDateTime localDateTime = LocalDateTime.parse(foundId, formatter);

            MapSqlParameterSource params = new MapSqlParameterSource();
            params.addValue("msg_id", msgEventTracker.getMsgId());
            params.addValue("source", SFMS);
            params.addValue("target", msgEventTracker.getTarget());
            params.addValue("batch_id", msgEventTracker.getBatchId() != null ? msgEventTracker.getBatchId() : "");
            params.addValue("flow_type", INWARD);
            params.addValue("msg_type", checkNull(msgEventTracker.getMsgType()));
            params.addValue("original_req", msgEventTracker.getOrgnlReq());
            params.addValue("original_req_count", null);
            params.addValue("consolidate_amt", null);
            params.addValue("intermediate_req", null);
            params.addValue("intemdiate_count", null);
            params.addValue("invalid_msg", msgEventTracker.getInvalidReq());
            params.addValue("status", SENT_TO_DISPATCHER);
            params.addValue("batch_creation_date", msgEventTracker.getBatchCreationDate() != null ? msgEventTracker.getBatchCreationDate() : null);
            params.addValue("batch_timestamp", msgEventTracker.getBatchCreationTimestamp() != null ? msgEventTracker.getBatchCreationTimestamp() : null);
            params.addValue("created_time", localDateTime);
            params.addValue("modified_timestamp", timestamp);
            params.addValue("version", 1.0);

            namedParameterJdbcTemplate.update(sql, params);
        } else {
            String selectSql = "SELECT msg_id FROM network_il.msg_event_tracker WHERE msg_id = :msgId";

            Map<String, Object> params1 = new HashMap<>();
            params1.put("msgId", msgEventTracker.getMsgId());

            try {
                // Try to fetch the row
                String foundId = namedParameterJdbcTemplate.queryForObject(selectSql, params1, String.class);

                // If found, perform the update
                String updateSql = "UPDATE network_il.msg_event_tracker SET target = :target, batch_id =:batch_id, flow_type= :flow_type, msg_type= :msg_type," +
                        "original_req_count = :original_req_count, consolidate_amt = :consolidate_amt, intermediate_req = :intermediate_req, intemdiate_count = :intemdiate_count, status = :status, batch_creation_date= :batch_creation_date, batch_timestamp = :batch_timestamp, modified_timestamp = :modified_timestamp " +
                        "WHERE msg_id = :msg_id";

                MapSqlParameterSource params = new MapSqlParameterSource();
                params.addValue("msg_id", msgEventTracker.getMsgId());
                //params.addValue("source", msgEventTracker.getSource());
                params.addValue("target", msgEventTracker.getTarget());
                params.addValue("batch_id", msgEventTracker.getBatchId() != null ? msgEventTracker.getBatchId() : "");
                params.addValue("flow_type", checkNull(msgEventTracker.getFlowType()));
                params.addValue("msg_type", checkNull(msgEventTracker.getMsgType()));
                //params.addValue("original_req", checkNull(msgEventTracker.getOrgnlReq()));
                params.addValue("original_req_count", null);
                params.addValue("consolidate_amt", null);
                params.addValue("intermediate_req", null);
                params.addValue("intemdiate_count", null);
                params.addValue("status", SENT_TO_DISPATCHER);
                params.addValue("batch_creation_date", msgEventTracker.getBatchCreationDate() != null ? msgEventTracker.getBatchCreationDate() : null);
                params.addValue("batch_timestamp", msgEventTracker.getBatchCreationTimestamp() != null ? msgEventTracker.getBatchCreationTimestamp() : null);
                //params.addValue("created_time", timestamp);
                params.addValue("modified_timestamp", timestamp);
                //  params.addValue("version", new BigDecimal("1.0"));

                int updated = namedParameterJdbcTemplate.update(updateSql, params);
                //return updated > 0;

            } catch (EmptyResultDataAccessException e) {
                // Row not found for given msgId
                //return false;
            }
        }
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

}
