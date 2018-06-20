package org.superbiz.moviefun.albums;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class SchedulerDAO {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public boolean isJobRunning() {
        logger.debug("Checking scheduler_monitor table to see if job is already running...");
        return jdbcTemplate.query("select is_job_running from scheduler_monitor", new ResultSetExtractor<Boolean>() {
            @Override
            public Boolean extractData(ResultSet rs) throws SQLException,
                    DataAccessException {
                return rs.next() ? true : false;
            }
        });
    }

    public boolean flagJobAsRunning() {
        logger.debug("updating table to flag it as running");
        int exitCode = jdbcTemplate.update(
                "INSERT INTO scheduler_monitor (is_job_running) VALUES ('YES')");
        logger.debug("result from insert statement is " + exitCode);
        return exitCode==1?true:false;
    }

    public boolean flagJobAsCompleted() {
        logger.debug("deleting table to flag it as no jobs running");
        int exitCode = jdbcTemplate.update(
                "DELETE FROM scheduler_monitor");
        logger.debug("result from delete statement is " + exitCode);
        return exitCode==1?true:false;
    }
}

