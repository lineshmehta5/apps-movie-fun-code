package org.superbiz.moviefun.albums;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableAsync
@EnableScheduling
public class AlbumsUpdateScheduler extends SchedulerDAO{

    private static final long SECONDS = 1000;
    private static final long MINUTES = 60 * SECONDS;

    private final AlbumsUpdater albumsUpdater;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public AlbumsUpdateScheduler(AlbumsUpdater albumsUpdater) {
        this.albumsUpdater = albumsUpdater;
    }


    @Scheduled(initialDelay = 15 * SECONDS, fixedRate = 1 * MINUTES)
    public void run() {
        if (!isJobRunning()) {
            try {
                logger.debug("Starting albums update");
                flagJobAsRunning();
                albumsUpdater.update();
                flagJobAsCompleted();
                logger.debug("Finished albums update");
            } catch (Throwable e) {
                logger.error("Error while updating albums", e);
            }
        } else {
            logger.debug("MULTIPLE JOB ALERT! Job won't run as someone else is already running it.");
        }
    }
}
