package org.gbif.content.crawl.contentful.crawl;

import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class ProgrammeLinker {
    private static final Logger LOG = LoggerFactory.getLogger(ProgrammeLinker.class);

    private final CDAClient cdaClient;

    private final String programmeContentTypeId;

    private final String projectContentTypeId;

    private String programmeAcronym;

    public ProgrammeLinker(CDAClient cdaClient, String programmeContentTypeId, String projectContentTypeId) {
        this.cdaClient = cdaClient;
        this.programmeContentTypeId = programmeContentTypeId;
        this.projectContentTypeId = projectContentTypeId;
    }

    public void collectProgrammeAcronym(CDAEntry cdaEntry) {
        if (projectContentTypeId.equals(cdaEntry.contentType().id()) &&  cdaEntry.rawFields().containsKey("programme")) {
            Map<String,Object> programmeField = (Map<String, Object>) cdaEntry.rawFields().get("programme");
            LOG.info("Programme found {}", programmeField);
            String programmeId = (String)programmeField.get("id");

            CDAEntry programmeEntry = cdaClient.fetch(CDAEntry.class).withContentType(programmeContentTypeId).one(programmeId);
            programmeAcronym = programmeEntry.getField("acronym");
        }
    }

    public String getProgrammeAcronym() {
        return programmeAcronym;
    }
}
