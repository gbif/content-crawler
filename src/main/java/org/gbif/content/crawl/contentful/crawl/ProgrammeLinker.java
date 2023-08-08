package org.gbif.content.crawl.contentful.crawl;

import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAEntry;

import java.util.Map;
import java.util.Optional;

public class ProgrammeLinker {

    private final CDAClient cdaClient;

    private final String programmeContentTypeId;

    private String programmeAcronym;

    public ProgrammeLinker(CDAClient cdaClient, String programmeContentTypeId) {
        this.cdaClient = cdaClient;
        this.programmeContentTypeId = programmeContentTypeId;
    }

    public void collectProgrammeAcronym(CDAEntry cdaEntry) {
        if (cdaEntry.rawFields().containsKey("programme")) {
            Map<String,Object> programmeField = (Map<String, Object>) cdaEntry.rawFields().get("programme");
            String programmeId = (String)programmeField.get("id");

            CDAEntry programmeEntry = cdaClient.fetch(CDAEntry.class).withContentType(programmeContentTypeId).one(programmeId);
            programmeAcronym = programmeEntry.getField("acronym");
        }
    }

    public String getProgrammeAcronym() {
        return programmeAcronym;
    }
}
