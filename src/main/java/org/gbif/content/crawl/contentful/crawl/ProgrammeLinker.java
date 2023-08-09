/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.content.crawl.contentful.crawl;

import com.contentful.java.cda.CDAEntry;
public class ProgrammeLinker {

    private final String projectContentTypeId;

    private String programmeAcronym;

    public ProgrammeLinker(String projectContentTypeId) {
        this.projectContentTypeId = projectContentTypeId;
    }

    public void collectProgrammeAcronym(CDAEntry cdaEntry) {
        if (projectContentTypeId.equals(cdaEntry.contentType().id()) &&  cdaEntry.rawFields().containsKey("programme")) {
            CDAEntry programme = cdaEntry.getField("programme");
            if (programme != null && programme.rawFields().containsKey("acronym")) {
                programmeAcronym = programme.getField("acronym");
            }
        }
    }

    public String getProgrammeAcronym() {
        return programmeAcronym;
    }
}
