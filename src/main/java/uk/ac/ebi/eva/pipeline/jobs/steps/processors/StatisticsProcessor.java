/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantStats;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Convert a {@link DBObject} into {@link VariantWrapper}
 * Any extra filter, check, validation... should be placed here
 */
public class StatisticsProcessor implements ItemProcessor<Variant, Variant> {
    private static final Logger logger = LoggerFactory.getLogger(StatisticsProcessor.class);

    private String studyId;

    private Map<String, List<Integer>> populations;

    public StatisticsProcessor(String studyId, Map<String, List<Sample>> populations) {
        this.studyId = studyId;
        this.populations = populations;
    }

    @Override
    public Variant process(Variant variant) throws Exception {
        /*
        for each population
            for each sample
                get the genotype
                accumulate it
            save cohort stats in the variant
        return variant
         */
        Map<String, VariantStats> cohortStats = new HashMap<>(populations.size());
        for (Map.Entry<String, List<Integer>> population : populations.entrySet()) {
            List<Map<String, String>> samplesData = new ArrayList<>(population.getValue().size());
            for (Integer sample : population.getValue()) {
                Map<String, String> sampleData = searchGenotype(variant, studyId, sample);
                samplesData.add(sampleData);
            }
            VariantStats variantStats = new VariantStats(variant);
            variantStats.calculate(samplesData, null, null);
            cohortStats.put(population.getKey(), variantStats);
        }
        return "noooooo!";
    }

    private Map<String, String> searchGenotype(Variant variant, String studyId, Integer sample) {
        List<Map<String, String>> samplesData = variant.getSourceEntries().values().stream()
                .filter(sourceEntry -> sourceEntry.getStudyId().equals(studyId))
                .filter(sourceEntry -> sourceEntry.getSamplesData().size() > sample)
                .map(sourceEntry -> sourceEntry.getSampleData(sample)).collect(Collectors.toList());

        if (samplesData.size() > 1) {
            throw new IllegalStateException("the same sample " + sample + " has 2 entries in the study " + studyId
                    + " in variant at " + variant.toString());
        } else if (samplesData.size() == 0) {
            throw new IllegalStateException("the sample " + sample + " was not found in the study " + studyId
                    + " in variant at " + variant.toString());
        } else {
            return samplesData.get(0);
        }
    }
}
