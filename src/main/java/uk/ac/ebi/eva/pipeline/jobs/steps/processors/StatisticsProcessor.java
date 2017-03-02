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
import org.opencb.biodata.models.feature.Genotype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.models.data.Sample;
import uk.ac.ebi.eva.commons.models.data.SampleLocation;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.VariantStats;
import uk.ac.ebi.eva.pipeline.model.PopulationStatistics;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Convert a {@link DBObject} into {@link VariantWrapper}
 * Any extra filter, check, validation... should be placed here
 */
public class StatisticsProcessor implements ItemProcessor<Variant, List<PopulationStatistics>> {
    private static final Logger logger = LoggerFactory.getLogger(StatisticsProcessor.class);

    private String studyId;

    private Map<String, List<Sample>> populations;

    public StatisticsProcessor(String studyId, Map<String, List<Sample>> populations) {
        this.studyId = studyId;
        this.populations = populations;
    }

    @Override
    public List<PopulationStatistics> process(Variant variant) throws Exception {
        /*
        for each population
            for each sample
                get the genotype
                accumulate it
            save cohort stats in the variant
        return variant
         */
        List<PopulationStatistics> statsList = new ArrayList<>(populations.size());
        for (Map.Entry<String, List<Sample>> population : populations.entrySet()) {
            List<Map<String, String>> samplesData = new ArrayList<>(population.getValue().size());
            for (Sample sample : population.getValue()) {
                Map<String, String> sampleData = searchGenotype(variant, studyId, sample);
                samplesData.add(sampleData);
            }
            VariantStats variantStats = new VariantStats(variant);
            variantStats.calculate(samplesData, null, null);
            String populationName = population.getKey();
            PopulationStatistics populationStatistics = buildPopulationStatistics(variant, variantStats, populationName);
            statsList.add(populationStatistics);
        }
        return statsList;
    }

    public PopulationStatistics buildPopulationStatistics(Variant variant, VariantStats variantStats,
            String populationName) {
        return new PopulationStatistics(
                MongoDBHelper.buildStorageId(variant),
                variant.getChromosome(),
                variant.getStart(),
                variant.getReference(),
                variant.getAlternate(),
                populationName,
                studyId,
                variantStats.getMaf(),
                variantStats.getMgf(),
                variantStats.getMafAllele(),
                variantStats.getMgfGenotype(),
                variantStats.getMissingAlleles(),
                variantStats.getMissingGenotypes(),
                mapGenotypesToString(variantStats.getGenotypesCount()
                ));
    }

    private Map<String, Integer> mapGenotypesToString(Map<Genotype, Integer> genotypesCount) {
        return genotypesCount.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }

    private Map<String, String> searchGenotype(Variant variant, String studyId, Sample sample) {
        for (SampleLocation sampleLocation : sample.getLocations()) {
            VariantSourceEntry sourceEntry = variant.getSourceEntry(sampleLocation.getFileId(), studyId);
            if (sourceEntry != null) {
                return sourceEntry.getSampleData(sampleLocation.getIndex());
            }
        }
        throw new IllegalStateException("the sample " + sample + " was not found in the study " + studyId
                + " in variant at " + variant.toString());
    }
}
