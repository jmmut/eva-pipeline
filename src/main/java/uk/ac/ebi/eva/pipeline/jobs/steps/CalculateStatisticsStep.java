/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.readers.VcfReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.writers.StatisticsMongoWriterConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.writers.VariantWriterConfiguration;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.StatisticsProcessor;
import uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.PopulationStatisticsGeneratorStep;
import uk.ac.ebi.eva.pipeline.model.PopulationStatistics;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import java.util.List;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.CALCULATE_STATISTICS_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_DB_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_STATISTICS_PROCESSOR;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_WRITER;

/**
 * Configuration class that inject a step created with the tasklet {@link PopulationStatisticsGeneratorStep}
 */
@Configuration
@EnableBatchProcessing
@Import({VariantDBReader.class, StatisticsProcessor.class, StatisticsMongoWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class})
public class CalculateStatisticsStep {

    private static final Logger logger = LoggerFactory.getLogger(CalculateStatisticsStep.class);

    @Autowired
    @Qualifier(VARIANT_DB_READER)
    private ItemReader<Variant> reader;

    @Autowired
    @Qualifier(VARIANT_STATISTICS_PROCESSOR)
    private ItemProcessor<Variant, List<PopulationStatistics>> processor;

    @Autowired
    @Qualifier(VARIANT_WRITER)
    private ItemWriter<List<PopulationStatistics>> writer;

    @Bean(CALCULATE_STATISTICS_STEP)
    public Step generateCalculateStatisticsStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions,
            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        logger.debug("Building '" + CALCULATE_STATISTICS_STEP + "'");

        return stepBuilderFactory.get(CALCULATE_STATISTICS_STEP)
                .<Variant, List<PopulationStatistics>>chunk(chunkSizeCompletionPolicy)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .build();
    }
}
