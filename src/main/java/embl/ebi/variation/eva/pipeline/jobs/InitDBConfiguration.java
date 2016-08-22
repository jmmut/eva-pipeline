/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.pipeline.steps.*;
import org.opencb.datastore.core.ObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@EnableBatchProcessing
@Import({InitDBArgsConfig.class, FeaturesLoad.class})
public class InitDBConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(InitDBConfiguration.class);
    public static final String jobName = "initialize-database";

    @Autowired
    JobBuilderFactory jobBuilderFactory;
    @Autowired
    StepBuilderFactory stepBuilderFactory;
    @Autowired
    private ObjectMap pipelineOptions;

    @Qualifier("featuresLoadStep")
    @Autowired private Step featuresLoadStep;


    @Bean
    @Qualifier("initDBJob")
    public Job initDBJob() {

        JobBuilder jobBuilder = jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer());

        return jobBuilder
                .start(featuresLoadStep)
                .build();
    }
}
