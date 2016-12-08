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
package uk.ac.ebi.eva.pipeline.jobs;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.configuration.VariantAggregatedConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test for {@link AggregatedVcfJob}
 */

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = {JobOptions.class, AggregatedVcfJob.class, VariantAggregatedConfiguration.class, JobLauncherTestUtils.class})
public class AggregatedVcfJobTest {

    // TODO this test can't be modified to use fully the temporary folder rule / mongo rule.

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobOptions jobOptions;

    private String input;

    private String outputDir;

    private String compressExtension;

    private String dbName;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System
            .getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void aggregatedTransformAndLoadShouldBeExecuted() throws Exception {
        Config.setOpenCGAHome(opencgaHome);
        mongoRule.getTemporaryDatabase(dbName);

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // check execution flow
        Assert.assertEquals(1, jobExecution.getStepExecutions().size());
        List<StepExecution> steps = new ArrayList<>(jobExecution.getStepExecutions());
        StepExecution load = steps.get(0);

        Assert.assertEquals(AggregatedVcfJob.LOAD_VARIANTS, load.getStepName());

        // check ((documents in DB) == (lines in file))
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        String file = jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF);
        long lines = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(file)));
        Assert.assertEquals(lines, JobTestUtils.count(iterator));

        // check that stats are loaded properly
        assertFalse(variantDBAdaptor.iterator(
                new QueryOptions()).next().getSourceEntries().values().iterator().next().getCohortStats().isEmpty());
    }

    @Test
    public void aggregationNoneIsNotAllowed() throws Exception {
        mongoRule.getTemporaryDatabase(dbName);
        VariantSource source =
                (VariantSource) jobOptions.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE);
        jobOptions.getVariantOptions().put(
                VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                        input,
                        source.getFileId(),
                        source.getStudyId(),
                        source.getStudyName(),
                        source.getType(),
                        VariantSource.Aggregation.NONE));

        Config.setOpenCGAHome(opencgaHome);

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.FAILED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.FAILED, jobExecution.getStatus());
    }


    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();

        input = jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF);
        outputDir = jobOptions.getOutputDir();
        compressExtension = jobOptions.getPipelineOptions().getString("compressExtension");
        dbName = jobOptions.getPipelineOptions().getString(JobParametersNames.DB_NAME);

        String inputFile = AggregatedVcfJobTest.class.getResource(input).getFile();
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, inputFile);
    }

}
