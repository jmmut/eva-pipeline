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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.readFirstLine;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import uk.ac.ebi.eva.pipeline.configuration.VepInputGeneratorStepConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.AnnotationJob;
import uk.ac.ebi.eva.pipeline.jobs.PopulationStatisticsJobTest;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

/**
 * @author Diego Poggioli
 *
 * Test {@link VepInputGeneratorStep}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { AnnotationJob.class, VepInputGeneratorStepConfiguration.class, JobLauncherTestUtils.class})
public class VepInputGeneratorStepTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private JobOptions jobOptions;

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
    }
    
    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(jobOptions.getDbName());
    }

    @Test
    public void shouldGenerateVepInput() throws Exception {
        String dump = PopulationStatisticsJobTest.class.getResource("/dump/VariantStatsConfigurationTest_vl").getFile();
        JobTestUtils.restoreMongoDbFromDump(dump, jobOptions.getDbName());
        File vepInputFile = new File(jobOptions.getVepInput());

        if(vepInputFile.exists())
            vepInputFile.delete();

        assertFalse(vepInputFile.exists());

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(VepInputGeneratorStep.FIND_VARIANTS_TO_ANNOTATE);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertTrue(vepInputFile.exists());
        assertEquals("20\t60343\t60343\tG/A\t+", readFirstLine(vepInputFile));
    }
}
