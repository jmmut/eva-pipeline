/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ebi.eva.pipeline.jobs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.getLines;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

/**
 * Test for {@link GenotypedVcfJob}
 * <p>
 * TODO: FILE_WRONG_NO_ALT should be renamed because the alt allele is not missing but is the same as the reference
 */
@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE,Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:genotyped-vcf.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {GenotypedVcfJob.class, BatchTestConfiguration.class})
public class GenotypedVcfJobTest {
    private static final String MOCK_VEP = "/mockvep.pl";

    private static final int EXPECTED_VALID_ANNOTATIONS = 536;

    private static final int EXPECTED_ANNOTATIONS = 537;

    private static final int EXPECTED_VARIANTS = 300;

    private static final String INPUT_VCF_ID = "1";

    private static final String INPUT_STUDY_ID = "genotyped-job";

    private static final String INPUT_FILE = "/small20.vcf.gz";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private static String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System
            .getenv("OPENCGA_HOME") : "/opt/opencga";

    @Test
    public void fullGenotypedVcfJob() throws Exception {
        Config.setOpenCGAHome(opencgaHome);
        File inputFile = getResource(INPUT_FILE);
        String dbName = mongoRule.getRandomTemporaryDatabaseName();

        String outputDirStats = temporaryFolderRule.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();

        File variantsStatsFile = new File(URLHelper.getVariantsStatsUri(outputDirStats, INPUT_STUDY_ID, INPUT_VCF_ID));
        File sourceStatsFile = new File(URLHelper.getSourceStatsUri(outputDirStats, INPUT_STUDY_ID, INPUT_VCF_ID));

        File vepInputFile = new File(URLHelper.resolveVepInput(outputDirAnnotation, INPUT_STUDY_ID, INPUT_VCF_ID));
        File vepOutputFile = new File(URLHelper.resolveVepOutput(outputDirAnnotation, INPUT_STUDY_ID, INPUT_VCF_ID));

        VariantDBIterator iterator;

        // Run the Job
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName("files")
                .collectionVariantsName("variants")
                .databaseName(dbName)
                .inputFasta("")
                .inputStudyId(INPUT_STUDY_ID)
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId(INPUT_VCF_ID)
                .outputDirAnnotation(outputDirAnnotation)
                .outputDirStats(outputDirStats)
                .vepCachePath("")
                .vepCacheSpecies("")
                .vepCacheVersion("")
                .vepNumForks("")
                .vepPath(getResource(MOCK_VEP).getPath())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // 1 load step: check ((documents in DB) == (lines in transformed file))
        //variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        //variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        iterator = getVariantDBIterator(dbName);
        assertEquals(EXPECTED_VARIANTS, count(iterator));

        // 2 create stats step
        assertTrue(variantsStatsFile.exists());
        assertTrue(sourceStatsFile.exists());

        // 3 load stats step: check the DB docs have the field "st"
        iterator = getVariantDBIterator(dbName);
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());

        // 4 annotation flow
        // annotation input vep generate step
        checkAnnotationInput(vepInputFile);

        // 5 annotation create step
        assertTrue(vepInputFile.exists());
        assertTrue(vepOutputFile.exists());

        // Check output file length
        assertEquals(EXPECTED_ANNOTATIONS, getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));

        // 6 Annotation load step: check documents in DB have annotation (only consequence type)
        checkLoadedAnnotation(dbName);

        //check that one line is skipped because malformed
        List<StepExecution> variantAnnotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals(BeanNames.LOAD_VEP_ANNOTATION_STEP))
                .collect(Collectors.toList());
        assertEquals(1, variantAnnotationLoadStepExecution.get(0).getReadSkipCount());

    }

    private VariantDBIterator getVariantDBIterator(String dbName) throws IllegalAccessException,
            ClassNotFoundException, InstantiationException, StorageManagerException {
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        return variantDBAdaptor.iterator(new QueryOptions());
    }

    private void checkAnnotationInput(File vepInputFile) throws IOException {
        BufferedReader testReader = new BufferedReader(new InputStreamReader(new FileInputStream(
                getResource("/preannot.sorted"))));
        BufferedReader actualReader = new BufferedReader(new InputStreamReader(new FileInputStream(
                vepInputFile.toString())));

        ArrayList<String> rows = new ArrayList<>();

        String s;
        while ((s = actualReader.readLine()) != null) {
            rows.add(s);
        }
        Collections.sort(rows);

        String testLine = testReader.readLine();
        for (String row : rows) {
            assertEquals(testLine, row);
            testLine = testReader.readLine();
        }
        assertNull(testLine); // if both files have the same length testReader should be after the last line
    }


    private void checkLoadedAnnotation(String dbName) throws IllegalAccessException, ClassNotFoundException,
            InstantiationException, StorageManagerException {
        VariantDBIterator iterator;
        iterator = getVariantDBIterator(dbName);

        int count = 0;
        int consequenceTypeCount = 0;
        while (iterator.hasNext()) {
            count++;
            Variant next = iterator.next();
            if (next.getAnnotation().getConsequenceTypes() != null) {
                consequenceTypeCount += next.getAnnotation().getConsequenceTypes().size();
            }
        }

        assertEquals(EXPECTED_VARIANTS, count);
        assertEquals(EXPECTED_VALID_ANNOTATIONS, consequenceTypeCount);
    }

}
