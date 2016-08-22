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
package embl.ebi.variation.eva;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

/**
 *
 * Class to extract configuration from properties files and from command line.
 * Default values are in resources/application.properties
 *
 * @author Diego Poggioli &lt;diego@ebi.ac.uk&gt;
 *
 * TODO: 20/05/2016 add type/null/file/dir validators
 * TODO validation checks for all the parameters
 */
@Component
public class InitDBArgs {
    private static final Logger logger = LoggerFactory.getLogger(InitDBArgs.class);

    // Input
    @Value("${input.gtf}") private String gtf;

    // Output
    @Value("${app.opencga.path}") private String opencgaAppHome;

    /// DB connection (most parameters read from OpenCGA "conf" folder)
    private String dbHosts;
    private String dbAuthenticationDb;
    private String dbUser;
    private String dbPassword;
    private String dbName;
    private String dbCollectionVariantsName;
    private String dbCollectionFilesName;
    @Value("${config.db.read-preference}") private String readPreference;
    @Value("${db.collections.features.name}") private String dbCollectionGenesName;

    //steps
    @Value("${config.restartability.allow:false}") private boolean allowStartIfComplete;

    private ObjectMap variantOptions  = new ObjectMap();
    private ObjectMap pipelineOptions  = new ObjectMap();

    public void loadArgs() throws IOException {
        logger.info("Loading job arguments");
        
        if (opencgaAppHome == null || opencgaAppHome.isEmpty()) {
            opencgaAppHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        }
        Config.setOpenCGAHome(opencgaAppHome);

        loadDbConnectionOptions();
        loadOpencgaOptions();
        loadPipelineOptions();
    }

    private void loadDbConnectionOptions() throws IOException {
        URI configUri = URI.create(Config.getOpenCGAHome() + "/").resolve("conf/").resolve("storage-mongodb.properties");
        Properties properties = new Properties();
        properties.load(new InputStreamReader(new FileInputStream(configUri.getPath())));
        
        dbHosts = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOSTS");
        dbAuthenticationDb = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.AUTHENTICATION.DB", "");
        dbUser = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER", "");
        dbPassword = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS", "");
        dbName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME");
        dbCollectionVariantsName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.VARIANTS", "variants");
        dbCollectionFilesName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.FILES", "files");
        
        if (dbHosts == null || dbHosts.isEmpty()) {
            throw new IllegalArgumentException("Please provide a database hostname");
        }
        if (dbName == null || dbName.isEmpty()) {
            throw new IllegalArgumentException("Please provide a database name");
        }
        if (dbCollectionVariantsName == null || dbCollectionVariantsName.isEmpty()) {
            throw new IllegalArgumentException("Please provide a name for the collection to store the variant information into");
        }
        if (dbCollectionFilesName == null || dbCollectionFilesName.isEmpty()) {
            throw new IllegalArgumentException("Please provide a name for the collection to store the file information into");
        }
    }
            
    private void loadOpencgaOptions() {
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME, dbName);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOSTS, dbHosts);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_AUTHENTICATION_DB, dbAuthenticationDb);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER, dbUser);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS, dbPassword);

        logger.debug("Using as variantOptions: {}", variantOptions.entrySet().toString());
    }

    private void loadPipelineOptions() {
        pipelineOptions.put("input.gtf", gtf);
        pipelineOptions.put("dbHosts", dbHosts);
        pipelineOptions.put("dbAuthenticationDb", dbAuthenticationDb);
        pipelineOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put("dbCollectionVariantsName", dbCollectionVariantsName);
        pipelineOptions.put("dbCollectionFilesName", dbCollectionFilesName);
        pipelineOptions.put("db.collections.features.name", dbCollectionGenesName);
        pipelineOptions.put("dbUser", dbUser);
        pipelineOptions.put("dbPassword", dbPassword);
        pipelineOptions.put("config.db.read-preference", readPreference);

        pipelineOptions.put("config.restartability.allow", allowStartIfComplete);

        logger.debug("Using as pipelineOptions: {}", pipelineOptions.entrySet().toString());
    }

    public ObjectMap getVariantOptions() {
        return variantOptions;
    }

    public ObjectMap getPipelineOptions() {
        return pipelineOptions;
    }
}
