package de.intranda.goobi.plugins;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.internal.WhiteboxImpl;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.NIOFileUtils;
import de.sub.goobi.helper.StorageProvider;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ConfigPlugins.class, ZopExportPlugin.class, StorageProvider.class })
@PowerMockIgnore({ "javax.management.*", "javax.net.ssl.*", "jdk.internal.reflect.*" })
public class ZopExportPluginTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private File tempFolder;
    private static String resourcesFolder;
    private String tempFolderDirectory;

    @BeforeClass
    public static void setUpClass() throws Exception {
        resourcesFolder = "src/test/resources/"; // for junit tests in eclipse

        if (!Files.exists(Paths.get(resourcesFolder))) {
            resourcesFolder = "target/test-classes/"; // to run mvn test from cli or in jenkins
        }

        String log4jFile = resourcesFolder + "log4j2.xml"; // for junit tests in eclipse
        System.setProperty("log4j.configurationFile", log4jFile);
    }

    @Before
    public void setUp() throws Exception {
        tempFolder = folder.newFolder("tmp");
        tempFolderDirectory = tempFolder.getAbsolutePath().toString();

        PowerMock.mockStatic(ConfigPlugins.class);
        EasyMock.expect(ConfigPlugins.getPluginConfig(EasyMock.anyString())).andReturn(getConfig()).anyTimes();
        PowerMock.replay(ConfigPlugins.class);

        PowerMock.mockStatic(StorageProvider.class);
        EasyMock.expect(StorageProvider.getInstance()).andReturn(new NIOFileUtils());
        PowerMock.replay(StorageProvider.class);
    }

    @Test
    public void testConstructor() {
        ZopExportPlugin plugin = new ZopExportPlugin();
        assertNotNull(plugin);
    }

    private XMLConfiguration getConfig() {
        String file = "plugin_intranda_export_sample.xml";
        XMLConfiguration config = new XMLConfiguration();
        config.setDelimiterParsingDisabled(true);
        try {
            config.load(resourcesFolder + file);
        } catch (ConfigurationException e) {
        }
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        return config;
    }

    /*================= Tests for the private methods ================= */

    /* Tests for the method createFolderLocal(Path) */
    @Test
    public void testCreateFolderLocalGivenNull() throws Exception {
        ZopExportPlugin plugin = new ZopExportPlugin();
        assertFalse(WhiteboxImpl.invokeMethod(plugin, "createFolderLocal", null));
    }

    @Test
    public void testCreateFolderLocalGivenEmptyPath() throws Exception {
        ZopExportPlugin plugin = new ZopExportPlugin();
        assertFalse(WhiteboxImpl.invokeMethod(plugin, "createFolderLocal", Paths.get("")));
    }

    @Test
    public void testCreateFolderLocalGivenExistingPath() throws Exception {
        ZopExportPlugin plugin = new ZopExportPlugin();
        Path temp = tempFolder.toPath();
        assertTrue(Files.exists(temp));
        assertTrue(WhiteboxImpl.invokeMethod(plugin, "createFolderLocal", temp));
    }

    @Test
    public void testCreateFolderLocalGivenUnexistingPath() throws Exception {
        ZopExportPlugin plugin = new ZopExportPlugin();
        final Path path = Path.of(tempFolderDirectory, "unexisting_path");
        assertFalse(Files.exists(path));
        assertTrue(WhiteboxImpl.invokeMethod(plugin, "createFolderLocal", path));
        assertTrue(Files.exists(path));
        Files.delete(path);
        assertFalse(Files.exists(path));
    }

    /* Tests for the method createCTLLocal(Path) */
    @Test
    public void testCreateCTLLocalGivenAPath() throws Exception {
        ZopExportPlugin plugin = new ZopExportPlugin();
        final Path path = Path.of(tempFolderDirectory, "ctl_test");
        final Path filePath = path.getParent().resolve(path.getFileName().toString().concat(".ctl"));
        assertFalse(Files.exists(filePath));
        WhiteboxImpl.invokeMethod(plugin, "createCTLLocal", path);
        assertTrue(Files.exists(filePath));
        Files.delete(filePath);
        assertFalse(Files.exists(filePath));
    }

}
