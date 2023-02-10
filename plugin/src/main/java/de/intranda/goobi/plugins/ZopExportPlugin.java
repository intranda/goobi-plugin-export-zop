package de.intranda.goobi.plugins;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.util.StringUtil;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.production.enums.LogType;
import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.interfaces.IExportPlugin;
import org.goobi.production.plugin.interfaces.IPlugin;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.StorageProviderInterface;
import de.sub.goobi.helper.VariableReplacer;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.ExportFileException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.helper.exceptions.UghHelperException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import net.xeoh.plugins.base.annotations.PluginImplementation;
import ugh.dl.DigitalDocument;
import ugh.dl.DocStruct;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.Prefs;
import ugh.exceptions.DocStructHasNoTypeException;
import ugh.exceptions.MetadataTypeNotAllowedException;
import ugh.exceptions.PreferencesException;
import ugh.exceptions.ReadException;
import ugh.exceptions.TypeNotAllowedForParentException;
import ugh.exceptions.WriteException;

@PluginImplementation
@Log4j2
public class ZopExportPlugin implements IExportPlugin, IPlugin {

    private static final long serialVersionUID = 1476710081498869499L;
    private static final String ABORTION_MESSAGE = "Export aborted for process with ID ";
    private static final String COMPLETION_MESSAGE = "Export executed for process with ID ";
    private static final String NAME_SEPARATOR = "-"; // used to connect the foldername's two parts for multi-volume works
    @Getter
    private String title = "intranda_export_zop";
    @Getter
    private PluginType type = PluginType.Export;
    @Getter
    @Setter
    private Step step;

    @Getter
    private List<String> problems;

    private transient ChannelSftp sftpChannel;
    private String username;
    private String hostname;
    private String keyPath;

    @Override
    public void setExportFulltext(boolean arg0) {
        // will not be used in this plugin
    }

    @Override
    public void setExportImages(boolean arg0) {
        // will not be used in this plugin
    }

    @Override
    public boolean startExport(Process process) throws IOException, InterruptedException, DocStructHasNoTypeException, PreferencesException,
    WriteException, MetadataTypeNotAllowedException, ExportFileException, UghHelperException, ReadException, SwapException, DAOException,
    TypeNotAllowedForParentException {
        String benutzerHome = process.getProjekt().getDmsImportImagesPath();
        return startExport(process, benutzerHome);
    }

    @Override
    public boolean startExport(Process process, String destination) throws IOException, InterruptedException, DocStructHasNoTypeException,
    PreferencesException, WriteException, MetadataTypeNotAllowedException, ExportFileException, UghHelperException, ReadException,
    SwapException, DAOException, TypeNotAllowedForParentException {

        log.debug("=============================== Starting ZOP Export ===============================");

        String masterPath = process.getImagesOrigDirectory(false);
        log.debug("masterPath is: " + masterPath);
        // assure that the source folder is not empty
        if (new File(masterPath).list().length == 0) {
            logBoth(process.getId(), LogType.ERROR, "There is nothing to copy from '" + masterPath + "', it is empty!");
            logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
            return false;
        }

        // read information from config file
        SubnodeConfiguration config = getConfig(process);
        String path = config.getString("path").trim();
        // destination will be used as default value only if <path> is not configured
        // hence we only have to assure that it is not null in that scenario
        if (StringUtils.isBlank(path)) {
            log.debug("Target 'path' is not configured, using default settings instead.");
            if (StringUtils.isBlank(destination)) {
                log.debug("The parameter 'destination' is invalid, restarting export with default settings.");
                return startExport(process);
            }
            path = destination;
        }

        // read information from config file
        String fieldIdentifier = config.getString("identifier").trim();
        String fieldVolume = config.getString("volume").trim();

        if (StringUtils.isBlank(fieldIdentifier) || StringUtils.isBlank(fieldVolume)) {
            logBoth(process.getId(), LogType.ERROR, "The configuration file for the ZOP export is incomplete.");
            logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
            return false;
        }

        Path savingPath;

        String id = ""; // aimed to be the system number, e.g. ALMA MMS-ID
        String volumeTitle = ""; // used to distinguish volumes from one another
        String folderName; // prepared for the new folder to be created

        boolean isOneVolumeWork = true;

        // read mets file to get its logical structure
        try {
            Fileformat ff = process.readMetadataFile();
            DigitalDocument dd = ff.getDigitalDocument();
            DocStruct logical = dd.getLogicalDocStruct();
            Prefs prefs = process.getRegelsatz().getPreferences();
            VariableReplacer replacer = new VariableReplacer(dd, prefs, process, null);

            // replace Goobi Variables in the path string and get the Path object of it
            path = replacer.replace(path);
            savingPath = Paths.get(path);
            log.debug("target path = " + path);

            // get the ID
            id = findMetadata(logical, fieldIdentifier);
            // assure that id is valid
            if (StringUtils.isBlank(id)) {
                logBoth(process.getId(), LogType.ERROR, "No valid id found. It seems that " + fieldIdentifier + " is invalid. Recheck it please.");
                logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
                return false;
            }

            // get the volumeTitle if the work is composed of several volumes 
            if (logical.getType().isAnchor()) {
                isOneVolumeWork = false;
                logical = logical.getAllChildren().get(0);
                volumeTitle = findMetadata(logical, fieldVolume).replace(" ", "_");
                if (StringUtils.isBlank(volumeTitle)) {
                    logBoth(process.getId(), LogType.ERROR,
                            "No valid volumeTitle found. It seems that " + fieldVolume + " is invalid. Recheck it please.");
                    logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
                    return false;
                }
            }

            log.debug("isOneVolumeWork = " + isOneVolumeWork);
            log.debug("id = " + id);
            if (!isOneVolumeWork) {
                log.debug("volumeTitle = " + volumeTitle);
            }

        } catch (ReadException | PreferencesException | IOException | SwapException e) {
            logBoth(process.getId(), LogType.ERROR, "Error happened: " + e);
            logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
            return false;
        }

        // prepare sftpChannel if necessary
        boolean useSftp = config.getBoolean("sftp", false);
        if (useSftp) {
            username = config.getString("username").trim();
            hostname = config.getString("hostname").trim();
            keyPath = config.getString("keyPath").trim();

            if (StringUtil.isBlank(username) || StringUtil.isBlank(hostname)) {
                logBoth(process.getId(), LogType.ERROR, "The configuration file for the ZOP export is incomplete.");
                logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
                return false;
            }

            try {
                sftpChannel = setupJSch();
                sftpChannel.connect();
            } catch (JSchException e) {
                log.debug("failed to initialize sftpChannel");
                e.printStackTrace();
                return false;
            }
        }

        folderName = isOneVolumeWork ? id : id + NAME_SEPARATOR + volumeTitle;
        log.debug("folderName = " + folderName);

        // create a folder named after folderName
        savingPath = savingPath.resolve(folderName);
        if (!createFolder(useSftp, savingPath)) {
            logBoth(process.getId(), LogType.ERROR, "Something went wrong trying to create the directory: " + savingPath.toString());
            logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
            return false;
        }

        // if everything went well so far, then we only need to do the copy
        return tryCopy(process, Paths.get(masterPath), savingPath, useSftp);
    }

    /**
     * 
     * @param process
     * @return SubnodeConfiguration object according to the project's name
     */
    private SubnodeConfiguration getConfig(Process process) {
        String projectName = process.getProjekt().getTitel();
        log.debug("projectName = " + projectName);
        XMLConfiguration xmlConfig = ConfigPlugins.getPluginConfig(title);
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());
        xmlConfig.setReloadingStrategy(new FileChangedReloadingStrategy());
        SubnodeConfiguration conf = null;

        // order of configuration is:
        // 1.) project name matches
        // 2.) project is *
        try {
            conf = xmlConfig.configurationAt("//config[./project = '" + projectName + "']");
        } catch (IllegalArgumentException e) {
            conf = xmlConfig.configurationAt("//config[./project = '*']");
        }

        return conf;
    }

    /**
     * 
     * @param useSftp true if use SFTP, false otherwise
     * @param path absolute path of the target folder
     * @return true if the folder already exists or is successfully created, false if failure happened.
     */
    private boolean createFolder(boolean useSftp, Path path) {
        if (path == null) {
            log.error("The path provided is null!");
            return false;
        }
        if (StringUtils.isBlank(path.toString())) {
            log.error("The path provided is empty!");
            return false;
        }
        try {
            return useSftp ? createFolderSftp(path) : createFolderLocal(path);
        } catch (SftpException e) {
            log.error("Failed to create directory remotely: " + path.toString());
            return false;
        }
    }

    /**
     * 
     * @param path the absolute path of the targeted folder
     * @return true if the folder already exists or is successfully created, false if failure happens.
     */
    private boolean createFolderLocal(Path path) {
        if (path == null) {
            log.error("The path provided is null!");
            return false;
        }
        if (StringUtils.isBlank(path.toString())) {
            log.error("The path provided is empty!");
            return false;
        }
        StorageProviderInterface provider = StorageProvider.getInstance();
        if (provider.isFileExists(path)) {
            log.debug("Directory already exisits: " + path.toString());
            return true;
        }
        try {
            provider.createDirectories(path);
            log.debug("Directory created: " + path.toString());
            return true;
        } catch (IOException e) {
            log.error("Failed to create directory: " + path.toString());
            return false;
        }
    }

    /**
     * 
     * @param path absolute path of the target folder
     * @return true if the folder already exists or is successfully created, false if failure happened.
     * @throws SftpException
     */
    private boolean createFolderSftp(Path path) throws SftpException {
        sftpChannel.cd("/");
        log.debug("pwd = " + sftpChannel.pwd());

        boolean directoryCreated = false;
        String pathString = path.toString();
        String[] folders = pathString.split("/");
        for (String folder : folders) {
            if (folder.equals(".") || folder.equals("..")) {
                sftpChannel.cd(folder);
                continue;
            }
            if (folder.length() > 0 && !folder.contains(".")) { // avoid creating hidden folders
                // this is a valid folder
                try {
                    sftpChannel.cd(folder);
                    log.debug("pwd = " + sftpChannel.pwd());
                } catch (SftpException e) {
                    // no such folder yet, hence try to create it first
                    sftpChannel.mkdir(folder);
                    log.debug("folder created: " + folder);
                    sftpChannel.cd(folder);
                    log.debug("pwd = " + sftpChannel.pwd());
                    directoryCreated = true;
                }
            }
        }
        // check if the directory is successfully created
        if (sftpChannel.ls(pathString).size() >= 2) { // because of the existence of `.` and `..` in empty folders
            String temp = directoryCreated ? "Directory created remotely: " : "Directory already exisits remotely: ";
            log.debug(temp + path.toString());
            return true;
        }

        return false;
    }

    /**
     * 
     * @param logical logical structure of a book as an object of DocStruct
     * @param fieldName value of which we want inside "logical"
     * @return the value of "fieldName" inside "logical" as String
     */
    private String findMetadata(DocStruct logical, String fieldName) {
        String fieldValue = "";
        for (Metadata md : logical.getAllMetadata()) {
            if (md.getType().getName().equals(fieldName)) {
                // field found
                fieldValue = md.getValue().trim();
                break;
            }
        }
        return fieldValue;
    }

    /**
     * 
     * @param process
     * @param fromPath absolute path to the source folder
     * @param toPath absolute path to the target folder
     * @param useSftp true if use SFTP, false otherwise
     * @return true if the copy is successfully performed, false otherwise
     */
    private boolean tryCopy(Process process, Path fromPath, Path toPath, boolean useSftp) {
        try {
            return useSftp ? tryCopySftp(process, fromPath, toPath) : tryCopyLocal(process, fromPath, toPath);
        } finally {
            if (sftpChannel!=null) {
                sftpChannel.exit();                
            }
            log.debug("=============================== Stopping ZOP Export ===============================");
        }
    }

    /**
     * 
     * @param process
     * @param fromPath absolute path to the souce folder
     * @param toPath absolute path to the target folder
     * @return true if the copy is successfully performed, false otherwise
     */
    private boolean tryCopyLocal(Process process, Path fromPath, Path toPath) {
        StorageProviderInterface provider = StorageProvider.getInstance();
        if (!provider.list(toPath.toString()).isEmpty()) {
            logBoth(process.getId(), LogType.ERROR, "The directory: '" + toPath.toString() + "' is not empty!");
            logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
            return false;
        }
        // if the folder is empty, great!
        try {
            copyImagesLocal(fromPath, toPath);
            createCTLLocal(toPath);

        } catch (IOException e) {
            logBoth(process.getId(), LogType.ERROR,
                    "Errors happened trying to copy from '" + fromPath.toString() + "' to '" + toPath.toString() + "'.");
            logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
            return false;
        }
        logBoth(process.getId(), LogType.INFO, "Images from '" + fromPath.toString() + "' are successfully copied to '" + toPath.toString() + "'.");
        logBoth(process.getId(), LogType.INFO, COMPLETION_MESSAGE + process.getId());
        return true;
    }

    /**
     * 
     * @param process
     * @param fromPath absolute path to the source folder
     * @param toPath absolute path to the target folder
     * @return true if the copy is successfully performed, false otherwise
     */
    private boolean tryCopySftp(Process process, Path fromPath, Path toPath) {
        try {
            // check if the targeted directory is empty:
            if (sftpChannel.ls(toPath.toString()).size() > 2) { // because of the existence of `.` and `..` in empty folders
                logBoth(process.getId(), LogType.ERROR, "The directory: '" + toPath.toString() + "' is not empty!");
                logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
                return false;
            }
            // if the folder is empty, great!
            copyImagesSftp(fromPath, toPath);
            createCTLSftp(toPath);

        } catch (SftpException e) {
            logBoth(process.getId(), LogType.ERROR,
                    "Errors happened trying to copy from '" + fromPath.toString() + "' to '" + username + "@" + hostname + ":" + toPath.toString()
                            + "'.");
            logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
            return false;
        } catch (IOException ee) {
            logBoth(process.getId(), LogType.ERROR, "Errors happened trying to create the .ctl file.");
            logBoth(process.getId(), LogType.ERROR, ABORTION_MESSAGE + process.getId());
            return false;
        }
        logBoth(process.getId(), LogType.INFO, "Images from '" + fromPath.toString() + "' are successfully copied to '" + username + "@" + hostname
                + ":" + toPath.toString() + "'.");
        logBoth(process.getId(), LogType.INFO, COMPLETION_MESSAGE + process.getId());
        return true;
    }

    /**
     * 
     * @param fromPath absolute path to the source folder
     * @param toPath absolute path to the targeted folder
     * @throws IOException
     */
    private void copyImagesLocal(Path fromPath, Path toPath) throws IOException {
        log.debug("Copy images from '" + fromPath.toString() + "' to '" + toPath.toString() + "'.");
        StorageProviderInterface provider = StorageProvider.getInstance();
        List<String> files = provider.list(fromPath.toString());

        for (String file : files) {
            Path srcPath = fromPath.resolve(file);
            Path destPath = toPath.resolve(file);
            provider.copyFile(srcPath, destPath);

            // get the checksums of the original file and the copy
            String fromChecksum = DigestUtils.sha256Hex(Files.newInputStream(srcPath));
            String toChecksum = DigestUtils.sha256Hex(Files.newInputStream(destPath));

            // compare these two checksums
            // if they are not equal, then something went wrong during the copy process of this file
            if (!fromChecksum.equals(toChecksum)) {
                // retry once
                provider.deleteFile(destPath);
                provider.copyFile(srcPath, destPath);
                toChecksum = DigestUtils.sha256Hex(Files.newInputStream(destPath));
                // if still not equal, delete the already copied contents and throw an IOException
                if (!fromChecksum.equals(toChecksum)) {
                    log.error("Checksum check failed twice while trying to copy the file: '" + srcPath.toString() + "'");
                    log.debug("checksum original = " + fromChecksum);
                    log.debug("checksum after copy = " + toChecksum);
                    log.debug("Already copied contents will be deleted.");
                    provider.deleteInDir(toPath);
                    provider.deleteDir(toPath);
                    throw new IOException("Checksum check failed twice!");
                }
            }
        }
    }

    /**
     * @param fromPath absolute path to the source folder
     * @param toPath absolute path to the target folder
     * @throws SftpException
     */
    private void copyImagesSftp(Path fromPath, Path toPath) throws SftpException {
        log.debug("Copy images from '" + fromPath.toString() + "' to '" + username + "@" + hostname + ":" + toPath.toString() + "'.");
        StorageProviderInterface provider = StorageProvider.getInstance();
        List<String> files = provider.list(fromPath.toString());
        for (String file : files) {
            Path srcPath = fromPath.resolve(file);
            Path destPath = toPath.resolve(file);
            sftpChannel.put(srcPath.toString(), destPath.toString());
        }

        // No need for a checksum checking logic here, since the JSch library uses its internal algorithms to assure the integrity of transfered data.
    }

    /**
     * 
     * @param path whose folderName and parent will be used
     * @throws IOException
     */
    private void createCTLLocal(Path path) throws IOException {
        // the .ctl file should have the same name as the folder specified by this path
        String fileName = path.getFileName().toString().concat(".ctl");
        // and it should be created next to the folder, i.e. into the folder's parent's path
        Path parentPath = path.getParent();
        StorageProviderInterface provider = StorageProvider.getInstance();
        try {
            provider.createFile(parentPath.resolve(fileName));
        } catch (IOException e) {
            log.debug("Some error happened while trying to create the .ctl file.");
            throw e;
        }
    }

    /**
     * 
     * @param path whose folderName and parent will be used
     * @throws IOException
     * @throws SftpException
     */
    private void createCTLSftp(Path path) throws IOException, SftpException {
        // the .ctl file should have the same name as the folder specified by this path
        String fileName = path.getFileName().toString().concat(".ctl");
        // and it should be created next to the folder, i.e. into the folder's parent's path
        Path parentPath = path.getParent();
        StorageProviderInterface provider = StorageProvider.getInstance();
        try {
            // create the empty .ctl file locally under the default temporary folder, e.g. /tmp for Linux
            Path srcPath = Path.of(System.getProperty("java.io.tmpdir"), fileName);
            provider.createFile(srcPath);

            // copy this .ctl file to the remote location
            Path destPath = parentPath.resolve(fileName);
            sftpChannel.put(srcPath.toString(), destPath.toString());

            // remove the local .ctl file
            provider.deleteFile(srcPath);
        } catch (IOException e) {
            log.debug("Some error happened while trying to create the .ctl file locally.");
            throw e;
        }
    }

    /**
     * 
     * @param processId
     * @param logType
     * @param message message to be shown to both terminal and journal
     */
    private void logBoth(int processId, LogType logType, String message) {
        String logMessage = "ZOP Export Plugin: " + message;
        switch (logType) {
            case ERROR:
                log.error(logMessage);
                break;
            case DEBUG:
                log.debug(logMessage);
                break;
            case WARN:
                log.warn(logMessage);
                break;
            default: // INFO
                log.info(logMessage);
                break;
        }
        if (processId > 0) {
            Helper.addMessageToProcessJournal(processId, logType, logMessage);
        }
    }

    /**
     * 
     * @return ChannelSftp object
     * @throws JSchException
     */
    private ChannelSftp setupJSch() throws JSchException {
        JSch.setConfig("StrictHostKeyChecking", "no");
        JSch jsch = new JSch();
        jsch.addIdentity(keyPath);
        Session jschSession = jsch.getSession(username, hostname);
        jschSession.connect();
        return (ChannelSftp) jschSession.openChannel("sftp");
    }

}