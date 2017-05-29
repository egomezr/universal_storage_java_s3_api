package com.universal.storage;

import junit.framework.TestCase;
import java.io.File;
import com.universal.error.UniversalStorageException;
import org.apache.commons.io.FileUtils;
import com.universal.util.FileUtil;
import com.universal.storage.settings.UniversalSettings;

/**
 * This class is the implementation of a storage that will manage files as a s3 bucket.
 * This implementation will manage file using a setting to store files within a s3 bucket.
 */
public class TestUniversalS3Storage extends TestCase {

    private static UniversalStorage us = null;
    protected void setUp() {
        try {
            if (us == null) {
                us = UniversalStorage.Impl.
                    getInstance(new UniversalSettings(new File("src/test/resources/settings.json")));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void setUpTest(String fileName, String folderName) {
        try {
            File newFile = new File(System.getProperty("user.home"), fileName);
            if (!newFile.exists()) {
                try {
                    newFile.createNewFile();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }

            us.storeFile(new File(System.getProperty("user.home"), fileName), folderName);
            us.storeFile(new File(System.getProperty("user.home"), fileName));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    public void testRetrieveFileAsS3Provider() {
        String fileName = System.nanoTime() + ".txt";
        try {
            setUpTest(fileName, "retrieve/innerfolder");
            us.retrieveFile("retrieve/innerfolder/" + fileName);
        } catch (UniversalStorageException e) {
            fail(e.getMessage());
        }

        try {
            us.retrieveFile("retrieve/innerfolder/Target.txttxt");
            fail("This method should throw an error.");
        } catch (UniversalStorageException ignore) {
            
        }

        try {
            FileUtils.copyInputStreamToFile(us.retrieveFileAsStream("retrieve/innerfolder/" + fileName), 
                new File(FileUtil.completeFileSeparator(System.getProperty("user.home")) + fileName));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * This test will execute the remove file process using a s3 provider.
     */
    public void testRemoveFileAsS3Provider() {
        String fileName = System.nanoTime() + ".txt";
        setUpTest(fileName, "remove/innerfolder");

        try {
            us.removeFile(fileName);
            us.removeFile("remove/innerfolder/" + fileName);
            us.removeFile("remove/" + fileName);
        } catch (UniversalStorageException e) {
            fail(e.getMessage());
        }
    }

    /**
     * This test will execute the create folder process using a s3 provider.
     */
    public void testCreateFolderAsS3Provider() {
        try {
            us.createFolder("myNewFolder");
            us.removeFolder("myNewFolder");
        } catch (UniversalStorageException e) {
            fail(e.getMessage());
        }
    }

    /**
     * This test will clean the storage's context.
     */
    public void testCleanStorageAsS3Provider() {
        try {
            us.clean();
        } catch (UniversalStorageException e) {
            fail(e.getMessage());
        }
    }
}