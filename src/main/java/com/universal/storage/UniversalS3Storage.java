package com.universal.storage;

import com.universal.util.PathValidator;
import com.universal.util.FileUtil;
import com.universal.error.UniversalIOException;
import com.universal.storage.settings.UniversalSettings;
import java.io.File;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.io.FileUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.regions.Regions;


/**
 * The MIT License (MIT)
 * 
 * Copyright (c) 2015 Dynamicloud
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 * This class is the implementation of a storage that will manage files within a S3 bucket.
 * This implementation will manage file using a S3 bucket as a root storage.
 */
public class UniversalS3Storage extends UniversalStorage {
    private AmazonS3 s3client;
    /**
     * This constructor receives the settings for this new FileStorage instance.
     * 
     * @param settings for this new FileStorage instance.
     */
    public UniversalS3Storage(UniversalSettings settings) {
        super(settings);
        this.s3client = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(settings.getS3Region())).
                    withCredentials(new UniversalProfileCredentialsProvider(this.settings)).build();
    }

    /**
     * This method stores a file within the storage provider according to the current settings.
     * The method will replace the file if already exists within the root.
     * 
     * For exemple:
     * 
     * path == null
     * File = /var/wwww/html/index.html
     * Root = /storage/
     * Copied File = /storage/index.html
     * 
     * path == "myfolder"
     * File = /var/wwww/html/index.html
     * Root = /storage/
     * Copied File = /storage/myfolder/index.html
     * 
     * If this file is a folder, a error will be thrown informing that should call the createFolder method.
     * 
     * Validations:
     * Validates if root is a bucket.
     * 
     * @param file to be stored within the storage.
     * @param path is the path for this new file within the root.
     * @throws UniversalIOException when a specific IO error occurs.
     */
    void storeFile(File file, String path) throws UniversalIOException {
        if (file.isDirectory()) {
            throw new UniversalIOException(file.getName() + " is a folder.  You should call the createFolder method.");
        }

        if (path == null) {
            path = "";
        }
        
        ObjectMetadata objectMetadata = new ObjectMetadata();
        if (this.settings.getEncryption()) {
            objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);     
        }

        List<Tag> tags = new ArrayList<Tag>();
        for (String key : this.settings.getTags().keySet()) {
            tags.add(new Tag(key, this.settings.getTags().get(key)));
        }

        PutObjectRequest request = new PutObjectRequest(this.settings.getRoot() + ("".equals(path) ? "" : ("/" + path)), file.getName(), file);
        request.setMetadata(objectMetadata);
        request.setTagging(new ObjectTagging(tags));
        request.setStorageClass(getStorageClass());

        this.s3client.putObject(request);
    }

    /**
     * Gets the enum from StorageClass according to the storage class from the settings.
     */
    private StorageClass getStorageClass() {
        String sc = this.settings.getStorageClass();
        if ("REDUCED_REDUNDANCY".equals(sc)) {
            return StorageClass.ReducedRedundancy;
        } else if ("STANDARD_IA".equals(sc)) {
            return StorageClass.StandardInfrequentAccess;
        } else if ("STANDARD".equals(sc)) {
            return StorageClass.Standard;
        }
        
        return StorageClass.Standard;
    }

    /**
     * This method stores a file according to the provided path within the storage provider 
     * according to the current settings.
     * 
     * @param path pointing to the file which will be stored within the storage.
     * @throws UniversalIOException when a specific IO error occurs.
     */
    void storeFile(String path) throws UniversalIOException {
        this.storeFile(new File(path), null);
    }

    /**
     * This method stores a file according to the provided path within the storage provider according to the current settings.
     * 
     * @param path pointing to the file which will be stored within the storage.
     * @param targetPath is the path within the storage.
     * 
     * @throws UniversalIOException when a specific IO error occurs.
     */
    void storeFile(String path, String targetPath) throws UniversalIOException {
        PathValidator.validatePath(path);
        PathValidator.validatePath(targetPath);

        this.storeFile(new File(path), targetPath);
    }

    /**
     * This method removes a file from the storage.  This method will use the path parameter 
     * to localte the file and remove it from the storage.  The deletion process will delete the last
     * version of this object.
     * 
     * Root = /s3storage/
     * path = myfile.txt
     * Target = /s3storage/myfile.txt
     * 
     * Root = /s3storage/
     * path = myfolder/myfile.txt
     * Target = /s3storage/myfolder/myfile.txt 
     * 
     * @param path is the object's path within the storage.  
     * @throws UniversalIOException when a specific IO error occurs.
     */
    void removeFile(String path) throws UniversalIOException {
        PathValidator.validatePath(path);

        try {
            s3client.deleteObject(new DeleteObjectRequest(this.settings.getRoot(), path));
        } catch (Exception e) {
            throw new UniversalIOException(e.getMessage());
        }        
    }

    /**
     * This method creates a new folder within the storage using the passed path. If the new folder name already
     * exists within the storage, this  process will skip the creation step.
     * 
     * Root = /s3storage/
     * path = /myFolder
     * Target = /s3storage/myFolder
     * 
     * Root = /s3storage/
     * path = /folders/myFolder
     * Target = /s3storage/folders/myFolder
     * 
     * @param path is the folder's path.  A path must end with forward slash '/', the back slash '\' is not 
     *        considered a folder indicator.
     * @param storeFiles is a flag to store the files after folder creation.
     * 
     * @throws UniversalIOException when a specific IO error occurs.
     * @throws IllegalArgumentException is path has an invalid value.
     */
    void createFolder(String path) throws UniversalIOException {
        PathValidator.validatePath(path);

        if ("".equals(path.trim())) {
            throw new UniversalIOException("Invalid path.  The path shouldn't be empty.");
        }

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest = new PutObjectRequest(this.settings.getRoot(),
                path.endsWith("/") ? path : (path + "/"), emptyContent, metadata);

        s3client.putObject(putObjectRequest);
    }

    /**
     * This method removes the folder located on that path.
     * The folder should be empty in order for removing.
     * 
     * Root = /storage/
     * path = myFolder
     * Target = /storage/myFolder
     * 
     * Root = /storage/
     * path = folders/myFolder
     * Target = /storage/folders/myFolder
     * 
     * @param path is the folder's path.  A path must end with forward slash '/', the back slash '\' is not 
     *        considered a folder indicator.
     */
    void removeFolder(String path) throws UniversalIOException {
        PathValidator.validatePath(path);

        if ("".equals(path.trim())) {
            return;
        }

        try {
            s3client.deleteObject(new DeleteObjectRequest(this.settings.getRoot(), 
                    path.endsWith("/") ? path : (path + "/")));
        } catch (Exception e) {
            throw new UniversalIOException(e.getMessage());
        }
    }

    /**
     * This method retrieves a file from the storage.
     * The method will retrieve the file according to the passed path.  
     * A file will be stored within the settings' tmp folder.
     * 
     * @param path in context.
     * @returns a file pointing to the retrieved file.
     */
    public File retrieveFile(String path) throws UniversalIOException {
        PathValidator.validatePath(path);

        if ("".equals(path.trim())) {
            return null;
        }

        if (path.trim().endsWith("/")) {
            throw new UniversalIOException("Invalid path.  Looks like you're trying to retrieve a folder.");
        }
        
        File dest = null;
        InputStream objectData = null;
        try {
            S3Object object = s3client.getObject(new GetObjectRequest(this.settings.getRoot(), path));
            objectData = object.getObjectContent();

            String name = object.getKey();
            int index = name.lastIndexOf("/");
            if (index != -1) {
                name = name.substring(index);
            }

            dest = new File(FileUtil.completeFileSeparator(this.settings.getTmp()) + name);

            FileUtils.copyInputStreamToFile(objectData, dest);
        } catch (Exception e) {
            throw new UniversalIOException(e.getMessage());
        } finally {
            if (objectData != null) {
                try {
                    objectData.close();
                } catch(Exception ignore) {}
            }
        }

        return dest;
    }

    /**
     * This method retrieves a file from the storage as InputStream.
     * The method will retrieve the file according to the passed path.  
     * A file will be stored within the settings' tmp folder.
     * 
     * @param path in context.
     * @returns an InputStream pointing to the retrieved file.
     */
    public InputStream retrieveFileAsStream(String path) throws UniversalIOException {
        PathValidator.validatePath(path);

        if ("".equals(path.trim())) {
            return null;
        }

        if (path.trim().endsWith("/")) {
            throw new UniversalIOException("Invalid path.  Looks like you're trying to retrieve a folder.");
        }

        S3Object object = s3client.getObject(new GetObjectRequest(this.settings.getRoot(), path));
        return object.getObjectContent();
    }

    /**
     * This method cleans the context of this storage.  This method doesn't remove any file from the storage.
     * The method will clean the tmp folder to release disk usage.
     */
    public void clean() throws UniversalIOException  {
        try {
            FileUtils.cleanDirectory(new File(this.settings.getTmp()));
        } catch (Exception e) {
            throw new UniversalIOException(e.getMessage());
        }
    }
}