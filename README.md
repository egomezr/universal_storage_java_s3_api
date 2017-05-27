# Universal Storage Java API
## AWS S3 provider

[![Build Status](https://travis-ci.org/dynamicloud/universal_storage_java_s3_api.svg?branch=master)](https://travis-ci.org/dynamicloud/universal_storage_java_s3_api)
![Version](https://img.shields.io/badge/api-v1.0.0-brightgreen.svg)

Universal storage provides you an interface for storing files according to your needs. With this Universal Storage Java API, you will be able to develop programs in Java and use an interface for storing your files within a s3 bucket as storage.

<hr>

**This documentation has the following content:**

1. [Maven project](maven-project)
2. [Test API](#test-api)
3. [Settings](#settings)
4. [How to use](#how-to-use)

# Maven project
This API follows the Maven structure to ease its installation within your project.

# Test API
If you want to test the API, follow these steps:

1. Open with a text editor the settings.json located on test/resources/settings.json
```json
{
	"provider": "aws.s3",
	"root": "bucket-name-in-s3",
	"tmp": "/home/tmp",
	"aws_s3": {
		"access_key": "...",
		"secret_key": "...",
		"storage_class": "STANDARD",
		"s3_region": "us-east-1",
		"encryption": false,
		"tags": [
			{
				"key": "application",
				"value": "Test cases"
			}
		]
	}
}
```
2. The root and tmp keys are the main data to be filled.  Create a local folder called **tmp** and paste its path on the key **tmp**.
3. Create a folder i.e: **universalstorage** in your root app folder, copy the name and then paste it on root attribute.
4. Save the settings.json file.

**Now execute the following command:**

`mvn clean test` 

# Settings
**These are the steps for setting up Universal Storage in your project:**
1. You must create a file called settings.json (can be any name) and paste the following. 
```json
{
	"provider": "aws.s3",
	"root": "bucket-name-in-s3",
	"tmp": "/home/tmp",
	"aws_s3": {
		"access_key": "...",
		"secret_key": "...",
		"storage_class": "STANDARD",
		"s3_region": "us-east-1",
		"encryption": false,
		"tags": [
			{
				"key": "application",
				"value": "Test cases"
			}
		]
	}
}
```
2. The root and tmp keys are the main data to be filled.  Create a local folder called **tmp** and paste its path on the key **tmp**.
3. Create a folder i.e: **universalstorage** in your root app folder, copy the name and then paste it on root attribute.
4. Save the file settings.json
5. Add the maven dependency in your pom.xml file.

```xml
<dependency>
   <groupId>org.dynamicloud.api</groupId>
   <artifactId>universalstorage.awss3</artifactId>
   <version>1.0.0</version>
</dependency>
```

The root folder is the storage where the files will be stored.

The tmp folder is where temporary files will be stored.
  
# How to use
**Examples for Storing files:**

1. Passing the settings programmatically
```java
try {
      UniversalStorage us = UniversalStorage.Impl.
          getInstance(new UniversalSettings(new File("/home/test/resources/settings.json")));
      us.storeFile(new File("/home/test/resources/settings.json"), "myfolder/innerfolder");
      us.storeFile(new File("/home/test/resources/settings.json"));
      us.storeFile(new File("/home/test/resources/settings.json").getAbsolutePath(), "myfolder/innerfolder");
      us.storeFile(new File("/home/test/resources/settings.json").getAbsolutePath());
} catch (UniversalStorageException e) {
    fail(e.getMessage());
}
```
2. The settings could be passed through either jvm parameter or environment variable.
3. If you want to pass the settings.json path through jvm parameter, in your java command add the following parameter:
     `-Duniversal.storage.settings=/home/test/resources/settings.json`
4. If your want to pass the settings.json path through environment variable, add the following variable:
     `universal_storage_settings=/home/test/resources/settings.json`

```java
try {
      UniversalStorage us = UniversalStorage.Impl.getInstance();
      us.storeFile(new File("/home/test/resources/settings.json"), "myfolder/innerfolder");
      us.storeFile(new File("/home/test/resources/settings.json"));
      us.storeFile(new File("/home/test/resources/settings.json").getAbsolutePath(), "myfolder/innerfolder");
      us.storeFile(new File("/home/test/resources/settings.json").getAbsolutePath());
} catch (UniversalStorageException e) {
    fail(e.getMessage());
}
```

**Remove file:**
```java
try {
      UniversalStorage us = UniversalStorage.Impl.getInstance();
      us.removeFile("/home/test/resources/settings.json");
} catch (UniversalStorageException e) {
    e.printStackTrace();
}

```

**Create folder:**

```java
try {
      UniversalStorage us = UniversalStorage.Impl.getInstance();
      us.createFolder("/myNewFolder");
} catch (UniversalStorageException e) {
    e.printStackTrace();
}

```

**Remove folder:**
```java
try {
      UniversalStorage us = UniversalStorage.Impl.getInstance();
      us.removeFolder("/myNewFolder");
} catch (UniversalStorageException e) {
    e.printStackTrace();
}
```

**Retrieve file:**

This file will be stored into the tmp folder.
```java
try {
      UniversalStorage us = UniversalStorage.Impl.getInstance();
      File file = us.retrieveFile("myFolder/file.txt");
} catch (UniversalStorageException e) {
    e.printStackTrace();
}
```

**Retrieve file as InputStream:**

This inputstream will use a file that was stored into the tmp folder.
```java
try {
      UniversalStorage us = UniversalStorage.Impl.getInstance();
      InputSstream stream = us.retrieveFileAsStream("myFolder/file.txt");
} catch (UniversalStorageException e) {
    e.printStackTrace();
}
```

**Clean up tmp folder:**
```java
try {
      UniversalStorage us = UniversalStorage.Impl.getInstance();
      us.clean();
} catch (UniversalStorageException e) {
    e.printStackTrace();
}
```

