package com.universal.storage;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.AWSCredentials;
import com.universal.storage.settings.UniversalSettings;

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
 * This class represents a provider of access keys from the settings in context.
 */
public class UniversalProfileCredentialsProvider extends ProfileCredentialsProvider {
    private UniversalSettings settings;
    private static AWSCredentials credentials = null;

    /**
     * This constructor creates a new instance.
     * 
     * @param settings in context.
     */
    public UniversalProfileCredentialsProvider(UniversalSettings settings) {
        this.settings = settings;
    }

    /**
     * Returns AWSCredentials which the caller can use to authorize an AWS request. Each implementation of
     * AWSCredentialsProvider can chose its own strategy for loading credentials. For example, an implementation 
     * might load credentials from an existing key management system, or load new credentials when credentials are 
     * rotated.
     * 
     * This method is overridden to get the access key from the settings in context.
     * 
     * Returns a singleton instance.
     */
    public AWSCredentials getCredentials() {
        if (credentials == null) {
            credentials = new AWSCredentials() {
                public String getAWSAccessKeyId() {
                    return UniversalProfileCredentialsProvider.this.settings.getAWSAccessKeyId();
                }

                public String getAWSSecretKey() {
                    return UniversalProfileCredentialsProvider.this.settings.getAWSSecretKey();
                }
            };
        }

        return credentials;
    }
}