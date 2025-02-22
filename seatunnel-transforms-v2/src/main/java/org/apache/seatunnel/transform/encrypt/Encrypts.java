/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.shade.com.google.common.base.Charsets;
import org.apache.seatunnel.shade.com.google.common.hash.HashFunction;
import org.apache.seatunnel.shade.com.google.common.hash.Hashing;

import org.apache.commons.lang3.StringUtils;

public enum Encrypts {
    MD5 {
        @Override
        public String encrypt(String value) {
            return Hashing.md5().newHasher().putString(value, Charsets.UTF_8).hash().toString();
        }
    },

    MURMUR3_32 {
        @Override
        public String encrypt(String value, String seed) {
            HashFunction hashFunction;
            if (StringUtils.isEmpty(seed)) {
                hashFunction = Hashing.murmur3_32();
            } else {
                hashFunction = Hashing.murmur3_32(seed);
            }
            return hashFunction.newHasher().putString(value, Charsets.UTF_8).hash().toString();
        }
    },

    SHA_256 {
        @Override
        public String encrypt(String value) {
            return Hashing.sha256().newHasher().putString(value, Charsets.UTF_8).hash().toString();
        }
    };

    public String encrypt(String value) {
        throw new UnsupportedOperationException();
    }

    public String encrypt(String value, String seed) {
        throw new UnsupportedOperationException();
    }
}
