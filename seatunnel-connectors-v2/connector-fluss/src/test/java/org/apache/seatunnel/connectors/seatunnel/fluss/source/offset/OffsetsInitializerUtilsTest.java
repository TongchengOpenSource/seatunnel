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

package org.apache.seatunnel.connectors.seatunnel.fluss.source.offset;

import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartupMode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link OffsetsInitializerUtils}.
 */
class OffsetsInitializerUtilsTest {

    @Test
    void testParseNumericTimestamp() {
        long expected = 1678883047356L;
        assertEquals(expected, OffsetsInitializerUtils.parseTimestamp("1678883047356"));
        assertEquals(expected, OffsetsInitializerUtils.parseTimestamp(" 1678883047356 "));
    }

    @Test
    void testParseFormattedTimestamp() {
        // Note: This test might be sensitive to timezone, but for testing purposes we assume system timezone
        long result = OffsetsInitializerUtils.parseTimestamp("2023-03-15 10:30:47");
        assertTrue(result > 0);
        
        // Test with leading/trailing spaces
        long result2 = OffsetsInitializerUtils.parseTimestamp(" 2023-03-15 10:30:47 ");
        assertEquals(result, result2);
    }

    @Test
    void testParseTimestampInvalidFormats() {
        // Null and empty
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializerUtils.parseTimestamp(null));
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializerUtils.parseTimestamp(""));
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializerUtils.parseTimestamp("   "));
        
        // Invalid numeric
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializerUtils.parseTimestamp("0"));
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializerUtils.parseTimestamp("-1"));
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializerUtils.parseTimestamp("abc"));
        
        // Invalid date format
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializerUtils.parseTimestamp("2023-13-45 25:70:80"));
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializerUtils.parseTimestamp("2023/03/15 10:30:47"));
        assertThrows(IllegalArgumentException.class, () -> OffsetsInitializerUtils.parseTimestamp("15-03-2023 10:30:47"));
    }

    @Test
    void testFormatTimestamp() {
        long timestamp = 1678883047356L; // 2023-03-15 10:30:47.356 (approximately, depends on timezone)
        String formatted = OffsetsInitializerUtils.formatTimestamp(timestamp);
        assertNotNull(formatted);
        assertTrue(formatted.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));
    }

    @Test
    void testValidateTimestampForMode() {
        // TIMESTAMP mode - valid
        assertDoesNotThrow(() -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.TIMESTAMP, 1678883047356L));
        
        // TIMESTAMP mode - invalid
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.TIMESTAMP, null));
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.TIMESTAMP, 0L));
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.TIMESTAMP, -1L));
        
        // Other modes - should not throw even with timestamp
        assertDoesNotThrow(() -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.EARLIEST, null));
        assertDoesNotThrow(() -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.EARLIEST, 1678883047356L));
        assertDoesNotThrow(() -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.LATEST, null));
        assertDoesNotThrow(() -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.LATEST, 1678883047356L));
        assertDoesNotThrow(() -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.FULL, null));
        assertDoesNotThrow(() -> OffsetsInitializerUtils.validateTimestampForMode(StartupMode.FULL, 1678883047356L));
    }

    @Test
    void testCreateFromModeAndTimestamp() {
        // EARLIEST mode
        OffsetsInitializer earliest = OffsetsInitializerUtils.createFromModeAndTimestamp(StartupMode.EARLIEST, null);
        assertEquals(StartupMode.EARLIEST, earliest.getStartupMode());
        
        // LATEST mode
        OffsetsInitializer latest = OffsetsInitializerUtils.createFromModeAndTimestamp(StartupMode.LATEST, null);
        assertEquals(StartupMode.LATEST, latest.getStartupMode());
        
        // FULL mode
        OffsetsInitializer full = OffsetsInitializerUtils.createFromModeAndTimestamp(StartupMode.FULL, null);
        assertEquals(StartupMode.FULL, full.getStartupMode());
        
        // TIMESTAMP mode with numeric timestamp
        OffsetsInitializer timestamp1 = OffsetsInitializerUtils.createFromModeAndTimestamp(
                StartupMode.TIMESTAMP, "1678883047356");
        assertEquals(StartupMode.TIMESTAMP, timestamp1.getStartupMode());
        assertEquals(1678883047356L, timestamp1.getTimestamp());
        
        // TIMESTAMP mode with formatted timestamp
        OffsetsInitializer timestamp2 = OffsetsInitializerUtils.createFromModeAndTimestamp(
                StartupMode.TIMESTAMP, "2023-03-15 10:30:47");
        assertEquals(StartupMode.TIMESTAMP, timestamp2.getStartupMode());
        assertNotNull(timestamp2.getTimestamp());
        assertTrue(timestamp2.getTimestamp() > 0);
    }

    @Test
    void testCreateFromModeAndTimestampInvalid() {
        // TIMESTAMP mode without timestamp
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializerUtils.createFromModeAndTimestamp(StartupMode.TIMESTAMP, null));
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializerUtils.createFromModeAndTimestamp(StartupMode.TIMESTAMP, ""));
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializerUtils.createFromModeAndTimestamp(StartupMode.TIMESTAMP, "   "));
        
        // TIMESTAMP mode with invalid timestamp
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializerUtils.createFromModeAndTimestamp(StartupMode.TIMESTAMP, "0"));
        assertThrows(IllegalArgumentException.class, 
                () -> OffsetsInitializerUtils.createFromModeAndTimestamp(StartupMode.TIMESTAMP, "invalid"));
    }

    @Test
    void testDescribe() {
        // Test null
        assertEquals("null", OffsetsInitializerUtils.describe(null));
        
        // Test EARLIEST
        OffsetsInitializer earliest = OffsetsInitializer.earliest();
        assertEquals("earliest offsets", OffsetsInitializerUtils.describe(earliest));
        
        // Test LATEST
        OffsetsInitializer latest = OffsetsInitializer.latest();
        assertEquals("latest offsets", OffsetsInitializerUtils.describe(latest));
        
        // Test FULL
        OffsetsInitializer full = OffsetsInitializer.full();
        assertEquals("full snapshot then changelog", OffsetsInitializerUtils.describe(full));
        
        // Test TIMESTAMP
        long timestamp = 1678883047356L;
        OffsetsInitializer timestampInit = OffsetsInitializer.timestamp(timestamp);
        String description = OffsetsInitializerUtils.describe(timestampInit);
        assertTrue(description.startsWith("timestamp " + timestamp));
        assertTrue(description.contains("("));
        assertTrue(description.contains(")"));
    }
}
