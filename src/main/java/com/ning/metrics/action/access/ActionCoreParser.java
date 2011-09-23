/*
 * Copyright 2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.action.access;

import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Action Core Parser -- hides the details of the encoding of the json
 */
public class ActionCoreParser
{
    private static final Logger log = LoggerFactory.getLogger(ActionCoreParser.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private final ActionCoreParserFormat format;
    private final List<String> allEventFields;

    public enum ActionCoreParserFormat
    {
        ACTION_CORE_FORMAT_MR("MR"),
        ACTION_CORE_FORMAT_DEFAULT("DEFAULT");
        private final String parserTypeValue;

        ActionCoreParserFormat(final String type)
        {
            parserTypeValue = type;
        }

        public static ActionCoreParserFormat getFromString(final String in)
        {
            for (final ActionCoreParserFormat cur : ActionCoreParserFormat.values()) {
                if (cur.parserTypeValue.equals(in)) {
                    return cur;
                }
            }
            return null;
        }
    }


    public ActionCoreParser(final ActionCoreParserFormat format, final List<String> allEventFields, final String delimiter)
    {
        this.format = format;
        this.allEventFields = allEventFields;
    }

    public ImmutableList<Map<String, Object>> parse(final String json) throws Exception
    {
        switch (format) {
            case ACTION_CORE_FORMAT_DEFAULT:
                return parseDefault(json);
            case ACTION_CORE_FORMAT_MR:
                return parseMR(json);
            default:
                throw new RuntimeException("Format " + format + " not supported");
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private ImmutableList<Map<String, Object>> parseMR(final String json) throws Exception
    {
        final ImmutableList.Builder<Map<String, Object>> builder = new ImmutableList.Builder<Map<String, Object>>();
        final Map eventTop = mapper.readValue(json, Map.class);

        final Iterable<Map> entriesDirectory = (List<Map>) eventTop.get("entries");
        for (final Map entryDirectory : entriesDirectory) {
            List<Map> entries = null;
            Object entriesRow = null;
            try {
                entriesRow = entryDirectory.get("content");
                if (entriesRow == null) {
                    continue;
                }
                if (entriesRow instanceof String && entriesRow.equals("")) {
                    continue;
                }
                entries = (List<Map>) entriesRow;
            }
            catch (Exception e) {
                log.error("Failed to deserialize the event {}", entriesRow);
            }

            if (entries != null) {
                for (final Map<String, Object> event : entries) {
                    final Map<String, Object> simplifiedEvent = extractEventTabSep((String) event.get("record"));
                    builder.add(simplifiedEvent);
                }
            }
        }
        return builder.build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private ImmutableList<Map<String, Object>> parseDefault(final String json) throws Exception
    {
        final ImmutableList.Builder<Map<String, Object>> builder = new ImmutableList.Builder<Map<String, Object>>();
        final Map eventTop = mapper.readValue(json, Map.class);

        final Iterable<Map> entriesDirectory = (List<Map>) eventTop.get("entries");
        for (final Map entryDirectory : entriesDirectory) {
            final Object contentRow = entryDirectory.get("content");
            if (contentRow instanceof String && contentRow.equals("")) {
                continue;
            }

            final Iterable<Map> entryContent = (Iterable<Map>) contentRow;
            for (final Map<String, Object> event : entryContent) {
                final Map<String, Object> simplifiedEvent = extractEvent(event);
                builder.add(simplifiedEvent);
            }
        }

        return builder.build();
    }

    private Map<String, Object> extractEvent(final Map<String, Object> eventFull)
    {
        final Map<String, Object> result = new HashMap<String, Object>();
        for (final String key : allEventFields) {
            final Object value = eventFull.get(key);
            if (value == null) {
                log.warn("Event {} is missing key {}", eventFull, key);
                continue;
            }
            result.put(key, value);
        }

        return result;
    }

    private Map<String, Object> extractEventTabSep(final String event)
    {
        final Map<String, Object> result = new HashMap<String, Object>();
        if (event == null) {
            return result;
        }

        final String[] parts = event.split("\\t");
        if (parts == null || parts.length != allEventFields.size()) {
            log.warn("Unexpected event content size = {}", parts == null ? 0 : parts.length);
            return result;
        }

        int i = 0;
        for (final String key : allEventFields) {
            result.put(key, parts[i]);
            i++;
        }

        return result;
    }
}
