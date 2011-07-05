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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Action Core Parser -- hides the details of the encoding of the json
 */
public class ActionCoreParser
{
    private static final Logger logger = Logger.getLogger(ActionCoreParser.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final ActionCoreParserFormat format;
    private final List<String> allEventFields;
    private final String eventName;
    private final String delimiter;

    public enum ActionCoreParserFormat
    {
        ACTION_CORE_FORMAT_MR("MR"),
        ACTION_CORE_FORMAT_DEFAULT("DEFAULT");
        private String parserTypeValue;

        ActionCoreParserFormat(String type)
        {
            parserTypeValue = type;
        }

        public static ActionCoreParserFormat getFromString(String in)
        {
            for (ActionCoreParserFormat cur : ActionCoreParserFormat.values()) {
                if (cur.parserTypeValue.equals(in)) {
                    return cur;
                }
            }
            return null;
        }
    }


    public ActionCoreParser(ActionCoreParserFormat format, String eventName, List<String> allEventFields, String delimiter)
    {
        this.format = format;
        this.allEventFields = allEventFields;
        this.eventName = eventName;
        this.delimiter = delimiter;
    }


    public ImmutableList<Map<String, Object>> parse(String json) throws Exception
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
    private ImmutableList<Map<String, Object>> parseMR(String json) throws Exception
    {
        ImmutableList.Builder<Map<String, Object>> builder = new ImmutableList.Builder<Map<String, Object>>();
        Map eventTop = mapper.readValue(json, Map.class);
        List<Map> entriesDirectory = (List<Map>) eventTop.get("entries");
        for (Map entryDirectory : entriesDirectory) {
            List<Map> entries = null;
            Object entriesRow = null;
            try {
                entriesRow = entryDirectory.get("content");
                if (entriesRow == null) {
                    continue;
                }
                if (entriesRow instanceof String && ((String) entriesRow).equals("")) {
                    continue;
                }
                entries = (List<Map>) entriesRow;
            }
            catch (Exception e) {
                logger.error("Failed to deserialize the event " + entriesRow.toString());
            }
            for (Map<String, Object> event : entries) {
                Map<String, Object> simplifiedEvent = extractEventTabSep((String) event.get("record"));
                builder.add(simplifiedEvent);
            }
        }
        return builder.build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private ImmutableList<Map<String, Object>> parseDefault(String json) throws Exception
    {
        ImmutableList.Builder<Map<String, Object>> builder = new ImmutableList.Builder<Map<String, Object>>();
        Map eventTop = mapper.readValue(json, Map.class);
        List<Map> entriesDirectory = (List<Map>) eventTop.get("entries");
        for (Map entryDirectory : entriesDirectory) {
            Object contentRow = entryDirectory.get("content");
            if (contentRow instanceof String && ((String) contentRow).equals("")) {
                continue;
            }
            ArrayList<Map> entryContent = (ArrayList<Map>) contentRow;
            for (Map<String, Object> event : entryContent) {
                Map<String, Object> simplifiedEvent = extractEvent(event);
                builder.add(simplifiedEvent);
            }

        }
        return builder.build();
    }

    private Map<String, Object> extractEvent(Map<String, Object> eventFull)
    {
        Map<String, Object> result = new HashMap<String, Object>();
        for (String key : allEventFields) {
            Object value = eventFull.get(key);
            if (value == null) {
                logger.warn("Event " + eventName + " is missing key " + key);
                continue;
            }
            result.put(key, value);
        }
        return result;
    }

    private Map<String, Object> extractEventTabSep(String event)
    {
        Map<String, Object> result = new HashMap<String, Object>();
        if (event == null) {
            return result;
        }
        String[] parts = event.split("\\t");
        if (parts == null || parts.length != allEventFields.size()) {
            logger.warn("Unexpected event content size = " + ((parts == null) ? 0 : parts.length));
            return result;
        }
        int i = 0;
        for (String key : allEventFields) {
            result.put(key, parts[i]);
            i++;
        }
        return result;
    }
}