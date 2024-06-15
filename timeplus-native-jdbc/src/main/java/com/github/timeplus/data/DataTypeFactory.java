/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.timeplus.data;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.github.timeplus.client.NativeContext;
import com.github.timeplus.data.type.DataTypeDate;
import com.github.timeplus.data.type.DataTypeDate32;
import com.github.timeplus.data.type.DataTypeFloat32;
import com.github.timeplus.data.type.DataTypeFloat64;
import com.github.timeplus.data.type.DataTypeIPv4;
import com.github.timeplus.data.type.DataTypeIPv6;
import com.github.timeplus.data.type.DataTypeInt16;
import com.github.timeplus.data.type.DataTypeInt32;
import com.github.timeplus.data.type.DataTypeInt64;
import com.github.timeplus.data.type.DataTypeInt8;
import com.github.timeplus.data.type.DataTypeBool;
import com.github.timeplus.data.type.DataTypeUInt16;
import com.github.timeplus.data.type.DataTypeUInt32;
import com.github.timeplus.data.type.DataTypeUInt64;
import com.github.timeplus.data.type.DataTypeUInt8;
import com.github.timeplus.data.type.DataTypeUUID;
import com.github.timeplus.data.type.complex.*;
import com.github.timeplus.misc.LRUCache;
import com.github.timeplus.misc.SQLLexer;
import com.github.timeplus.misc.Validate;
import com.github.timeplus.settings.TimeplusDefines;

public class DataTypeFactory {
    private static final LRUCache<String, IDataType<?, ?>> DATA_TYPE_CACHE = new LRUCache<>(TimeplusDefines.DATA_TYPE_CACHE_SIZE);

    public static IDataType<?, ?> get(String type, NativeContext.ServerContext serverContext) throws SQLException {
        IDataType<?, ?> dataType = DATA_TYPE_CACHE.get(type);
        if (dataType != null) {
            DATA_TYPE_CACHE.put(type, dataType);
            return dataType;
        }

        SQLLexer lexer = new SQLLexer(0, type);
        dataType = get(lexer, serverContext);
        Validate.isTrue(lexer.eof());

        DATA_TYPE_CACHE.put(type, dataType);
        return dataType;
    }

    private static final Map<String, IDataType<?, ?>> dataTypes = initialDataTypes();

    public static IDataType<?, ?> get(SQLLexer lexer, NativeContext.ServerContext serverContext) throws SQLException {
        String dataTypeName = String.valueOf(lexer.bareWord());

        if (dataTypeName.equalsIgnoreCase("tuple")) {
            return DataTypeTuple.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("array")) {
            return DataTypeArray.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("enum8")) {
            return DataTypeEnum8.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("enum16")) {
            return DataTypeEnum16.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("datetime")) {
            return DataTypeDateTime.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("datetime64")) {
            return DataTypeDateTime64.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("nullable")) {
            return DataTypeNullable.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("low_cardinality")) {
            return DataTypeLowCardinality.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("fixed_string") || dataTypeName.equals("binary")) {
            return DataTypeFixedString.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("decimal")) {
            return DataTypeDecimal.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("string")) {
            return DataTypeString.CREATOR.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("nothing")) {
            return DataTypeNothing.CREATOR.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("map")) {
            return DataTypeMap.creator.createDataType(lexer, serverContext);
        } else {
            IDataType<?, ?> dataType = dataTypes.get(dataTypeName.toLowerCase(Locale.ROOT));
            Validate.isTrue(dataType != null, "Unknown data type: " + dataTypeName);
            return dataType;
        }
    }

    /**
     * Some framework like Spark JDBC will convert all types name to lower case
     */
    private static Map<String, IDataType<?, ?>> initialDataTypes() {
        Map<String, IDataType<?, ?>> creators = new HashMap<>();

        registerType(creators, new DataTypeIPv4());
        registerType(creators, new DataTypeIPv6());
        registerType(creators, new DataTypeUUID());
        registerType(creators, new DataTypeFloat32());
        registerType(creators, new DataTypeFloat64());

        registerType(creators, new DataTypeBool());
        registerType(creators, new DataTypeInt8());
        registerType(creators, new DataTypeInt16());
        registerType(creators, new DataTypeInt32());
        registerType(creators, new DataTypeInt64());

        registerType(creators, new DataTypeUInt8());
        registerType(creators, new DataTypeUInt16());
        registerType(creators, new DataTypeUInt32());
        registerType(creators, new DataTypeUInt64());

        registerType(creators, new DataTypeDate());
        registerType(creators, new DataTypeDate32());
        return creators;
    }

    private static void registerType(Map<String, IDataType<?, ?>> creators, IDataType<?, ?> type) {
        creators.put(type.name().toLowerCase(Locale.ROOT), type);
        for (String typeName : type.getAliases()) {
            creators.put(typeName.toLowerCase(Locale.ROOT), type);
        }
    }

    // TODO
    private static Map<String, DataTypeCreator<?, ?>> initComplexDataTypes() {
        return new HashMap<>();
    }

    private static void registerComplexType(
            Map<String, DataTypeCreator<?, ?>> creators, IDataType<?, ?> type, DataTypeCreator<?, ?> creator) {

        creators.put(type.name().toLowerCase(Locale.ROOT), creator);
        for (String typeName : type.getAliases()) {
            creators.put(typeName.toLowerCase(Locale.ROOT), creator);
        }
    }
}
