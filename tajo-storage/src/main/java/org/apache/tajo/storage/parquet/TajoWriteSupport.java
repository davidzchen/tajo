/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.parquet;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.datum.Datum;

/**
 * Tajo implementation of {@link WriteSupport} for {@link Tuple}s.
 * Users should use {@link TajoParquetWriter} or {@link ParquetFileAppender}
 * and not this class directly.
 */
public class TajoWriteSupport extends WriteSupport<Tuple> {
  private static final String TAJO_SCHEMA = "parquet.tajo.schema";

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private Schema rootTajoSchema;

  public TajoWriteSupport(Schema tajoSchema) {
    this.rootSchema = new TajoSchemaConverter().convert(tajoSchema);
    this.rootTajoSchema = tajoSchema;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(TajoReadSupport.TAJO_SCHEMA_METADATA_KEY,
                      rootTajoSchema.toJson());
    return new WriteContext(rootSchema, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(Tuple tuple) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, rootTajoSchema, tuple);
    recordConsumer.endMessage();
  }

  private void writeRecordFields(GroupType schema, Schema tajoSchema,
                                 Tuple tuple) {
    List<Type> fields = schema.getFields();
    for (int i = 0; i < tajoSchema.size(); ++i) {
      Column column = tajoSchema.getColumn(i);
      Datum datum = tuple.get(i);
      Type fieldType = fields.get(i);
      if (!tuple.isNull(i)) {
        recordConsumer.startField(fieldType.getName(), i);
        writeValue(fieldType, column, datum);
        recordConsumer.endField(fieldType.getName(), i);
      } else if (fieldType.isRepetition(Type.Repetition.REQUIRED)) {
        throw new RuntimeException("Null-value for required field: " +
            column.getSimpleName());
      }
    }
  }

  private void writeValue(Type fieldType, Column column, Datum datum) {
    switch (column.getDataType().getType()) {
      case NULL_TYPE:
        break;
      case BOOLEAN:
        recordConsumer.addBoolean((Boolean) datum.asBool());
        break;
      case BIT:
      case INT2:
      case INT4:
        recordConsumer.addInteger(datum.asInt4());
        break;
      case INT8:
        recordConsumer.addLong(datum.asInt8());
        break;
      case FLOAT4:
        recordConsumer.addFloat(datum.asFloat4());
        break;
      case FLOAT8:
        recordConsumer.addDouble(datum.asFloat8());
        break;
      case CHAR:
      case TEXT:
        recordConsumer.addBinary(Binary.fromString(datum.asChars()));
        break;
      case PROTOBUF:
        throw new RuntimeException("Writing PROTOBUF not supported.");
      case BLOB:
        recordConsumer.addBinary(Binary.fromByteArray(datum.asByteArray()));
        break;
      case INET4:
      case INET6:
        recordConsumer.addBinary(Binary.fromByteArray(datum.asByteArray()));
        break;
      default:
        break;
    }
  }
}
