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

import com.google.protobuf.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.nio.ByteBuffer;

import parquet.io.api.GroupConverter;
import parquet.io.api.Converter;
import parquet.io.api.PrimitiveConverter;
import parquet.io.api.Binary;
import parquet.schema.Type;
import parquet.schema.GroupType;

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.BlobDatum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;

public class TajoRecordConverter extends GroupConverter {
  private final GroupType parquetSchema;
  private final Schema tajoReadSchema;
  private final int[] projectionMap;
  private final int tupleSize;
  private final int projectionSize;

  private final Converter[] converters;

  private Tuple currentTuple;

  public TajoRecordConverter(GroupType parquetSchema, Schema tajoReadSchema,
                             int[] projectionMap) {
    this.parquetSchema = parquetSchema;
    this.tajoReadSchema = tajoReadSchema;
    this.projectionMap = projectionMap;
    this.tupleSize = tajoReadSchema.size();
    this.projectionSize = parquetSchema.getFieldCount();
    if (projectionMap.length != projectionSize) {
      throw new IllegalArgumentException("Projection sizes do not match: " +
          projectionSize + ", " + projectionMap.length);
    }

    this.converters = new Converter[projectionSize];
    for (int i = 0; i < projectionSize; ++i) {
      final int projectionIndex = projectionMap[i];
      Column column = tajoReadSchema.getColumn(projectionIndex);
      Type type = parquetSchema.getType(i);
      converters[i] = newConverter(column, type, new ParentValueContainer() {
        @Override
        void add(Object value) {
          TajoRecordConverter.this.set(projectionIndex, value);
        }
      });
    }
  }

  private void set(int index, Object value) {
    currentTuple.put(index, (Datum) value);
  }

  private Converter newConverter(Column column, Type type,
                                 ParentValueContainer parent) {
    DataType dataType = column.getDataType();
    switch (dataType.getType()) {
      case BOOLEAN:
        return new FieldBooleanConverter(parent);
      case BIT:
        return new FieldBitConverter(parent);
      case CHAR:
        return new FieldCharConverter(parent);
      case INT2:
        return new FieldInt2Converter(parent);
      case INT4:
        return new FieldInt4Converter(parent);
      case INT8:
        return new FieldInt8Converter(parent);
      case FLOAT4:
        return new FieldFloat4Converter(parent);
      case FLOAT8:
        return new FieldFloat8Converter(parent);
      case INET4:
        return new FieldInet4Converter(parent);
      case INET6:
        throw new RuntimeException("No converter for INET6");
      case TEXT:
        return new FieldTextConverter(parent);
      case PROTOBUF:
        return new FieldProtobufConverter(parent, dataType);
      case BLOB:
        return new FieldBlobConverter(parent);
      case NULL_TYPE:
        throw new RuntimeException("No converter for NULL_TYPE.");
      default:
        throw new RuntimeException("Unsupported data type");
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    currentTuple = new VTuple(tupleSize);
  }

  @Override
  public void end() {
  }

  public Tuple getCurrentRecord() {
    return currentTuple;
  }

  static abstract class ParentValueContainer {
    /**
     * Adds the value to the parent.
     *
     * @param value The value to add.
     */
    abstract void add(Object value);
  }

  static final class FieldBooleanConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldBooleanConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.add(DatumFactory.createBool(value));
    }
  }

  static final class FieldBitConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldBitConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createBit((byte)(value & 0xff)));
    }
  }

  static final class FieldCharConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldCharConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(DatumFactory.createChar(value.toStringUsingUTF8()));
    }
  }

  static final class FieldInt2Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldInt2Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createInt2((short)value));
    }
  }

  static final class FieldInt4Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldInt4Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createInt4(value));
    }
  }

  static final class FieldInt8Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldInt8Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addLong(long value) {
      parent.add(DatumFactory.createInt8(value));
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createInt8(Long.valueOf(value)));
    }
  }

  static final class FieldFloat4Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldFloat4Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createFloat4(Float.valueOf(value)));
    }

    @Override
    final public void addLong(long value) {
      parent.add(DatumFactory.createFloat4(Float.valueOf(value)));
    }

    @Override
    final public void addFloat(float value) {
      parent.add(DatumFactory.createFloat4(value));
    }
  }

  static final class FieldFloat8Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldFloat8Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createFloat8(Double.valueOf(value)));
    }

    @Override
    final public void addLong(long value) {
      parent.add(DatumFactory.createFloat8(Double.valueOf(value)));
    }

    @Override
    final public void addFloat(float value) {
      parent.add(DatumFactory.createFloat8(Double.valueOf(value)));
    }

    @Override
    final public void addDouble(double value) {
      parent.add(DatumFactory.createFloat8(value));
    }
  }

  static final class FieldInet4Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldInet4Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(DatumFactory.createInet4(value.getBytes()));
    }
  }

  static final class FieldTextConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldTextConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(DatumFactory.createText(value.toStringUsingUTF8()));
    }
  }

  static final class FieldBlobConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldBlobConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(new BlobDatum(ByteBuffer.wrap(value.getBytes())));
    }
  }

  static final class FieldProtobufConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;
    private final DataType dataType;

    public FieldProtobufConverter(ParentValueContainer parent,
                                  DataType dataType) {
      this.parent = parent;
      this.dataType = dataType;
    }

    @Override
    final public void addBinary(Binary value) {
      try {
        ProtobufDatumFactory factory =
            ProtobufDatumFactory.get(dataType.getCode());
        Message.Builder builder = factory.newBuilder();
        builder.mergeFrom(value.getBytes());
        parent.add(factory.createDatum(builder));
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
