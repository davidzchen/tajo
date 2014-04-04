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

package org.apache.tajo.storage.avro;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.BlobDatum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.FileFragment;

import java.io.IOException;

/**
 * FileScanner for reading Avro files
 */
public class AvroScanner extends FileScanner {
  private Schema avroSchema;
  private List<Schema.Field> avroFields;
  private DataFileReader<GenericRecord> dataFileReader;
  private int[] projectionMap;

  /**
   * Creates a new AvroScanner.
   *
   * @param conf
   * @param schema
   * @param meta
   * @param fragment
   */
  public AvroScanner(Configuration conf,
                     final org.apache.tajo.catalog.Schema schema,
                     final TableMeta meta, final FileFragment fragment) {
    super(conf, schema, meta, fragment);
  }

  /**
   * Initializes the AvroScanner.
   */
  @Override
  public void init() throws IOException {
    if (targets == null) {
      targets = schema.toArray();
    }
    prepareProjection(targets);

    avroSchema = AvroUtil.getAvroSchema(meta, conf);
    avroFields = avroSchema.getFields();

    DatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>(avroSchema);
    SeekableInput input = new FsInput(fragment.getPath(), conf);
    dataFileReader = new DataFileReader<GenericRecord>(input, datumReader);
    super.init();
  }

  private void prepareProjection(Column[] targets) {
    projectionMap = new int[targets.length];
    for (int i = 0; i < targets.length; ++i) {
      projectionMap[i] = schema.getColumnId(targets[i].getQualifiedName());
    }
  }

  private static String fromAvroString(Object value) {
    if (value instanceof Utf8) {
      Utf8 utf8 = (Utf8)value;
      return utf8.toString();
    }
    return value.toString();
  }

  private static Schema getNonNull(Schema schema) {
    if (!schema.getType().equals(Schema.Type.UNION)) {
      return schema;
    }
    List<Schema> schemas = schema.getTypes();
    if (schemas.size() != 2) {
      return schema;
    }
    if (schemas.get(0).getType().equals(Schema.Type.NULL)) {
      return schemas.get(1);
    } else if (schemas.get(1).getType().equals(Schema.Type.NULL)) {
      return schemas.get(0);
    } else {
      return schema;
    }
  }

  /**
   * Reads the next Tuple from the Avro file.
   *
   * @return The next Tuple from the Avro file or null if end of file is
   *         reached.
   */
  @Override
  public Tuple next() throws IOException {
    if (!dataFileReader.hasNext()) {
      return null;
    }

    Tuple tuple = new VTuple(schema.size());
    GenericRecord record = dataFileReader.next();
    for (int i = 0; i < projectionMap.length; ++i) {
      int columnIndex = projectionMap[i];
      Schema.Field avroField = avroFields.get(columnIndex);
      Schema nonNullAvroSchema = getNonNull(avroField.schema());
      Schema.Type avroType = nonNullAvroSchema.getType();
      Object value = record.get(columnIndex);
      switch (avroType) {
        case NULL:
          tuple.put(columnIndex, NullDatum.get());
          break;
        case BOOLEAN:
          tuple.put(columnIndex, DatumFactory.createBool((Boolean)value));
          break;
        case INT:
          tuple.put(columnIndex, DatumFactory.createInt4((Integer)value));
          break;
        case LONG:
          tuple.put(columnIndex, DatumFactory.createInt8((Long)value));
          break;
        case FLOAT:
          tuple.put(columnIndex, DatumFactory.createFloat4((Float)value));
          break;
        case DOUBLE:
          tuple.put(columnIndex, DatumFactory.createFloat8((Double)value));
          break;
        case BYTES:
          tuple.put(columnIndex, new BlobDatum((ByteBuffer)value));
          break;
        case STRING:
          tuple.put(columnIndex, DatumFactory.createText(fromAvroString(value)));
          break;
        case RECORD:
          throw new RuntimeException("Avro RECORD not supported.");
        case ENUM:
          throw new RuntimeException("Avro ENUM not supported.");
        case MAP:
          throw new RuntimeException("Avro MAP not supported.");
        case UNION:
          throw new RuntimeException("Avro UNION not supported.");
        case FIXED:
          tuple.put(columnIndex, new BlobDatum(((GenericFixed)value).bytes()));
          break;
        default:
          throw new RuntimeException("Unknown type.");
      }
    }
    return tuple;
  }

  /**
   * Resets the scanner
   */
  @Override
  public void reset() throws IOException {
  }

  /**
   * Closes the scanner.
   */
  @Override
  public void close() throws IOException {
    dataFileReader.close();
  }

  /**
   * Returns whether this scanner is projectable.
   *
   * @return true
   */
  @Override
  public boolean isProjectable() {
    return true;
  }

  /**
   * Returns whether this scanner is selectable.
   *
   * @return false
   */
  @Override
  public boolean isSelectable() {
    return false;
  }

  /**
   * Returns whether this scanner is splittable.
   *
   * @return false
   */
  @Override
  public boolean isSplittable() {
    return false;
  }
}
