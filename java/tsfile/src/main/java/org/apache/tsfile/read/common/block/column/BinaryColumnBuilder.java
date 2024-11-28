/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.read.common.block.column;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.block.column.ColumnBuilderStatus;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Arrays;

import static java.lang.Math.max;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.calculateBlockResetSize;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOf;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOf;

public class BinaryColumnBuilder implements ColumnBuilder {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(BinaryColumnBuilder.class);

  private final ColumnBuilderStatus columnBuilderStatus;
  private final TSDataType dataType;
  public static final BinaryColumn NULL_STRING_BLOCK =
      new BinaryColumn(0, 1, new boolean[] {true}, new Binary[1], TSDataType.STRING);
  public static final BinaryColumn NULL_TEXT_BLOCK =
      new BinaryColumn(0, 1, new boolean[] {true}, new Binary[1], TSDataType.TEXT);
  public static final BinaryColumn NULL_BLOB_BLOCK =
      new BinaryColumn(0, 1, new boolean[] {true}, new Binary[1], TSDataType.BLOB);

  private boolean initialized;
  private final int initialEntryCount;

  private int positionCount;
  private boolean hasNullValue;
  private boolean hasNonNullValue;

  // it is assumed that these arrays are the same length
  private boolean[] valueIsNull = new boolean[0];
  private Binary[] values = new Binary[0];

  private long arraysRetainedSizeInBytes;

  public BinaryColumnBuilder(
      ColumnBuilderStatus columnBuilderStatus, int expectedEntries, TSDataType dataType) {
    this.initialEntryCount = max(expectedEntries, 1);
    this.columnBuilderStatus = columnBuilderStatus;
    this.dataType = dataType;
    updateArraysDataSize();
  }

  @Override
  public ColumnBuilder writeBinary(Binary value) {
    if (values.length <= positionCount) {
      growCapacity();
    }

    values[positionCount] = value;

    hasNonNullValue = true;
    positionCount++;
    return this;
  }

  /** Write an Object to the current entry, which should be the Binary type; */
  @Override
  public ColumnBuilder writeObject(Object value) {
    if (value instanceof Binary) {
      writeBinary((Binary) value);
      return this;
    }
    throw new UnSupportedDataTypeException("BinaryColumn only support Binary data type");
  }

  @Override
  public ColumnBuilder write(Column column, int index) {
    return writeBinary(column.getBinary(index));
  }

  @Override
  public ColumnBuilder writeTsPrimitiveType(TsPrimitiveType value) {
    return writeBinary(value.getBinary());
  }

  @Override
  public ColumnBuilder appendNull() {
    if (values.length <= positionCount) {
      growCapacity();
    }

    valueIsNull[positionCount] = true;

    hasNullValue = true;
    positionCount++;
    return this;
  }

  @Override
  public Column build() {
    if (!hasNonNullValue) {
      if (dataType == TSDataType.STRING) {
        return new RunLengthEncodedColumn(NULL_STRING_BLOCK, positionCount);
      } else if (dataType == TSDataType.TEXT) {
        return new RunLengthEncodedColumn(NULL_TEXT_BLOCK, positionCount);
      } else if (dataType == TSDataType.BLOB) {
        return new RunLengthEncodedColumn(NULL_BLOB_BLOCK, positionCount);
      }
    }
    return new BinaryColumn(0, positionCount, hasNullValue ? valueIsNull : null, values, dataType);
  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public long getRetainedSizeInBytes() {
    // TODO we need to sum up all the Binary's retainedSize here
    long size = INSTANCE_SIZE + arraysRetainedSizeInBytes;
    if (columnBuilderStatus != null) {
      size += ColumnBuilderStatus.INSTANCE_SIZE;
    }
    return size;
  }

  @Override
  public ColumnBuilder newColumnBuilderLike(ColumnBuilderStatus columnBuilderStatus) {
    // TODO we should take retain size into account here
    return new BinaryColumnBuilder(
        columnBuilderStatus, calculateBlockResetSize(positionCount), dataType);
  }

  private void growCapacity() {
    int newSize;
    if (initialized) {
      newSize = ColumnUtil.calculateNewArraySize(values.length);
    } else {
      newSize = initialEntryCount;
      initialized = true;
    }

    valueIsNull = Arrays.copyOf(valueIsNull, newSize);
    values = Arrays.copyOf(values, newSize);
    updateArraysDataSize();
  }

  private void updateArraysDataSize() {
    arraysRetainedSizeInBytes = sizeOf(valueIsNull) + shallowSizeOf(values);
  }
}
