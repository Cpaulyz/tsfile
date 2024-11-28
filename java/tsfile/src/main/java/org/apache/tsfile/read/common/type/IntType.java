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

package org.apache.tsfile.read.common.type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;

public class IntType extends AbstractIntType {

  public static final IntType INT32 = new IntType();

  private IntType() {}

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    return new IntColumnBuilder(null, expectedEntries, TSDataType.INT32);
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.INT32;
  }

  @Override
  public String getDisplayName() {
    return "INT32";
  }

  public static IntType getInstance() {
    return INT32;
  }
}
