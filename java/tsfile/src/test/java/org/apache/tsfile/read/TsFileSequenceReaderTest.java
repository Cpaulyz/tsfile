/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.read;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.FileGenerator;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TsFileSequenceReaderTest {

  private static final String FILE_PATH = FileGenerator.outputDataFile;
  private TsFileReader tsFile;

  @Before
  public void before() throws IOException {
    int rowCount = 100;
    FileGenerator.generateFile(rowCount, 10000);
    TsFileSequenceReader fileReader = new TsFileSequenceReader(FILE_PATH);
    tsFile = new TsFileReader(fileReader);
  }

  @After
  public void after() throws IOException {
    tsFile.close();
    FileGenerator.after();
  }

  @Test
  public void testReadTsFileSequentially() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);
    reader.position(TSFileConfig.MAGIC_STRING.getBytes().length + 1);
    Map<IDeviceID, List<Pair<Long, Long>>> deviceChunkGroupMetadataOffsets = new HashMap<>();

    long startOffset = reader.position();
    byte marker;
    while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
      switch (marker) {
        case MetaMarker.CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          ChunkHeader header = reader.readChunkHeader(marker);
          int dataSize = header.getDataSize();
          while (dataSize > 0) {
            PageHeader pageHeader =
                reader.readPageHeader(
                    header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
            ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
            dataSize -= pageHeader.getSerializedPageSize();
          }
          break;
        case MetaMarker.CHUNK_GROUP_HEADER:
          ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
          long endOffset = reader.position();
          Pair<Long, Long> pair = new Pair<>(startOffset, endOffset);
          deviceChunkGroupMetadataOffsets.putIfAbsent(
              chunkGroupHeader.getDeviceID(), new ArrayList<>());
          List<Pair<Long, Long>> metadatas =
              deviceChunkGroupMetadataOffsets.get(chunkGroupHeader.getDeviceID());
          metadatas.add(pair);
          startOffset = endOffset;
          break;
        case MetaMarker.OPERATION_INDEX_RANGE:
          reader.readPlanIndex();
          break;
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }
    reader.close();
  }

  @Test
  public void testReadChunkMetadataInDevice() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);

    // test for exist device "d2"
    Map<String, List<ChunkMetadata>> chunkMetadataMap =
        reader.readChunkMetadataInDevice(IDeviceID.Factory.DEFAULT_FACTORY.create("d2"));
    int[] res = new int[] {20, 75, 100, 13};

    Assert.assertEquals(4, chunkMetadataMap.size());
    for (int i = 0; i < chunkMetadataMap.size(); i++) {
      int id = i + 1;
      List<ChunkMetadata> metadataList = chunkMetadataMap.get("s" + id);
      int numOfPoints = 0;
      for (ChunkMetadata metadata : metadataList) {
        numOfPoints += (int) metadata.getNumOfPoints();
      }
      Assert.assertEquals(res[i], numOfPoints);
    }

    // test for non-exist device "d3"
    assertTrue(
        reader.readChunkMetadataInDevice(IDeviceID.Factory.DEFAULT_FACTORY.create("d3")).isEmpty());
    reader.close();
  }

  @Test
  public void testReadChunkMetadataInSimilarDevice() throws IOException, WriteProcessException {
    File testFile = new File(TestConstant.BASE_OUTPUT_PATH + "test.tsfile");
    try (TsFileWriter writer = new TsFileWriter(testFile)) {
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.topic1");
      writer.registerTimeseries(deviceID, new MeasurementSchema("s", TSDataType.DOUBLE));
      TSRecord record = new TSRecord(deviceID, 0);
      record.addTuple(new DoubleDataPoint("s", 0.0));
      writer.writeRecord(record);
    }

    TsFileSequenceReader reader = new TsFileSequenceReader(testFile.getAbsolutePath());

    List<IChunkMetadata> iChunkMetadataList =
        reader.getIChunkMetadataList(Factory.DEFAULT_FACTORY.create("root.topic2"), "s");
    assertTrue(iChunkMetadataList.isEmpty());
  }

  @Test
  public void testReadBloomFilter() throws IOException, WriteProcessException {
    File testFile = new File(TestConstant.BASE_OUTPUT_PATH + "test.tsfile");
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.topic1");
    try (TsFileWriter writer = new TsFileWriter(testFile)) {
      writer.registerTimeseries(deviceID, new MeasurementSchema("s", TSDataType.DOUBLE));
      TSRecord record = new TSRecord(deviceID, 0);
      record.addTuple(new DoubleDataPoint("s", 0.0));
      writer.writeRecord(record);
    }

    TsFileSequenceReader reader = new TsFileSequenceReader(testFile.getAbsolutePath());
    BloomFilter bloomFilter = reader.readBloomFilter();
    assertNotNull(bloomFilter);
    assertTrue(bloomFilter.contains(deviceID.toString() + ".s"));
    assertFalse(
        bloomFilter.contains(Factory.DEFAULT_FACTORY.create("root.topic2").toString() + ".s"));
  }

  @Test
  public void testReadEmptyPageInSelfCheck() throws IOException, WriteProcessException {
    int oldMaxPagePointNum =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
    File testFile = new File(FILE_PATH);

    // create tsfile with empty page
    try (TsFileWriter tsFileWriter = new TsFileWriter(testFile)) {
      // register aligned timeseries
      List<IMeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
      alignedMeasurementSchemas.add(
          new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
      alignedMeasurementSchemas.add(
          new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
      tsFileWriter.registerAlignedTimeseries(new Path("d1"), alignedMeasurementSchemas);

      List<IMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // only write s1
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, "d1", writeMeasurementScheams, 25, 0, 0, true);

      // write s1 and s2, fill 2 empty pages for s2
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, "d1", writeMeasurementScheams, 10, 25, 0, true);
    } finally {
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(oldMaxPagePointNum);
    }

    // read tsfile with selfCheck method
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      Assert.assertEquals(
          TsFileCheckStatus.COMPLETE_FILE,
          reader.selfCheck(new Schema(), new ArrayList<>(), false));
    }
  }
}
