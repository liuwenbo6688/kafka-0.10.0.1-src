/**
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
package kafka.log

import kafka.message._
import kafka.common._
import kafka.utils._
import kafka.server.{LogOffsetMetadata, FetchDataInfo}
import org.apache.kafka.common.errors.CorruptRecordException

import scala.math._
import java.io.{IOException, File}


 /**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The message set containing log entries
 * @param index The offset index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment(val log: FileMessageSet,
                 val index: OffsetIndex,
                 val baseOffset: Long,
                 val indexIntervalBytes: Int, // index.interval.bytes
                 val rollJitterMs: Long,
                 time: Time) extends Logging {

  var created = time.milliseconds

  /**
   * the number of bytes since we last added an entry in the offset index
   * 自从上一次写入稀疏索引条目之后到当前时间，写入日志段的数据字节数
   * */
  private var bytesSinceLastIndexEntry = 0

  def this(dir: File, startOffset: Long, indexIntervalBytes: Int, maxIndexSize: Int, rollJitterMs: Long, time: Time, fileAlreadyExists: Boolean = false, initFileSize: Int = 0, preallocate: Boolean = false) =

    this(
        // FileMessageSet ，里面会初始化一个 file channel
        new FileMessageSet(file = Log.logFilename(dir, startOffset), // log 的File对象
                           fileAlreadyExists = fileAlreadyExists,
                           initFileSize = initFileSize,
                           preallocate = preallocate),
        // OffsetIndex
        new OffsetIndex(Log.indexFilename(dir, startOffset),
                        baseOffset = startOffset,
                        maxIndexSize = maxIndexSize),
        startOffset, // 起始offset
        indexIntervalBytes,
        rollJitterMs,
        time
    )

  /* Return the size in bytes of this log segment */
  def size: Long = log.sizeInBytes()

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * It is assumed this method is being called from within a lock.
   *
   * @param offset The first offset in the message set. 要写入的这一批的数据的第一个offset
   * @param messages The messages to append.
   */
  @nonthreadsafe
  def append(offset: Long, messages: ByteBufferMessageSet) {
    if (messages.sizeInBytes > 0) {
      trace("Inserting %d bytes at offset %d at position %d".format(messages.sizeInBytes, offset, log.sizeInBytes()))

      /**
       * append an entry to the index (if needed)
       * 先写index文件  index本质是一个稀疏索引
       * indexIntervalBytes 默认 4096，也就是每写入 4096字节 数据添加一条索引
       */
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        /**
          * OffsetIndex的append，追加稀疏索引
         * offset 1023 -> 物理位置
         * offset 3065 -> 物理位置
         * offset 5078 -> 物理位置
         * offset 7093 -> 物理位置
         * 通过offset二分查找
          */
        index.append(offset, log.sizeInBytes())
        this.bytesSinceLastIndexEntry = 0 // 重新置为0，下次到4096的时候，再写一条系数索引
      }

      /**
       *  append the messages
       *  这就是顺序写入数据， ByteBufferMessageSet封装了本次写入的全部数据
       */
      log.append(messages)

      this.bytesSinceLastIndexEntry += messages.sizeInBytes
    }
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   *
   * The lowerBound argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   *
   * @return The position in the log storing the message with the least offset >= the requested offset or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): OffsetPosition = {
    // 第一步：二分法查找，找最近的偏移量
    // 举个例子：比如索引文件
    // 相对offset  文件便宜地址
    // 1          100
    // 30         200
    // 50         400
    // 70         500
    // 加入要搜索的是 相对offset是38,返回30 ，200
    val mapping = index.lookup(offset)

    // 第二步: 根据最近的偏移，顺序搜索
    log.searchFor(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read
   * @param maxSize The maximum number of bytes to include in the message set we read
   * @param maxOffset An optional maximum offset for the message set we read
   * @param maxPosition The maximum position in the log segment that should be exposed for read
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  @threadsafe
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int, maxPosition: Long = size): FetchDataInfo = {
    if(maxSize < 0)
      throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

    val logSize = log.sizeInBytes // this may change, need to save a consistent copy

    /**
      * 稀疏索引的二分查找
      * 返回offset
      */
    val startPosition = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    if(startPosition == null)
      return null

    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition.position)

    // if the size is zero, still return a log segment but with zero size
    if(maxSize == 0)
      return FetchDataInfo(offsetMetadata, MessageSet.Empty)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    val length = maxOffset match {
      case None =>
        // no max offset, just read until the max position
        min((maxPosition - startPosition.position).toInt, maxSize)
      case Some(offset) =>
        // there is a max offset, translate it to a file position and use that to calculate the max read size;
        // when the leader of a partition changes, it's possible for the new leader's high watermark to be less than the
        // true high watermark in the previous leader for a short window. In this window, if a consumer fetches on an
        // offset between new leader's high watermark and the log end offset, we want to return an empty response.
        if(offset < startOffset)
          return FetchDataInfo(offsetMetadata, MessageSet.Empty)

        val mapping = translateOffset(offset, startPosition.position)
        val endPosition =
          if(mapping == null)
            logSize // the max offset is off the end of the log, use the end of the file
          else
            mapping.position
        min(min(maxPosition, endPosition) - startPosition.position, maxSize).toInt
    }


    FetchDataInfo(
          offsetMetadata,
          /**
            * log.read 从某个物理position 位置读取指定的长度 length
            */
          log.read(startPosition.position, length)
    )
  }

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log and index.
   *
   * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
   * is corrupt.
   *
   * @return The number of bytes truncated from the log
   */
  @nonthreadsafe
  def recover(maxMessageSize: Int): Int = {
    index.truncate()
    index.resize(index.maxIndexSize)
    var validBytes = 0
    var lastIndexEntry = 0
    val iter = log.iterator(maxMessageSize)
    try {
      while(iter.hasNext) {
        val entry = iter.next
        entry.message.ensureValid()
        if(validBytes - lastIndexEntry > indexIntervalBytes) {
          // we need to decompress the message, if required, to get the offset of the first uncompressed message
          val startOffset =
            entry.message.compressionCodec match {
              case NoCompressionCodec =>
                entry.offset
              case _ =>
                ByteBufferMessageSet.deepIterator(entry).next().offset
          }
          index.append(startOffset, validBytes)
          lastIndexEntry = validBytes
        }
        validBytes += MessageSet.entrySize(entry.message)
      }
    } catch {
      case e: CorruptRecordException =>
        logger.warn("Found invalid messages in log segment %s at byte offset %d: %s.".format(log.file.getAbsolutePath, validBytes, e.getMessage))
    }
    val truncated = log.sizeInBytes - validBytes
    log.truncateTo(validBytes)
    index.trimToValidSize()
    truncated
  }

  override def toString() = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    val mapping = translateOffset(offset)
    if(mapping == null)
      return 0
    index.truncateTo(offset)
    // after truncation, reset and allocate more space for the (new currently  active) index
    index.resize(index.maxIndexSize)
    val bytesTruncated = log.truncateTo(mapping.position)
    if(log.sizeInBytes == 0)
      created = time.milliseconds
    bytesSinceLastIndexEntry = 0
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def nextOffset(): Long = {
    val ms = read(index.lastOffset, None, log.sizeInBytes)
    if(ms == null) {
      baseOffset
    } else {
      ms.messageSet.lastOption match {
        case None => baseOffset
        case Some(last) => last.nextOffset
      }
    }
  }

  /**
   * Flush this log segment to disk
   */
  @threadsafe
  def flush() {
    LogFlushStats.logFlushTimer.time {
      // 刷新到磁盘，其实就是 force
      // 为什么index文件用MappedByteBuffer？
      // 因为索引文件系数索引是非常小的，完全可以都写着在内存里，然后定期flush
      log.flush()
      index.flush()
    }
  }

  /**
   * Change the suffix for the index and log file for this log segment
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String) {

    def kafkaStorageException(fileType: String, e: IOException) =
      new KafkaStorageException(s"Failed to change the $fileType file suffix from $oldSuffix to $newSuffix for log segment $baseOffset", e)

    try log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("log", e)
    }
    try index.renameTo(new File(CoreUtils.replaceSuffix(index.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("index", e)
    }
  }

  /**
   * Close this log segment
   */
  def close() {
    CoreUtils.swallow(index.close)
    CoreUtils.swallow(log.close)
  }

  /**
   * Delete this log segment from the filesystem.
   * @throws KafkaStorageException if the delete fails.
   */
  def delete() {
    val deletedLog = log.delete()
    val deletedIndex = index.delete()
    if(!deletedLog && log.file.exists)
      throw new KafkaStorageException("Delete of log " + log.file.getName + " failed.")
    if(!deletedIndex && index.file.exists)
      throw new KafkaStorageException("Delete of index " + index.file.getName + " failed.")
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    log.file.setLastModified(ms)
    index.file.setLastModified(ms)
  }
}
