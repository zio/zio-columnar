package zio.columnar

import zio.Chunk

object parquet {

  final case class FileMetadata(version: Int, schemaElements: Chunk[SchemaElement], rowCount: Long, rowGroup: Chunk[RowGroup], keyValueMetadata: Chunk[KeyValue])
  final case class RowGroup(columns: Chunk[ColumnChunk], totalByteSize: Long, rowCount: Long)
  final case class SchemaElement(`type`: Type, typeLength: Int, repetitionField: FieldRepetitionType, name: String,  childCount: Int)
  final case class KeyValue(key: String, value: String)
  final case class ColumnChunk(filePath: String, fileOffset: Long, metaData: ColumnMetaData)

  final case class ColumnMetaData(`type`: Type, encodings: Chunk[Encoding], pathInSchema: Chunk[String], codec: CompressionCodec, valueCount: Long, totalUncompressedSize: Long, totalCompressedSize: Long, keyValueMetaData: Chunk[KeyValue], dataPageOffset: Long, indexPageOffset: Long, dictionaryPageOffset: Long)

  sealed trait Encoding {
    def ordinal: Int
  }

  object Encoding {
    val allTypes = List(
      Plain,
      GroupVarInt,
      PlainDictionary,
      Rle,
      BitPacked
    )

    def fromOrdinal(int: Int): Option[Encoding] =
      allTypes.lift(int)

    sealed abstract class AbstractEncodingType(val ordinal: Int) extends Encoding

    case object Plain extends AbstractEncodingType(0)
    case object GroupVarInt extends AbstractEncodingType(1)
    case object PlainDictionary extends AbstractEncodingType(2)
    case object Rle extends AbstractEncodingType(3)
    case object BitPacked extends AbstractEncodingType(4)
  }

  sealed trait CompressionCodec {
    def ordinal: Int
  }

  object CompressionCodec {

    val allTypes = Vector(Uncompressed, Snappy, Gzip, Lzo)

    def fromOrdinal(int: Int): Option[CompressionCodec] =
      allTypes.lift(int)

    sealed abstract class AbstractCompressionCodec(val ordinal: Int) extends CompressionCodec

    case object Uncompressed extends AbstractCompressionCodec(0)
    case object Snappy extends AbstractCompressionCodec(1)
    case object Gzip extends AbstractCompressionCodec(2)
    case object Lzo extends AbstractCompressionCodec(3)
  }

  final case class PageHeader(`type`: PageType, uncompressedPageSize: Int, compressedPageSize: Int, crc: Int, dataPageHeader: DataPageHeader, indexPageHeader: IndexPageHeader, dictionaryPageHeader: DictionaryPageHeader)


  final case class DataPageHeader(valueCount: Int, encoding: Encoding, definitionLevelEncoding: Encoding, repetitionLevelEncoding: Encoding)
  final case class IndexPageHeader()
  final case class DictionaryPageHeader(valueCount: Int)

  sealed trait ConvertedType {
    def ordinal: Int
  }

  object ConvertedType {
    val allTypes =
      Vector(
        Utf8,
        Map,
        MapKeyValue,
        List
      )

    def fromOrdinal(int: Int): Option[ConvertedType] =
      allTypes.lift(int)

    sealed abstract class AbstractConvertedType(val ordinal: Int) extends ConvertedType

    case object Utf8 extends AbstractConvertedType(0)
    case object Map extends AbstractConvertedType(1)
    case object MapKeyValue extends AbstractConvertedType(2)
    case object List extends AbstractConvertedType(3)
  }


  sealed trait PageType {
    def ordinal: Int
  }

  object PageType {
    val allTypes = Vector(DataPage, IndexPage)

    def fromOrdinal(int: Int): Option[PageType] =
      allTypes.lift(int)

    abstract sealed class AbstractPageType(val ordinal: Int) extends PageType
    case object DataPage extends AbstractPageType(0)
    case object IndexPage extends AbstractPageType(1)
  }

  sealed trait Type {
    def ordinal: Int
  }

  object Type {
    val allTypes: Vector[AbstractType] = Vector(
        Boolean,
        Int32,
        Int64,
        Int96,
        Float,
        Double,
        ByteArray,
        FixedLengthByteArray
    )

    def fromOrdinal(int: Int): Option[Type] =
      allTypes.lift(int)

    sealed abstract class AbstractType(val ordinal: Int) extends Type

    case object Boolean extends AbstractType(0)
    case object Int32 extends AbstractType(1)
    case object Int64 extends AbstractType(2)
    case object Int96 extends AbstractType(3)
    case object Float extends AbstractType(4)
    case object Double extends AbstractType(5)
    case object ByteArray extends AbstractType(6)
    case object FixedLengthByteArray extends AbstractType(7)
  }

  sealed trait FieldRepetitionType {
    def ordinal: Int
  }

  object FieldRepetitionType {

    val allTypes: Vector[AbstractFieldRepetitionType] = Vector(
      Required, Optional, Required
    )

    def fromOrdinal(int: Int): Option[FieldRepetitionType] =
      allTypes.lift(int)

    sealed abstract class AbstractFieldRepetitionType(val ordinal: Int) extends FieldRepetitionType

    final case object Required extends AbstractFieldRepetitionType(0)
    final case object Optional extends AbstractFieldRepetitionType(1)
    final case object Repeated extends AbstractFieldRepetitionType(2)
  }
}
