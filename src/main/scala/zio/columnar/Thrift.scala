package zio.columnar

import java.io.IOException
import java.lang.annotation.ElementType

import zio.{ Chunk, IO, UIO, ZIO }
import zio.columnar.thrift.TType.AbstractTType

object thrift {
  private val protocolId      = 0x82.toByte
  private val version         = 1
  private val versionMask     = 0x1f // 0001 1111
  private val typeMask        = 0xE0.toByte // 1110 0000
  private val typeBits        = 0x07 // 0000 0111
  private val typeShiftAmount = 5

  final case class TStruct(name: String)
  final case class TField(name: String, fieldType: TType, id: Short)
  final case class TList(elemType: TType, size: Int)
  final case class TSet(elemType: TType, size: Int)
  final case class TMap(keyType: TType, valueType: TType, size: Int)
  final case class TMessage(name: String, messageType: TType, sequenceId: Int)

  sealed trait TMessageType

  object TMessageType {}

  sealed trait TType {
    def code: Int
  }

  object TType {
    val allTypes = Vector(Stop, Void, Bool, Byte, Double, I16, I32, I64, String, Struct, Map, Set, List, Enum)

    def fromOrdinal(int: Int): Option[TType] =
      allTypes.lift(int)

    abstract class AbstractTType(val code: Int) extends TType

    case object Stop   extends AbstractTType(0)
    case object Void   extends AbstractTType(1)
    case object Bool   extends AbstractTType(2)
    case object Byte   extends AbstractTType(3)
    case object Double extends AbstractTType(4)
    case object I16    extends AbstractTType(6)
    case object I32    extends AbstractTType(8)
    case object I64    extends AbstractTType(10)
    case object String extends AbstractTType(11)
    case object Struct extends AbstractTType(12)
    case object Map    extends AbstractTType(13)
    case object Set    extends AbstractTType(14)
    case object List   extends AbstractTType(15)
    case object Enum   extends AbstractTType(16)
  }

  class TCompactProtocol(transport: TTransport, stringLengthLimit: Long, containerLengthLimit: Long) extends TProtocol {
    override def writeMessageBegin(message: TMessage): Unit = {
      writeByteDirect(protocolId)
      writeByteDirect((version & versionMask) | ((message.messageType.code << typeShiftAmount) & typeMask))
      writeVarint32(message.sequenceId)
      writeString(message.name)
    }

    override def writeMessageEnd(): Unit = ???

    override def writeStructBegin(struct: TStruct): Unit = ???

    override def writeStructEnd(): Unit = ???

    override def writeFieldBegin(field: TField): Unit = ???

    override def writeFieldEnd(): Unit = ???

    override def writeFieldStop(): Unit = ???

    override def writeMapBegin(map: TMap): Unit = ???

    override def writeMapEnd(): Unit = ???

    override def writeListBegin(list: TList): Unit = ???

    override def writeListEnd(): Unit = ???

    override def writeSetBegin(tset: TSet): Unit = ???

    override def writeSetEnd(): Unit = ???

    override def writeBool(b: Boolean): Unit = ???

    override def writeByte(b: Byte): Unit = ???

    override def writeI16(i16: Short): Unit = ???

    override def writeI32(i32: Int): Unit = ???

    override def writeI64(i64: Long): Unit = ???

    override def writeDouble(dub: Double): Unit = ???

    override def writeBinary(buf: Chunk[Byte]): Unit = ???

    override def writeBinary(buf: Array[Byte], offset: Int, length: Int): Unit = ???

    /**
     * Reading methods.
     */
    override def readMessageBegin: TMessage = ???

    override def readMessageEnd(): Unit = ???

    override def readStructBegin: TStruct = ???

    override def readStructEnd(): Unit = ???

    override def readFieldBegin: TField = ???

    override def readFieldEnd(): Unit = ???

    override def readMapBegin: TMap = ???

    override def readMapEnd(): Unit = ???

    override def readListBegin: TList = ???

    override def readListEnd(): Unit = ???

    override def readSetBegin: TSet = ???

    override def readSetEnd(): Unit = ???

    override def readBool: Boolean = ???

    override def readByte: Byte = ???

    override def readI16: Short = ???

    override def readI32: Int = ???

    override def readI64: Long = ???

    override def readDouble: Double = ???

    override def readString: String = ???

    override def readBinary: Chunk[Byte] = ???

    private val temp = new Array[Byte](10)

    private def writeByteDirect(n: Int): Unit = {
      temp(0) = n.toByte
      transport.write(temp, 0, 1)
    }

    private def writeVarint32(n0: Int): Unit = {
      var n    = n0
      var idx  = 0
      var loop = true

      while (loop) {
        if ((n & ~0x7F) == 0) {
          temp(idx) = n.toByte
          idx += 1
          loop = false
        } else {
          temp(idx) = ((n & 0x7F) | 0x80).toByte // writeByteDirect((byte)((n & 0x7F) | 0x80));
          idx += 1
          n >>>= 7
        }
      }
      transport.write(temp, 0, idx)
    }

    import java.nio.charset.StandardCharsets

    def writeString(str: String): Unit = {
      val bytes = str.getBytes(StandardCharsets.UTF_8)
      writeBinary(bytes, 0, bytes.length)
    }
  }

  object TCompactProtocol {
    sealed trait TypeCode

    object TypeCode {
      val allTypes = Vector(
        BooleanTrue,
        BooleanFalse,
        Byte,
        I16,
        I32,
        I64,
        Double,
        Binary,
        List,
        Set,
        Map,
        Struct
      )

      def fromOrdinal(int: Int): Option[TypeCode] =
        allTypes.lift(int)

      abstract class AbstractTypeCode(val code: Int) extends TypeCode

      case object BooleanTrue  extends AbstractTypeCode(1)
      case object BooleanFalse extends AbstractTypeCode(2)
      case object Byte         extends AbstractTypeCode(3)
      case object I16          extends AbstractTypeCode(4)
      case object I32          extends AbstractTypeCode(5)
      case object I64          extends AbstractTypeCode(6)
      case object Double       extends AbstractTypeCode(7)
      case object Binary       extends AbstractTypeCode(8)
      case object List         extends AbstractTypeCode(9)
      case object Set          extends AbstractTypeCode(10)
      case object Map          extends AbstractTypeCode(11)
      case object Struct       extends AbstractTypeCode(12)
    }
  }

  trait TProtocol {
    import zio.columnar.thrift.TField
    import zio.columnar.thrift.TList
    import zio.columnar.thrift.TMessage
    import zio.columnar.thrift.TStruct

    def writeMessageBegin(message: thrift.TMessage): Unit

    def writeMessageEnd(): Unit

    def writeStructBegin(struct: thrift.TStruct): Unit

    def writeStructEnd(): Unit

    def writeFieldBegin(field: thrift.TField): Unit

    def writeFieldEnd(): Unit

    def writeFieldStop(): Unit

    def writeMapBegin(map: TMap): Unit

    def writeMapEnd(): Unit

    def writeListBegin(list: thrift.TList): Unit

    def writeListEnd(): Unit

    def writeSetBegin(tset: TSet): Unit

    def writeSetEnd(): Unit

    def writeBool(b: Boolean): Unit

    def writeByte(b: Byte): Unit

    def writeI16(i16: Short): Unit

    def writeI32(i32: Int): Unit

    def writeI64(i64: Long): Unit

    def writeDouble(dub: Double): Unit

    def writeString(str: String): Unit

    def writeBinary(buf: Chunk[Byte]): Unit

    def writeBinary(buf: Array[Byte], offset: Int, length: Int): Unit

    /**
     * Reading methods.
     */
    def readMessageBegin: thrift.TMessage

    def readMessageEnd(): Unit

    def readStructBegin: thrift.TStruct

    def readStructEnd(): Unit

    def readFieldBegin: thrift.TField

    def readFieldEnd(): Unit

    def readMapBegin: TMap

    def readMapEnd(): Unit

    def readListBegin: thrift.TList

    def readListEnd(): Unit

    // size
    def readSetBegin: TSet

    def readSetEnd(): Unit

    def readBool: Boolean

    def readByte: Byte

    def readI16: Short

    def readI32: Int

    def readI64: Long

    def readDouble: Double

    def readString: String

    def readBinary: Chunk[Byte]
  }

  trait TTransport {
    def peek(): Boolean
    def open(): Unit
    def close(): Unit
    def readUpToN(n: Int): Chunk[Byte]
    def readN(n: Int): Chunk[Byte]
    def write(bytes: Chunk[Byte]): Unit
    def write(bytes: Array[Byte], offset: Int, length: Int): Unit
    def flush(): Unit
  }
}
