package zio.columnar

import zio.Chunk
import zio.columnar.thrift.TType.AbstractTType

object thrift {
  private val protocolId = 0x82.toByte
  private val version = 1
  private val versionMask = 0x1f // 0001 1111
  private val typeMask = 0xE0.toByte // 1110 0000
  private val typeBits = 0x07 // 0000 0111
  private val typeShiftAmount = 5

  final case class TStruct(name: String)
  final case class TField(name: String, fieldType: TType, id: Short)
  final case class TList(elemType: TType, size: Int)
  final case class TMap(keyType: TType, valueType: TType, size: Int)
  final case class TMessage(name: String, messageType: TType, sequenceId: Int)

  sealed trait TMessageType

  object TMessageType {

  }

  sealed trait TType

  object TType {

    val allTypes = Vector(Stop, Void, Bool, Byte, Double, I16, I32, I64, String, Struct, Map, Set, List, Enum)


    def fromOrdinal(int: Int): Option[TType] =
      allTypes.lift(int)

    abstract class AbstractTType(val code: Int) extends TType

    case object Stop extends AbstractTType(0)
    case object Void extends AbstractTType(1)
    case object Bool extends AbstractTType(2)
    case object Byte extends AbstractTType(3)
    case object Double extends AbstractTType(4)
    case object I16 extends AbstractTType(6)
    case object I32 extends AbstractTType(8)
    case object I64 extends AbstractTType(10)
    case object String extends AbstractTType(11)
    case object Struct extends AbstractTType(12)
    case object Map extends AbstractTType(13)
    case object Set extends AbstractTType(14)
    case object List extends AbstractTType(15)
    case object Enum extends AbstractTType(16)

  }


  class TCompactProtocol(bitIndex0: Int, bitChunk: Chunk[Boolean]) {
    var bitIndex = bitIndex0
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

      case object BooleanTrue extends AbstractTypeCode(1)
      case object BooleanFalse extends AbstractTypeCode(2)
      case object Byte extends AbstractTypeCode(3)
      case object I16 extends AbstractTypeCode(4)
      case object I32 extends AbstractTypeCode(5)
      case object I64 extends AbstractTypeCode(6)
      case object Double extends AbstractTypeCode(7)
      case object Binary extends AbstractTypeCode(8)
      case object List extends AbstractTypeCode(9)
      case object Set extends AbstractTypeCode(10)
      case object Map extends AbstractTypeCode(11)
      case object Struct extends AbstractTypeCode(12)

    }

  }



}
