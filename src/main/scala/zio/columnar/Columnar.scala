package zio.columnar

import zio._
import zio.columnar.Page.Prim.{PSequence, PString}
import zio.columnar.Page.Record

sealed trait Page { self =>
  import Page._
  import Page.Prim._

  def dataType: DataType =
    self match {
      case PString(_, _)      => DataType.PrimType.PString
      case PInt(_, _)         => DataType.PrimType.PInt
      case PLong(_, _)        => DataType.PrimType.PLong
      case PFloat(_, _)       => DataType.PrimType.PFloat
      case PDouble(_, _)      => DataType.PrimType.PDouble
      case PBoolean(_, _)     => DataType.PrimType.PBoolean
      case PShort(_, _)       => DataType.PrimType.PShort
      case PByte(_, _)        => DataType.PrimType.PByte
      case PChar(_, _)        => DataType.PrimType.PChar
      case Record(map)        => DataType.Record(map.mapValues(_.dataType).toMap)
      case Union(left, right) => DataType.Union(left.dataType, right.dataType)
      case Sequence(elements) => DataType.Sequence(elements.map(_.dataType))
    }
}

/**
  * [{
  *      "id" : "foo",
  *     "employees" : [
  *        {
  *           name : 11
  *           age : 21
  *           ids: [11, 31, 41]
  *        },
  *        {
  *          name : 31
  *          age : 41
  *          ids: [11, 31, 41]
  *
  *        }
  *     ]
  *  },
  * {
  *     "id" : "bar",
  *     "employees" : [
  *        {
  *           name : 12
  *           age : 22
  *           ids: [12, 32, 42]
  *        },
  *     ]
  *  }]
  *
  *
  *   [0].employees[0].name=11
  *   [0].employees[0].age=21
  *   [0].employees[0].ids[0] = 11
  *   [0].employees[0].ids[1] = 31
  *   [0].employees[0].ids[2] = 41
  *
  *   [0].employees[1].name=31
  *   [0].employees[1].age=41
  *   [0].employees[1].ids[0] = 11
  *   [0].employees[1].ids[1] = 31
  *   [0].employees[1].ids[2] = 41
  *
  *   [1].employees[0].name=12
  *   [1].employees[0].age=22
  *   [1].employees[0].ids[0] = 12
  *   [1].employees[0].ids[1] = 32
  *   [1].employees[0].ids[2] = 42
  *
  *
  *   [*].id                   = ["foo", "bar"]
  *   [*].employees[*].name    = [[11, 31], [12]]
  *   [*].employees[*].age     = [[21, 41], [22]]
  *   [*].employees[*].ids[*]  = [[[11, 31, 41], [11, 31, 41]],  [[12, 32, 42]]]
  *
  *
  */
object EmployeesExample {}

final case class Page(data: Map[Cursor, ColumnarData])

sealed trait ColumnarData

object ColumnarData {
  final case class PSequence(rows: Chunk[ColumnarData]) extends ColumnarData

  final case class PString(rows: Chunk[String], mask: Chunk[Boolean])
      extends ColumnarData
  final case class PInt(rows: Chunk[Int], mask: Chunk[Boolean])
      extends ColumnarData
  final case class PLong(rows: Chunk[Long], mask: Chunk[Boolean])
      extends ColumnarData
  final case class PFloat(rows: Chunk[Float], mask: Chunk[Boolean])
      extends ColumnarData
  final case class PDouble(rows: Chunk[Double], mask: Chunk[Boolean])
      extends ColumnarData
  final case class PBoolean(rows: Chunk[Boolean], mask: Chunk[Boolean])
      extends ColumnarData
  final case class PShort(rows: Chunk[Short], mask: Chunk[Boolean])
      extends ColumnarData
  final case class PByte(rows: Chunk[Byte], mask: Chunk[Boolean])
      extends ColumnarData
  final case class PChar(rows: Chunk[Char], mask: Chunk[Boolean])
      extends ColumnarData
}

sealed trait DataType { self =>
  def |(that: DataType): DataType = DataType.Union(self, that)
}

object DataType {
  sealed trait PrimType extends DataType
  object PrimType {
    case object PString extends PrimType
    case object PInt extends PrimType
    case object PLong extends PrimType
    case object PFloat extends PrimType
    case object PDouble extends PrimType
    case object PBoolean extends PrimType
    case object PShort extends PrimType
    case object PByte extends PrimType
    case object PChar extends PrimType
  }
  final case class Record(valueType: Map[String, DataType]) extends DataType
  final case class Union(leftType: DataType, rightType: DataType)
      extends DataType
  final case class Sequence(elementType: DataType) extends DataType
  final case class Tuple(valueType: Chunk[DataType]) extends DataType
}

sealed trait Cursor

object Cursor {

  /**
    * Cursor.Field(Cursor.AllElements(Cursor.Root), "id")
    *
    * Map(Cursor.Field(Cursor.AllElements(Cursor.Root), "id") -> PString(Chunk("foo", "bar"))
    */
  case object Root extends Cursor
  final case class Field(parent: Cursor, name: String) extends Cursor
  final case class AllFields(parent: Cursor) extends Cursor
  final case class Element(parent: Cursor, index: Int) extends Cursor
  final case class AllElements(parent: Cursor) extends Cursor
  final case class Type(parent: Cursor, dataType: DataType) extends Cursor
}

sealed trait Trans { self =>
  import Trans._

  def +(that: Trans): Trans = Multiple(self.toChunk ++ that.toChunk)

  def run(page: Page): Page = self match {
    case Single(f)    => f(page)
    case Multiple(fs) => fs.foldLeft(page)((page, f) => f.run(page))
  }

  def toChunk: Chunk[Single] = self match {
    case Multiple(chunk) => chunk
    case s @ Single(_)   => Chunk(s)
  }
}

object Trans {
  final case class Single(run: Page => Page) extends Trans
  final case class Multiple(chunk: Chunk[Single]) extends Trans
}
