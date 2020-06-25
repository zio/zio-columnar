package zio.columnar

import zio._

sealed trait Page { self =>
  import Page._
  import Page.Prim._

  def dataType: DataType =
    self match {
      case PString(_, _) => DataType.PrimType.PString
      case PInt(_, _) => DataType.PrimType.PInt
      case PLong(_, _) => DataType.PrimType.PLong
      case PFloat(_, _) => DataType.PrimType.PFloat
      case PDouble(_, _) => DataType.PrimType.PDouble
      case PBoolean(_, _) => DataType.PrimType.PBoolean
      case PShort(_, _) => DataType.PrimType.PShort
      case PByte(_, _) => DataType.PrimType.PByte
      case PChar(_, _) => DataType.PrimType.PChar
      case Record(map) => DataType.Record(map.mapValues(_.dataType).toMap)
      case Union(left, right) => DataType.Union(left.dataType, right.dataType)
      case Sequence(elements) => DataType.Sequence(elements.map(_.dataType))
    }
}
object Page {
  sealed trait Prim extends Page
  object Prim {
    final case class PString(data: Chunk[String], mask: Chunk[Boolean]) extends Prim
    final case class PInt(data: Chunk[Int], mask: Chunk[Boolean]) extends Prim
    final case class PLong(data: Chunk[Long], mask: Chunk[Boolean]) extends Prim
    final case class PFloat(data: Chunk[Float], mask: Chunk[Boolean]) extends Prim
    final case class PDouble(data: Chunk[Double], mask: Chunk[Boolean]) extends Prim
    final case class PBoolean(data: Chunk[Boolean], mask: Chunk[Boolean]) extends Prim
    final case class PShort(data: Chunk[Short], mask: Chunk[Boolean]) extends Prim
    final case class PByte(data: Chunk[Byte], mask: Chunk[Boolean]) extends Prim
    final case class PChar(data: Chunk[Char], mask: Chunk[Boolean]) extends Prim
  }
  final case class Record(map: Map[String, Page]) extends Page
  final case class Union(left: Page, right: Page) extends Page
  final case class Sequence(elements: Chunk[Page]) extends Page
}

sealed trait DataType { self =>
  def | (that: DataType): DataType = DataType.Union(self, that)
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
  final case class Union(leftType: DataType, rightType: DataType) extends DataType
  final case class Sequence(valueType: Chunk[DataType]) extends DataType
}

sealed trait Cursor

object Cursor {
  case object Identity extends Cursor
  final case class Field(parent: Cursor, name: String) extends Cursor
  final case class AllFields(parent: Cursor) extends Cursor
  final case class Element(parent: Cursor, index: Int) extends Cursor
  final case class AllElements(parent: Cursor) extends Cursor
  final case class Type(parent: Cursor, dataType: DataType) extends Cursor
}

sealed trait Trans { self =>
  import Trans._

  def + (that: Trans): Trans = Multiple(self.toChunk ++ that.toChunk)

  def run(page: Page): Page = self match {
    case Single(f) => f(page)
    case Multiple(fs) => fs.foldLeft(page)((page, f) => f.run(page))
  }

  def toChunk: Chunk[Single] = self match {
    case Multiple(chunk) => chunk
    case s @ Single(_) => Chunk(s)
  }
}

object Trans {
  final case class Single(run: Page => Page) extends Trans
  final case class Multiple(chunk: Chunk[Single]) extends Trans
}