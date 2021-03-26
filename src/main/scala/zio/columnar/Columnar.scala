package zio.columnar

import zio._

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
 *   [*].employees[*].age#Int = [[21, 41], [undefined]]
 *   [*].employees[*].age#String = [[undefined, undefined], ["22"]]
 *   [*].employees[*].ids[*]  = [[[11, 31, 41], [11, 31, 41]],  [[12, 32, 42]]]
 *
 *
 */
object EmployeesExample {}

final case class CursorMap(private val map: Map[Cursor[_], ColumnarData[_]]) {
  def unsafeTransform(f: ColumnarData[_] => ColumnarData[_]): CursorMap =
    CursorMap(map.view.mapValues(f).toMap)

  def get[A](cursor: Cursor[A]): Option[ColumnarData[A]]            = ???
  def updated[A](key: Cursor[A], value: ColumnarData[A]): CursorMap = ???
  def filterKeys(f: Cursor[_] => Boolean): CursorMap = ???
}

final case class Page(private val data: CursorMap) {
  // TODO
  def +(that: Page): Page = ???
  def unary_- : Page = Page(data.unsafeTransform(- _))
  def get(cursor: Cursor[_]): Page =
    Page(data.filterKeys(_.startsWith(cursor)))
}

sealed trait ColumnarData[+A] { self =>
  import ColumnarData._


  // TODO; Complete the other cases.
  def +[A1 >: A](that: ColumnarData[A1]): ColumnarData[A1] =
    ((self, that) match {
      case (PInt(r1, m1), PInt(r2, m2)) => PInt(r1.zipWith(r2)(_ + _), m1.zipWith(m2)(_ && _))
    }).asInstanceOf[ColumnarData[A1]]

  def unary_- : ColumnarData[A] =
    (this match {
      case PSequence(rows) => PSequence(rows.map(- _))
      case NA(message) => NA(message)
      case PString(rows, mask) => NA("String type doesn't support unary negation.")
      case PInt(rows, mask) => PInt(rows.map(- _), mask)
      case PLong(rows, mask) => PLong(rows.map(- _), mask)
      case PFloat(rows, mask) => PFloat(rows.map(- _), mask)
      case PDouble(rows, mask) => PDouble(rows.map(- _), mask)
      case PBoolean(rows, mask) => NA("Boolean type doesn't support unary negation.")
      case PShort(rows, mask) => PShort(rows.map(v => (-v).toShort), mask)
      case PByte(rows, mask) => PByte(rows.map(v => (-v).toByte), mask)
      case PChar(rows, mask) => PChar(rows.map(v => (-v).toChar), mask)
    }).asInstanceOf[ColumnarData[A]]
}

object ColumnarData {
  final case class PSequence[A](rows: Chunk[ColumnarData[A]]) extends ColumnarData[A]

  final case class NA(message: String) extends ColumnarData[Nothing]
  final case class PString(rows: Chunk[String], mask: Chunk[Boolean])   extends ColumnarData[String]
  final case class PInt(rows: Chunk[Int], mask: Chunk[Boolean])         extends ColumnarData[Int]
  final case class PLong(rows: Chunk[Long], mask: Chunk[Boolean])       extends ColumnarData[Long]
  final case class PFloat(rows: Chunk[Float], mask: Chunk[Boolean])     extends ColumnarData[Float]
  final case class PDouble(rows: Chunk[Double], mask: Chunk[Boolean])   extends ColumnarData[Double]
  final case class PBoolean(rows: Chunk[Boolean], mask: Chunk[Boolean]) extends ColumnarData[Boolean]
  final case class PShort(rows: Chunk[Short], mask: Chunk[Boolean])     extends ColumnarData[Short]
  final case class PByte(rows: Chunk[Byte], mask: Chunk[Boolean])       extends ColumnarData[Byte]
  final case class PChar(rows: Chunk[Char], mask: Chunk[Boolean])       extends ColumnarData[Char]
}

sealed trait DataType[+A] { self =>
  def |[B](that: DataType[B]): DataType[A with B] = DataType.Union(self, that)
}

object DataType {
  sealed trait PrimType[A] extends DataType[A]

  object PrimType {
    case object PString  extends PrimType[String]
    case object PInt     extends PrimType[Int]
    case object PLong    extends PrimType[Long]
    case object PFloat   extends PrimType[Float]
    case object PDouble  extends PrimType[Double]
    case object PBoolean extends PrimType[Boolean]
    case object PShort   extends PrimType[Short]
    case object PByte    extends PrimType[Byte]
    case object PChar    extends PrimType[Char]
  }
  final case class Record(valueType: Map[String, DataType[_]])                extends DataType[Unknown]
  final case class Union[A, B](leftType: DataType[A], rightType: DataType[B]) extends DataType[A with B]
  final case class Sequence(elementType: DataType[_])                         extends DataType[Unknown]
}

//
trait Unknown

sealed trait Cursor[+A] { self =>
  def field(name: String): Cursor[Unknown] =
    Cursor.Field(self, name)

  def element(index: Int): Cursor[Unknown] =
    Cursor.Element(self, index)

  def allFields: Cursor[Unknown] =
    Cursor.AllFields(self)

  def allElements: Cursor[Unknown] =
    Cursor.AllElements(self)

  def typed[B](dataType: DataType[B]): Cursor[B] =
    Cursor.Type(self, dataType)

  import Cursor._

  def startsWith(that: Cursor[_]): Boolean =
    if (that == Root)
      true
    else if (self == that)
      true
    else
      self match {
        case Root => false
        case (Field(parent1, _)) =>
          parent1.startsWith(that)
        case AllFields(parent1) =>
          parent1.startsWith(that)
        case Element(parent1, _) =>
          parent1.startsWith(that)
        case AllElements(parent1) =>
          parent1.startsWith(that)
        case Type(parent1, _) =>
          parent1.startsWith(that)
      }
}

// Introduce type paramet in cursor.
object Cursor {

  /**
   * Cursor.Field(Cursor.AllElements(Cursor.Root), "id")
   *
   * Map(Cursor.Field(Cursor.AllElements(Cursor.Root), "id") -> PString(Chunk("foo", "bar"))
   *
   * // So for age the cursor looks like this:
   *
   * // Cursor for age string
   * Cursor.Field(Cursor.AllElements(Cursor.Field(Cursor.Type(Cursor.AllElements(Cursor.Root), DataType.PrimType.Int), "age")), "employees")
   * Cursor.Field(Cursor.AllElements(Cursor.Field(Cursor.Type(Cursor.AllElements(Cursor.Root), DataType.PrimType.String), "age")), "employees")
   *
   */
  case object Root                                                   extends Cursor[Unknown]
  final case class Field(parent: Cursor[_], name: String)            extends Cursor[Unknown]
  final case class AllFields(parent: Cursor[_])                      extends Cursor[Unknown]
  final case class Element(parent: Cursor[_], index: Int)            extends Cursor[Unknown]
  final case class AllElements(parent: Cursor[_])                    extends Cursor[Unknown]
  final case class Type[A](parent: Cursor[_], dataType: DataType[A]) extends Cursor[A]
}

final case class Pipeline private (steps: Chunk[Page => Page]) { self =>
  //TODO
  def + (pipeline: Pipeline): Pipeline = ???
  def ++ (pipeline: Pipeline): Pipeline = Pipeline(steps ++ pipeline.steps)
  def run(page: Page): Page = steps.foldLeft(page)((page, step) => step(page))
  def unary_- : Pipeline =
    self ++ Pipeline(page => - page)

//  {
//    Pipeline(cursor, page =>  page.data.get(cursor).map(data => ))
//  }
  // def field(name: String): Pipeline[A] = copy(cursor = cursor.field(name))
}

object Pipeline {
  def apply(step: Page => Page): Pipeline =
    Pipeline(Chunk(step))
}
