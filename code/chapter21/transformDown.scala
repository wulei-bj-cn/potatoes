//Expression的转换
import org.apache.spark.sql.catalyst.expressions._
val myExpr: Expression = Multiply(Subtract(Literal(6), Literal(4)), Subtract(Literal(1), Literal(9)))
val transformed: Expression = myExpr transformDown {
  case BinaryOperator(l, r) => Add(l, r)
  case IntegerLiteral(i) if i > 5 => Literal(1)
  case IntegerLiteral(i) if i < 5 => Literal(0)
}
