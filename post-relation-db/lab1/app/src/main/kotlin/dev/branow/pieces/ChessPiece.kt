package dev.branow.pieces

abstract class ChessPiece(
  var color: String,
  var position: Pair<Int, Int>
) {
  abstract val name: String

  fun move(newPosition: Pair<Int, Int>) {
    if (isValidMove(newPosition)) {
      println("$name ($color) moves from $position to $newPosition")
      position = newPosition
    } else {
      println("Invalid move for $name ($color)")
    }
  }

  protected abstract fun isValidMove(newPosition: Pair<Int, Int>): Boolean
}
