package dev.branow.pieces

import dev.branow.pieces.ChessPiece
import kotlin.math.abs

class Knight(color: String, position: Pair<Int, Int>) : ChessPiece(color, position) {
  override val name = "Knight"

  override fun isValidMove(newPosition: Pair<Int, Int>): Boolean {
    val dx = kotlin.math.abs(position.first - newPosition.first)
    val dy = kotlin.math.abs(position.second - newPosition.second)
    return (dx == 2 && dy == 1) || (dx == 1 && dy == 2)
  }
}
