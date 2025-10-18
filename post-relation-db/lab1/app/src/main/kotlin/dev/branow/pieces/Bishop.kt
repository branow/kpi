package dev.branow.pieces

import dev.branow.pieces.ChessPiece
import kotlin.math.abs

class Bishop(color: String, position: Pair<Int, Int>) : ChessPiece(color, position) {
  override val name = "Bishop"

  override fun isValidMove(newPosition: Pair<Int, Int>): Boolean {
    val dx = abs(position.first - newPosition.first)
    val dy = abs(position.first - newPosition.first)
    return dx == dy
  }
}
