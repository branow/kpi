package dev.branow.pieces

import dev.branow.pieces.ChessPiece

class Rook(color: String, position: Pair<Int, Int>) : ChessPiece(color, position) {
  override val name = "Rook"

  private fun sameRowOrColumn(newPosition: Pair<Int, Int>): Boolean {
    return position.first == newPosition.first || 
        position.second == newPosition.second
  }

  override fun isValidMove(newPosition: Pair<Int, Int>): Boolean = 
      sameRowOrColumn(newPosition)
}
