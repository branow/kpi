package dev.branow

import dev.branow.pieces.ChessPiece
import dev.branow.pieces.Rook
import dev.branow.pieces.Bishop
import dev.branow.pieces.Knight

class Player(val name: String, val color: String) {
  val pieces: MutableList<ChessPiece> = mutableListOf()

  fun addPiece(piece: ChessPiece) {
    pieces.add(piece)
  }

  fun addPiece(pieceName: String, position: Pair<Int, Int>) {
    val piece = when(pieceName.lowercase()) {
      "rook" -> Rook(color, position)
      "bishop" -> Bishop(color, position)
      "knight" -> Knight(color, position)
      else -> throw IllegalArgumentException("Unknown piece: $pieceName")
    }
    pieces.add(piece)
  }

  fun showPieces() {
    println("$name ($color) has the following pieces:")
    for (piece in pieces) {
      println("- ${piece.name} at ${piece.position}")
    }
  }
}
