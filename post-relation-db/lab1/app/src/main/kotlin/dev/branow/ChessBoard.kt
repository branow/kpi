package dev.branow

import dev.branow.pieces.ChessPiece

class ChessBoard {
  private val board: Array<Array<ChessPiece?>> = Array(8) { Array<ChessPiece?>(8) { null } }

  fun placePiece(piece: ChessPiece) {
    val (x, y) = piece.position
    board[x][y] = piece
  }

  fun displayBoard() {
    println("Chessboard:")
    for (row in 0 until 8) {
      for (col in 0 until 8) {
        val piece = board[row][col]
        if (piece != null) print("${piece.name.first()}") else print(". ")
      }
      println()
    }
  }

}
