package dev.branow

import dev.branow.pieces.Rook

fun main() {
  val player1 = Player("Alice", "White")
  val player2 = Player("Bob", "Black")

  player1.addPiece(Rook("White", Pair(0, 0)))
  player1.addPiece("bishop", Pair(0, 2))
  player2.addPiece("knight", Pair(7, 1))

  player1.showPieces()
  player2.showPieces()

  var board = ChessBoard()
  player1.pieces.forEach { board.placePiece(it) }
  player2.pieces.forEach { board.placePiece(it) }
  board.displayBoard()

  player1.pieces[0].move(Pair(0, 5))
  player1.pieces[1].move(Pair(2, 4))
  player2.pieces[0].move(Pair(5, 2))
  player2.pieces[0].move(Pair(6, 6))
}
