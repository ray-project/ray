'''
This file implements a function mark_all_placements_for_dot().
The function simulates a Tetris game with a given board configuration,
and marks all valid placements on the board for a 1x1 block (a dot).
It takes an string array as the representation of the board
configuration (and drops the marks), and then outputs the board
with marks of all the valid placements.

In Tetris game, a player controls a block that can move left, right
or down. A block can be "placed" at a location that can no longer move
down any more. Normally, a player can also rotate a block, but for
the simplified case of controling a 1x1 block, rotation does nothing.
'''

from typing import List, Tuple

EMPTY = 0
BLOCK = 1
MARK = 2

EMPTY_CHAR = ' '
BLOCK_CHAR = 'x'
MARK_CHAR = '.'


def _block_value(c: str) -> int:
    if c == EMPTY_CHAR:
        return EMPTY # empty
    elif c == BLOCK_CHAR:
        return BLOCK # block
    elif c == MARK_CHAR:
        return MARK # mark
    else:
        raise ValueError(f'Invalid character {c}')


def _block_str(v: int) -> str:
    if v == EMPTY:
        return EMPTY_CHAR
    elif v == BLOCK:
        return BLOCK_CHAR
    elif v == MARK:
        return MARK_CHAR
    else:
        raise ValueError(f'Invalid block value {v}')


class Board:
    '''
    A board is a 2D array of blocks that represents a Tetris board.
    Each block can be empty, a block. Marks are always dropped on
    init, but can be added afterwards with the mark() method.
    '''
    def __init__(self, rows: List[str], discard_marks: bool = True):
        'Init the board; marks are treated as empty.'
        # Copy and reverse the board, so that row 0 is the bottom row.
        rows = rows.copy()
        rows.reverse() 

        self.height = len(rows)
        assert self.height > 0

        firstRow = rows[0]
        self.width = len(firstRow)
        assert self.width > 0

        board: List[int] = []
        for row in rows:
            assert len(row) == self.width
            if discard_marks:
                board += [ BLOCK if _block_value(c) == BLOCK else EMPTY for c in row ]
            else:
                board += [ _block_value(c) for c in row ]

        self.board = board

    def pos(self, row: int, col: int) -> int:
        return self.width * row + col

    def _is_block_pos(self, pos: int) -> bool:
        return self.board[pos] == BLOCK

    def is_block(self, row: int, col: int) -> bool:
        return self._is_block_pos(self.pos(row, col))
    
    def set(self, row: int, col: int, v: int):
        self.board[self.pos(row, col)] = v

    def mark(self, row: int, col: int):
        self.set(row, col, MARK)

    def board_str(self) -> List[str]:
        result = []
        for row in range(self.height):
            line = ''
            for col in range(self.width):
                line += _block_str(self.board[self.pos(row, col)])
            result.append(line)

        # Reverse so that the bottom row is the last row.
        result.reverse()
        return result


def _find_all_placements_for_dot(board: Board) -> List[Tuple[int, int]]:
    'Find all the valid placements of a 1x1 block on the board.'

    result: List[Tuple[int, int]] = []

    frontier: List[Tuple[int, int]] = []
    row = board.height - 1
    for col in range(board.width):
        if not board.is_block(row, col):
            # Check if the position is a stable placement
            frontier.append((row, col))
    
    visited: Set[int] = set()
    next: List[Tuple[int, int]] = []

    def expand(row: int, col: int):
        if board.is_block(row, col):
            return
        pos = board.pos(row, col)
        if pos not in visited:
            next.append((row, col))
    
    while frontier:
        for row, col in frontier:
            visited.add(board.pos(row, col))
            if row == 0 or board.is_block(row - 1, col):
                result.append((row, col))
        
        next = []
        for row, col in frontier:
            if col > 0:
                expand(row, col - 1) # Move left
            if col < board.width - 1:
                expand(row, col + 1) # Move right
            if row > 0:
                expand(row - 1, col) # Move down
        
        frontier = next

    return result


def mark_all_placements_for_dot(rows: List[str]) -> List[str]:
    'Mark all the valid placements of a 1x1 block on the board.'
    board = Board(rows)

    # Mark all the valid placements in the board.
    for row, col in _find_all_placements_for_dot(board):
        board.mark(row, col)

    # Output the board.
    return board.board_str()


if __name__ == '__main__':
    # Test cases for the markAllPlacementForDot function.
    # Each test case is a list of strings representing the tetris board.
    # Empty spaces are represented by ' ',
    # blocks are represented by 'x',
    # and marks are represented by '.'.
    # All marks should be empty spaces that are valid placements of a 1x1 block.
    test_cases: List[Tuple[List[str], List[str]]] = [
    (
        [' '], # input
        ['.'] # output
    ),
    (
        ['   '],
        ['...']
    ),
    (
        [
            '   ',
            '   '
        ],
        [
            '   ',
            '...'
        ]
    ),
    (
        [
            '   ',
            '   ',
            'xx '
        ],
        [
            '   ',
            '.. ',
            'xx.'
        ]
    ),
    (
        [
            '   ',
            '   ',
            'x x'
        ],
        [
            '   ',
            '. .',
            'x.x'
        ]
    ),
    (
        [
            '   ',
            ' x ',
            'x x'
        ],
        [
            ' . ',
            '.x.',
            'x x'
        ]
    ),
    (
        [
            '   ',
            ' x ',
            '  x'
        ],
        [
            ' . ',
            ' x.',
            '..x'
        ]
    ),
    (
        [
            ' xx',
            ' x ',
            '  x'
        ],
        [
            ' xx',
            ' x ',
            '..x'
        ]
    ),
    (
        [
            ' xx',
            '   ',
            ' xx'
        ],
        [
            ' xx',
            ' ..',
            '.xx'
        ]
    ),
    (
        [
            'xxx',
            'x x',
            '  x'
        ],
        [
            'xxx',
            'x x',
            '  x'
        ]
    ),
    (
        [
            ' xxx',
            '    ',
            'xxx ',
            '    ',
            ' x  '
        ],
        [
            ' xxx',
            '... ',
            'xxx ',
            ' .  ',
            '.x..'
        ]
    ),
    (
        [
            '  xxx  x   x x   x  xxxx  xxxx  xxx  x     xxxxx',
            ' x   x xx  x  x x  x     x     x   x x     x    ',
            ' xxxxx x x x   x    xxx  x     xxxxx x     xxxxx',
            ' x   x x  xx   x       x x     x   x x     x    ',
            ' x   x x   x   x   xxxx   xxxx x   x xxxxx xxxxx',
        ],
        [
            ' .xxx. x.  x x. .x .xxxx .xxxx .xxx. x     xxxxx',
            ' x   x xx. x  x.x  x...  x     x   x x     x    ',
            ' xxxxx x x.x   x    xxx. x     xxxxx x     xxxxx',
            ' x   x x  xx   x   ....x x.... x   x x.... x    ',
            '.x   x.x   x...x...xxxx...xxxx.x   x.xxxxx.xxxxx',
        ]
    )]

    def pretty(lines: List[str]) -> str:
        output = ''
        for line in lines:
            output += f"|{line}|\n"
        return output

    for c, want in test_cases:
        got = mark_all_placements_for_dot(c)
        if got != want:
            raise Exception(f'want:\n{pretty(want)}\n got:\n{pretty(got)}')

    print("All test cases passed!")
