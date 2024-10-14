package ast

import (
    `fmt`
    `strings`
    `unsafe`

    `github.com/bytedance/sonic/internal/native/types`
)

func (self *Parser) syntaxError(err types.ParsingError) SyntaxError {
    return SyntaxError{
        Pos : self.p,
        Src : self.s,
        Code: err,
    }
}

func newSyntaxError(err SyntaxError) *Node {
    msg := err.Description()
    return &Node{
        t: V_ERROR,
        v: int64(err.Code),
        p: unsafe.Pointer(&msg),
    }
}

type SyntaxError struct {
    Pos  int
    Src  string
    Code types.ParsingError
    Msg  string
}

func (self SyntaxError) Error() string {
    return fmt.Sprintf("%q", self.Description())
}

func (self SyntaxError) Description() string {
    return "Syntax error " + self.description()
}

func (self SyntaxError) description() string {
    i := 16
    p := self.Pos - i
    q := self.Pos + i

    /* check for empty source */
    if self.Src == "" {
        return fmt.Sprintf("no sources available: %#v", self)
    }

    /* prevent slicing before the beginning */
    if p < 0 {
        p, q, i = 0, q - p, i + p
    }

    /* prevent slicing beyond the end */
    if n := len(self.Src); q > n {
        n = q - n
        q = len(self.Src)

        /* move the left bound if possible */
        if p > n {
            i += n
            p -= n
        }
    }

    /* left and right length */
    x := clamp_zero(i)
    y := clamp_zero(q - p - i - 1)

    /* compose the error description */
    return fmt.Sprintf(
        "at index %d: %s\n\n\t%s\n\t%s^%s\n",
        self.Pos,
        self.Message(),
        self.Src[p:q],
        strings.Repeat(".", x),
        strings.Repeat(".", y),
    )
}

func (self SyntaxError) Message() string {
    if self.Msg == "" {
        return self.Code.Message()
    }
    return self.Msg
}

func clamp_zero(v int) int {
    if v < 0 {
        return 0
    } else {
        return v
    }
}
