package urn

import (
	"fmt"
)

var (
	errPrefix         = "expecting the prefix to be the \"urn\" string (whatever case) [col %d]"
	errIdentifier     = "expecting the identifier to be string (1..31 alnum chars, also containing dashes but not at its start) [col %d]"
	errSpecificString = "expecting the specific string to be a string containing alnum, hex, or others ([()+,-.:=@;$_!*']) chars [col %d]"
	errNoUrnWithinID  = "expecting the identifier to not contain the \"urn\" reserved string [col %d]"
	errHex            = "expecting the specific string hex chars to be well-formed (%%alnum{2}) [col %d]"
	errParse          = "parsing error [col %d]"
)

const start int = 1
const firstFinal int = 44

const enFail int = 46
const enMain int = 1

// Machine is the interface representing the FSM
type Machine interface {
	Error() error
	Parse(input []byte) (*URN, error)
}

type machine struct {
	data           []byte
	cs             int
	p, pe, eof, pb int
	err            error
	tolower        []int
}

// NewMachine creates a new FSM able to parse RFC 2141 strings.
func NewMachine() Machine {
	m := &machine{}

	return m
}

// Err returns the error that occurred on the last call to Parse.
//
// If the result is nil, then the line was parsed successfully.
func (m *machine) Error() error {
	return m.err
}

func (m *machine) text() []byte {
	return m.data[m.pb:m.p]
}

// Parse parses the input byte array as a RFC 2141 string.
func (m *machine) Parse(input []byte) (*URN, error) {
	m.data = input
	m.p = 0
	m.pb = 0
	m.pe = len(input)
	m.eof = len(input)
	m.err = nil
	m.tolower = []int{}
	output := &URN{}

	{
		m.cs = start
	}

	{
		if (m.p) == (m.pe) {
			goto _testEof
		}
		switch m.cs {
		case 1:
			goto stCase1
		case 0:
			goto stCase0
		case 2:
			goto stCase2
		case 3:
			goto stCase3
		case 4:
			goto stCase4
		case 5:
			goto stCase5
		case 6:
			goto stCase6
		case 7:
			goto stCase7
		case 8:
			goto stCase8
		case 9:
			goto stCase9
		case 10:
			goto stCase10
		case 11:
			goto stCase11
		case 12:
			goto stCase12
		case 13:
			goto stCase13
		case 14:
			goto stCase14
		case 15:
			goto stCase15
		case 16:
			goto stCase16
		case 17:
			goto stCase17
		case 18:
			goto stCase18
		case 19:
			goto stCase19
		case 20:
			goto stCase20
		case 21:
			goto stCase21
		case 22:
			goto stCase22
		case 23:
			goto stCase23
		case 24:
			goto stCase24
		case 25:
			goto stCase25
		case 26:
			goto stCase26
		case 27:
			goto stCase27
		case 28:
			goto stCase28
		case 29:
			goto stCase29
		case 30:
			goto stCase30
		case 31:
			goto stCase31
		case 32:
			goto stCase32
		case 33:
			goto stCase33
		case 34:
			goto stCase34
		case 35:
			goto stCase35
		case 36:
			goto stCase36
		case 37:
			goto stCase37
		case 38:
			goto stCase38
		case 44:
			goto stCase44
		case 39:
			goto stCase39
		case 40:
			goto stCase40
		case 45:
			goto stCase45
		case 41:
			goto stCase41
		case 42:
			goto stCase42
		case 43:
			goto stCase43
		case 46:
			goto stCase46
		}
		goto stOut
	stCase1:
		switch (m.data)[(m.p)] {
		case 85:
			goto tr1
		case 117:
			goto tr1
		}
		goto tr0
	tr0:

		m.err = fmt.Errorf(errParse, m.p)
		(m.p)--

		{
			goto st46
		}

		goto st0
	tr3:

		m.err = fmt.Errorf(errPrefix, m.p)
		(m.p)--

		{
			goto st46
		}

		m.err = fmt.Errorf(errParse, m.p)
		(m.p)--

		{
			goto st46
		}

		goto st0
	tr6:

		m.err = fmt.Errorf(errIdentifier, m.p)
		(m.p)--

		{
			goto st46
		}

		m.err = fmt.Errorf(errParse, m.p)
		(m.p)--

		{
			goto st46
		}

		goto st0
	tr41:

		m.err = fmt.Errorf(errSpecificString, m.p)
		(m.p)--

		{
			goto st46
		}

		m.err = fmt.Errorf(errParse, m.p)
		(m.p)--

		{
			goto st46
		}

		goto st0
	tr44:

		m.err = fmt.Errorf(errHex, m.p)
		(m.p)--

		{
			goto st46
		}

		m.err = fmt.Errorf(errSpecificString, m.p)
		(m.p)--

		{
			goto st46
		}

		m.err = fmt.Errorf(errParse, m.p)
		(m.p)--

		{
			goto st46
		}

		goto st0
	tr50:

		m.err = fmt.Errorf(errPrefix, m.p)
		(m.p)--

		{
			goto st46
		}

		m.err = fmt.Errorf(errIdentifier, m.p)
		(m.p)--

		{
			goto st46
		}

		m.err = fmt.Errorf(errParse, m.p)
		(m.p)--

		{
			goto st46
		}

		goto st0
	tr52:

		m.err = fmt.Errorf(errNoUrnWithinID, m.p)
		(m.p)--

		{
			goto st46
		}

		m.err = fmt.Errorf(errIdentifier, m.p)
		(m.p)--

		{
			goto st46
		}

		m.err = fmt.Errorf(errParse, m.p)
		(m.p)--

		{
			goto st46
		}

		goto st0
	stCase0:
	st0:
		m.cs = 0
		goto _out
	tr1:

		m.pb = m.p

		goto st2
	st2:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof2
		}
	stCase2:
		switch (m.data)[(m.p)] {
		case 82:
			goto st3
		case 114:
			goto st3
		}
		goto tr0
	st3:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof3
		}
	stCase3:
		switch (m.data)[(m.p)] {
		case 78:
			goto st4
		case 110:
			goto st4
		}
		goto tr3
	st4:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof4
		}
	stCase4:
		if (m.data)[(m.p)] == 58 {
			goto tr5
		}
		goto tr0
	tr5:

		output.prefix = string(m.text())

		goto st5
	st5:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof5
		}
	stCase5:
		switch (m.data)[(m.p)] {
		case 85:
			goto tr8
		case 117:
			goto tr8
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto tr7
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto tr7
			}
		default:
			goto tr7
		}
		goto tr6
	tr7:

		m.pb = m.p

		goto st6
	st6:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof6
		}
	stCase6:
		switch (m.data)[(m.p)] {
		case 45:
			goto st7
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st7
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st7
			}
		default:
			goto st7
		}
		goto tr6
	st7:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof7
		}
	stCase7:
		switch (m.data)[(m.p)] {
		case 45:
			goto st8
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st8
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st8
			}
		default:
			goto st8
		}
		goto tr6
	st8:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof8
		}
	stCase8:
		switch (m.data)[(m.p)] {
		case 45:
			goto st9
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st9
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st9
			}
		default:
			goto st9
		}
		goto tr6
	st9:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof9
		}
	stCase9:
		switch (m.data)[(m.p)] {
		case 45:
			goto st10
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st10
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st10
			}
		default:
			goto st10
		}
		goto tr6
	st10:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof10
		}
	stCase10:
		switch (m.data)[(m.p)] {
		case 45:
			goto st11
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st11
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st11
			}
		default:
			goto st11
		}
		goto tr6
	st11:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof11
		}
	stCase11:
		switch (m.data)[(m.p)] {
		case 45:
			goto st12
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st12
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st12
			}
		default:
			goto st12
		}
		goto tr6
	st12:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof12
		}
	stCase12:
		switch (m.data)[(m.p)] {
		case 45:
			goto st13
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st13
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st13
			}
		default:
			goto st13
		}
		goto tr6
	st13:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof13
		}
	stCase13:
		switch (m.data)[(m.p)] {
		case 45:
			goto st14
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st14
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st14
			}
		default:
			goto st14
		}
		goto tr6
	st14:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof14
		}
	stCase14:
		switch (m.data)[(m.p)] {
		case 45:
			goto st15
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st15
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st15
			}
		default:
			goto st15
		}
		goto tr6
	st15:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof15
		}
	stCase15:
		switch (m.data)[(m.p)] {
		case 45:
			goto st16
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st16
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st16
			}
		default:
			goto st16
		}
		goto tr6
	st16:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof16
		}
	stCase16:
		switch (m.data)[(m.p)] {
		case 45:
			goto st17
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st17
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st17
			}
		default:
			goto st17
		}
		goto tr6
	st17:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof17
		}
	stCase17:
		switch (m.data)[(m.p)] {
		case 45:
			goto st18
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st18
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st18
			}
		default:
			goto st18
		}
		goto tr6
	st18:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof18
		}
	stCase18:
		switch (m.data)[(m.p)] {
		case 45:
			goto st19
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st19
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st19
			}
		default:
			goto st19
		}
		goto tr6
	st19:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof19
		}
	stCase19:
		switch (m.data)[(m.p)] {
		case 45:
			goto st20
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st20
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st20
			}
		default:
			goto st20
		}
		goto tr6
	st20:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof20
		}
	stCase20:
		switch (m.data)[(m.p)] {
		case 45:
			goto st21
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st21
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st21
			}
		default:
			goto st21
		}
		goto tr6
	st21:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof21
		}
	stCase21:
		switch (m.data)[(m.p)] {
		case 45:
			goto st22
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st22
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st22
			}
		default:
			goto st22
		}
		goto tr6
	st22:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof22
		}
	stCase22:
		switch (m.data)[(m.p)] {
		case 45:
			goto st23
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st23
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st23
			}
		default:
			goto st23
		}
		goto tr6
	st23:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof23
		}
	stCase23:
		switch (m.data)[(m.p)] {
		case 45:
			goto st24
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st24
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st24
			}
		default:
			goto st24
		}
		goto tr6
	st24:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof24
		}
	stCase24:
		switch (m.data)[(m.p)] {
		case 45:
			goto st25
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st25
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st25
			}
		default:
			goto st25
		}
		goto tr6
	st25:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof25
		}
	stCase25:
		switch (m.data)[(m.p)] {
		case 45:
			goto st26
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st26
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st26
			}
		default:
			goto st26
		}
		goto tr6
	st26:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof26
		}
	stCase26:
		switch (m.data)[(m.p)] {
		case 45:
			goto st27
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st27
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st27
			}
		default:
			goto st27
		}
		goto tr6
	st27:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof27
		}
	stCase27:
		switch (m.data)[(m.p)] {
		case 45:
			goto st28
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st28
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st28
			}
		default:
			goto st28
		}
		goto tr6
	st28:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof28
		}
	stCase28:
		switch (m.data)[(m.p)] {
		case 45:
			goto st29
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st29
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st29
			}
		default:
			goto st29
		}
		goto tr6
	st29:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof29
		}
	stCase29:
		switch (m.data)[(m.p)] {
		case 45:
			goto st30
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st30
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st30
			}
		default:
			goto st30
		}
		goto tr6
	st30:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof30
		}
	stCase30:
		switch (m.data)[(m.p)] {
		case 45:
			goto st31
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st31
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st31
			}
		default:
			goto st31
		}
		goto tr6
	st31:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof31
		}
	stCase31:
		switch (m.data)[(m.p)] {
		case 45:
			goto st32
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st32
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st32
			}
		default:
			goto st32
		}
		goto tr6
	st32:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof32
		}
	stCase32:
		switch (m.data)[(m.p)] {
		case 45:
			goto st33
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st33
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st33
			}
		default:
			goto st33
		}
		goto tr6
	st33:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof33
		}
	stCase33:
		switch (m.data)[(m.p)] {
		case 45:
			goto st34
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st34
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st34
			}
		default:
			goto st34
		}
		goto tr6
	st34:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof34
		}
	stCase34:
		switch (m.data)[(m.p)] {
		case 45:
			goto st35
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st35
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st35
			}
		default:
			goto st35
		}
		goto tr6
	st35:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof35
		}
	stCase35:
		switch (m.data)[(m.p)] {
		case 45:
			goto st36
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st36
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st36
			}
		default:
			goto st36
		}
		goto tr6
	st36:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof36
		}
	stCase36:
		switch (m.data)[(m.p)] {
		case 45:
			goto st37
		case 58:
			goto tr10
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st37
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st37
			}
		default:
			goto st37
		}
		goto tr6
	st37:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof37
		}
	stCase37:
		if (m.data)[(m.p)] == 58 {
			goto tr10
		}
		goto tr6
	tr10:

		output.ID = string(m.text())

		goto st38
	st38:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof38
		}
	stCase38:
		switch (m.data)[(m.p)] {
		case 33:
			goto tr42
		case 36:
			goto tr42
		case 37:
			goto tr43
		case 61:
			goto tr42
		case 95:
			goto tr42
		}
		switch {
		case (m.data)[(m.p)] < 48:
			if 39 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 46 {
				goto tr42
			}
		case (m.data)[(m.p)] > 59:
			switch {
			case (m.data)[(m.p)] > 90:
				if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
					goto tr42
				}
			case (m.data)[(m.p)] >= 64:
				goto tr42
			}
		default:
			goto tr42
		}
		goto tr41
	tr42:

		m.pb = m.p

		goto st44
	st44:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof44
		}
	stCase44:
		switch (m.data)[(m.p)] {
		case 33:
			goto st44
		case 36:
			goto st44
		case 37:
			goto st39
		case 61:
			goto st44
		case 95:
			goto st44
		}
		switch {
		case (m.data)[(m.p)] < 48:
			if 39 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 46 {
				goto st44
			}
		case (m.data)[(m.p)] > 59:
			switch {
			case (m.data)[(m.p)] > 90:
				if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
					goto st44
				}
			case (m.data)[(m.p)] >= 64:
				goto st44
			}
		default:
			goto st44
		}
		goto tr41
	tr43:

		m.pb = m.p

		goto st39
	st39:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof39
		}
	stCase39:
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st40
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st40
			}
		default:
			goto tr46
		}
		goto tr44
	tr46:

		m.tolower = append(m.tolower, m.p-m.pb)

		goto st40
	st40:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof40
		}
	stCase40:
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st45
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st45
			}
		default:
			goto tr48
		}
		goto tr44
	tr48:

		m.tolower = append(m.tolower, m.p-m.pb)

		goto st45
	st45:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof45
		}
	stCase45:
		switch (m.data)[(m.p)] {
		case 33:
			goto st44
		case 36:
			goto st44
		case 37:
			goto st39
		case 61:
			goto st44
		case 95:
			goto st44
		}
		switch {
		case (m.data)[(m.p)] < 48:
			if 39 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 46 {
				goto st44
			}
		case (m.data)[(m.p)] > 59:
			switch {
			case (m.data)[(m.p)] > 90:
				if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
					goto st44
				}
			case (m.data)[(m.p)] >= 64:
				goto st44
			}
		default:
			goto st44
		}
		goto tr44
	tr8:

		m.pb = m.p

		goto st41
	st41:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof41
		}
	stCase41:
		switch (m.data)[(m.p)] {
		case 45:
			goto st7
		case 58:
			goto tr10
		case 82:
			goto st42
		case 114:
			goto st42
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st7
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st7
			}
		default:
			goto st7
		}
		goto tr6
	st42:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof42
		}
	stCase42:
		switch (m.data)[(m.p)] {
		case 45:
			goto st8
		case 58:
			goto tr10
		case 78:
			goto st43
		case 110:
			goto st43
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st8
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st8
			}
		default:
			goto st8
		}
		goto tr50
	st43:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof43
		}
	stCase43:
		if (m.data)[(m.p)] == 45 {
			goto st9
		}
		switch {
		case (m.data)[(m.p)] < 65:
			if 48 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 57 {
				goto st9
			}
		case (m.data)[(m.p)] > 90:
			if 97 <= (m.data)[(m.p)] && (m.data)[(m.p)] <= 122 {
				goto st9
			}
		default:
			goto st9
		}
		goto tr52
	st46:
		if (m.p)++; (m.p) == (m.pe) {
			goto _testEof46
		}
	stCase46:
		switch (m.data)[(m.p)] {
		case 10:
			goto st0
		case 13:
			goto st0
		}
		goto st46
	stOut:
	_testEof2:
		m.cs = 2
		goto _testEof
	_testEof3:
		m.cs = 3
		goto _testEof
	_testEof4:
		m.cs = 4
		goto _testEof
	_testEof5:
		m.cs = 5
		goto _testEof
	_testEof6:
		m.cs = 6
		goto _testEof
	_testEof7:
		m.cs = 7
		goto _testEof
	_testEof8:
		m.cs = 8
		goto _testEof
	_testEof9:
		m.cs = 9
		goto _testEof
	_testEof10:
		m.cs = 10
		goto _testEof
	_testEof11:
		m.cs = 11
		goto _testEof
	_testEof12:
		m.cs = 12
		goto _testEof
	_testEof13:
		m.cs = 13
		goto _testEof
	_testEof14:
		m.cs = 14
		goto _testEof
	_testEof15:
		m.cs = 15
		goto _testEof
	_testEof16:
		m.cs = 16
		goto _testEof
	_testEof17:
		m.cs = 17
		goto _testEof
	_testEof18:
		m.cs = 18
		goto _testEof
	_testEof19:
		m.cs = 19
		goto _testEof
	_testEof20:
		m.cs = 20
		goto _testEof
	_testEof21:
		m.cs = 21
		goto _testEof
	_testEof22:
		m.cs = 22
		goto _testEof
	_testEof23:
		m.cs = 23
		goto _testEof
	_testEof24:
		m.cs = 24
		goto _testEof
	_testEof25:
		m.cs = 25
		goto _testEof
	_testEof26:
		m.cs = 26
		goto _testEof
	_testEof27:
		m.cs = 27
		goto _testEof
	_testEof28:
		m.cs = 28
		goto _testEof
	_testEof29:
		m.cs = 29
		goto _testEof
	_testEof30:
		m.cs = 30
		goto _testEof
	_testEof31:
		m.cs = 31
		goto _testEof
	_testEof32:
		m.cs = 32
		goto _testEof
	_testEof33:
		m.cs = 33
		goto _testEof
	_testEof34:
		m.cs = 34
		goto _testEof
	_testEof35:
		m.cs = 35
		goto _testEof
	_testEof36:
		m.cs = 36
		goto _testEof
	_testEof37:
		m.cs = 37
		goto _testEof
	_testEof38:
		m.cs = 38
		goto _testEof
	_testEof44:
		m.cs = 44
		goto _testEof
	_testEof39:
		m.cs = 39
		goto _testEof
	_testEof40:
		m.cs = 40
		goto _testEof
	_testEof45:
		m.cs = 45
		goto _testEof
	_testEof41:
		m.cs = 41
		goto _testEof
	_testEof42:
		m.cs = 42
		goto _testEof
	_testEof43:
		m.cs = 43
		goto _testEof
	_testEof46:
		m.cs = 46
		goto _testEof

	_testEof:
		{
		}
		if (m.p) == (m.eof) {
			switch m.cs {
			case 44, 45:

				raw := m.text()
				output.SS = string(raw)
				// Iterate upper letters lowering them
				for _, i := range m.tolower {
					raw[i] = raw[i] + 32
				}
				output.norm = string(raw)

			case 1, 2, 4:

				m.err = fmt.Errorf(errParse, m.p)
				(m.p)--

				{
					goto st46
				}

			case 3:

				m.err = fmt.Errorf(errPrefix, m.p)
				(m.p)--

				{
					goto st46
				}

				m.err = fmt.Errorf(errParse, m.p)
				(m.p)--

				{
					goto st46
				}

			case 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 41:

				m.err = fmt.Errorf(errIdentifier, m.p)
				(m.p)--

				{
					goto st46
				}

				m.err = fmt.Errorf(errParse, m.p)
				(m.p)--

				{
					goto st46
				}

			case 38:

				m.err = fmt.Errorf(errSpecificString, m.p)
				(m.p)--

				{
					goto st46
				}

				m.err = fmt.Errorf(errParse, m.p)
				(m.p)--

				{
					goto st46
				}

			case 42:

				m.err = fmt.Errorf(errPrefix, m.p)
				(m.p)--

				{
					goto st46
				}

				m.err = fmt.Errorf(errIdentifier, m.p)
				(m.p)--

				{
					goto st46
				}

				m.err = fmt.Errorf(errParse, m.p)
				(m.p)--

				{
					goto st46
				}

			case 43:

				m.err = fmt.Errorf(errNoUrnWithinID, m.p)
				(m.p)--

				{
					goto st46
				}

				m.err = fmt.Errorf(errIdentifier, m.p)
				(m.p)--

				{
					goto st46
				}

				m.err = fmt.Errorf(errParse, m.p)
				(m.p)--

				{
					goto st46
				}

			case 39, 40:

				m.err = fmt.Errorf(errHex, m.p)
				(m.p)--

				{
					goto st46
				}

				m.err = fmt.Errorf(errSpecificString, m.p)
				(m.p)--

				{
					goto st46
				}

				m.err = fmt.Errorf(errParse, m.p)
				(m.p)--

				{
					goto st46
				}

			}
		}

	_out:
		{
		}
	}

	if m.cs < firstFinal || m.cs == enFail {
		return nil, m.err
	}

	return output, nil
}
