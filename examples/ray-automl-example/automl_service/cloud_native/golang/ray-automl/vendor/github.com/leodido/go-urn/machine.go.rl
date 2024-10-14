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

%%{
machine urn;

# unsigned alphabet
alphtype uint8;

action mark {
    m.pb = m.p
}

action tolower {
    m.tolower = append(m.tolower, m.p - m.pb)
}

action set_pre {
    output.prefix = string(m.text())
}

action set_nid {
    output.ID = string(m.text())
}

action set_nss {
    raw := m.text()
    output.SS = string(raw)
    // Iterate upper letters lowering them
    for _, i := range m.tolower {
        raw[i] = raw[i] + 32
    }
    output.norm = string(raw)
}

action err_pre {
    m.err = fmt.Errorf(errPrefix, m.p)
    fhold;
    fgoto fail;
}

action err_nid {
    m.err = fmt.Errorf(errIdentifier, m.p)
    fhold;
    fgoto fail;
}

action err_nss {
    m.err = fmt.Errorf(errSpecificString, m.p)
    fhold;
    fgoto fail;
}

action err_urn {
    m.err = fmt.Errorf(errNoUrnWithinID, m.p)
    fhold;
    fgoto fail;
}

action err_hex {
    m.err = fmt.Errorf(errHex, m.p)
    fhold;
    fgoto fail;
}

action err_parse {
    m.err = fmt.Errorf(errParse, m.p)
    fhold;
    fgoto fail;
}

pre = ([uU][rR][nN] @err(err_pre)) >mark %set_pre;

nid = (alnum >mark (alnum | '-'){0,31}) %set_nid;

hex = '%' (digit | lower | upper >tolower){2} $err(err_hex);

sss = (alnum | [()+,\-.:=@;$_!*']);

nss = (sss | hex)+ $err(err_nss);

fail := (any - [\n\r])* @err{ fgoto main; };

main := (pre ':' (nid - pre %err(err_urn)) $err(err_nid) ':' nss >mark %set_nss) $err(err_parse);

}%%

%% write data noerror noprefix;

// Machine is the interface representing the FSM
type Machine interface {
    Error() error
    Parse(input []byte) (*URN, error)
}

type machine struct {
    data              []byte
    cs                int
    p, pe, eof, pb    int
    err               error
    tolower           []int
}

// NewMachine creates a new FSM able to parse RFC 2141 strings.
func NewMachine() Machine {
    m := &machine{}

    %% access m.;
    %% variable p m.p;
    %% variable pe m.pe;
    %% variable eof m.eof;
    %% variable data m.data;

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

    %% write init;
    %% write exec;

    if m.cs < first_final || m.cs == en_fail {
        return nil, m.err
    }

    return output, nil
}
