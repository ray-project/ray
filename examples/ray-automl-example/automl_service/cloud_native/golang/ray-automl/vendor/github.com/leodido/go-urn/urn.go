package urn

import (
	"encoding/json"
	"fmt"
	"strings"
)

const errInvalidURN = "invalid URN: %s"

// URN represents an Uniform Resource Name.
//
// The general form represented is:
//
//	urn:<id>:<ss>
//
// Details at https://tools.ietf.org/html/rfc2141.
type URN struct {
	prefix string // Static prefix. Equal to "urn" when empty.
	ID     string // Namespace identifier
	SS     string // Namespace specific string
	norm   string // Normalized namespace specific string
}

// Normalize turns the receiving URN into its norm version.
//
// Which means: lowercase prefix, lowercase namespace identifier, and immutate namespace specific string chars (except <hex> tokens which are lowercased).
func (u *URN) Normalize() *URN {
	return &URN{
		prefix: "urn",
		ID:     strings.ToLower(u.ID),
		SS:     u.norm,
	}
}

// Equal checks the lexical equivalence of the current URN with another one.
func (u *URN) Equal(x *URN) bool {
	return *u.Normalize() == *x.Normalize()
}

// String reassembles the URN into a valid URN string.
//
// This requires both ID and SS fields to be non-empty.
// Otherwise it returns an empty string.
//
// Default URN prefix is "urn".
func (u *URN) String() string {
	var res string
	if u.ID != "" && u.SS != "" {
		if u.prefix == "" {
			res += "urn"
		}
		res += u.prefix + ":" + u.ID + ":" + u.SS
	}

	return res
}

// Parse is responsible to create an URN instance from a byte array matching the correct URN syntax.
func Parse(u []byte) (*URN, bool) {
	urn, err := NewMachine().Parse(u)
	if err != nil {
		return nil, false
	}

	return urn, true
}

// MarshalJSON marshals the URN to JSON string form (e.g. `"urn:oid:1.2.3.4"`).
func (u URN) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// MarshalJSON unmarshals a URN from JSON string form (e.g. `"urn:oid:1.2.3.4"`).
func (u *URN) UnmarshalJSON(bytes []byte) error {
	var str string
	if err := json.Unmarshal(bytes, &str); err != nil {
		return err
	}
	if value, ok := Parse([]byte(str)); !ok {
		return fmt.Errorf(errInvalidURN, str)
	} else {
		*u = *value
	}
	return nil
}