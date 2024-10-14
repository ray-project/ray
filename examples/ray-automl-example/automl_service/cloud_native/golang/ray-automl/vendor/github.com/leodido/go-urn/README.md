[![Build](https://img.shields.io/travis/leodido/go-urn/master.svg?style=for-the-badge)](https://travis-ci.org/leodido/go-urn) [![Coverage](https://img.shields.io/codecov/c/github/leodido/go-urn.svg?style=for-the-badge)](https://codecov.io/gh/leodido/go-urn) [![Documentation](https://img.shields.io/badge/godoc-reference-blue.svg?style=for-the-badge)](https://godoc.org/github.com/leodido/go-urn)

**A parser for URNs**.

> As seen on [RFC 2141](https://tools.ietf.org/html/rfc2141#ref-1).

[API documentation](https://godoc.org/github.com/leodido/go-urn).

## Installation

```
go get github.com/leodido/go-urn
```

## Performances

This implementation results to be really fast.

Usually below ½ microsecond on my machine<sup>[1](#mymachine)</sup>.

Notice it also performs, while parsing:

1. fine-grained and informative erroring
2. specific-string normalization

```
ok/00/urn:a:b______________________________________/-4      20000000      265 ns/op      182 B/op      6 allocs/op
ok/01/URN:foo:a123,456_____________________________/-4      30000000      296 ns/op      200 B/op      6 allocs/op
ok/02/urn:foo:a123%2c456___________________________/-4      20000000      331 ns/op      208 B/op      6 allocs/op
ok/03/urn:ietf:params:scim:schemas:core:2.0:User___/-4      20000000      430 ns/op      280 B/op      6 allocs/op
ok/04/urn:ietf:params:scim:schemas:extension:enterp/-4      20000000      411 ns/op      312 B/op      6 allocs/op
ok/05/urn:ietf:params:scim:schemas:extension:enterp/-4      20000000      472 ns/op      344 B/op      6 allocs/op
ok/06/urn:burnout:nss______________________________/-4      30000000      257 ns/op      192 B/op      6 allocs/op
ok/07/urn:abcdefghilmnopqrstuvzabcdefghilm:x_______/-4      20000000      375 ns/op      213 B/op      6 allocs/op
ok/08/urn:urnurnurn:urn____________________________/-4      30000000      265 ns/op      197 B/op      6 allocs/op
ok/09/urn:ciao:@!=%2c(xyz)+a,b.*@g=$_'_____________/-4      20000000      307 ns/op      248 B/op      6 allocs/op
ok/10/URN:x:abc%1dz%2f%3az_________________________/-4      30000000      259 ns/op      212 B/op      6 allocs/op
no/11/URN:-xxx:x___________________________________/-4      20000000      445 ns/op      320 B/op      6 allocs/op
no/12/urn::colon:nss_______________________________/-4      20000000      461 ns/op      320 B/op      6 allocs/op
no/13/urn:abcdefghilmnopqrstuvzabcdefghilmn:specifi/-4      10000000      660 ns/op      320 B/op      6 allocs/op
no/14/URN:a!?:x____________________________________/-4      20000000      507 ns/op      320 B/op      6 allocs/op
no/15/urn:urn:NSS__________________________________/-4      20000000      429 ns/op      288 B/op      6 allocs/op
no/16/urn:white_space:NSS__________________________/-4      20000000      482 ns/op      320 B/op      6 allocs/op
no/17/urn:concat:no_spaces_________________________/-4      20000000      539 ns/op      328 B/op      7 allocs/op
no/18/urn:a:/______________________________________/-4      20000000      470 ns/op      320 B/op      7 allocs/op
no/19/urn:UrN:NSS__________________________________/-4      20000000      399 ns/op      288 B/op      6 allocs/op
```

---

* <a name="mymachine">[1]</a>: Intel Core i7-7600U CPU @ 2.80GHz

---

[![Analytics](https://ga-beacon.appspot.com/UA-49657176-1/go-urn?flat)](https://github.com/igrigorik/ga-beacon)