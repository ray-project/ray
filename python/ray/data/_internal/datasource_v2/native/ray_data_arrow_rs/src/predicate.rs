//! Predicate IR for row-group statistics pruning (Track 4, part 1).
//!
//! The Python reader lowers the pushed-down Ray `Expr` predicate into the small
//! JSON IR parsed here (see `_predicate_to_ir` on the Python side), and this
//! module decides which row groups *cannot possibly* contain a matching row and
//! so may be skipped before any data is fetched or decoded. This replaces the
//! PyArrow `fragment.subset(filter=...)` pruning the reader used to depend on,
//! which is what let PyArrow stop opening supported files.
//!
//! SOUNDNESS CONTRACT
//! ------------------
//! [`can_match`] is *conservative*: it returns `true` (keep the row group)
//! unless the predicate is provably false for **every** row in the group given
//! its column statistics. Every source of uncertainty — a missing column, absent
//! statistics, a cross-type or NaN comparison it can't order, a `NOT`, or any op
//! it doesn't model — resolves to `true`. Over-pruning (dropping a group that
//! *could* have matched) is the only way stats pruning can silently lose rows, so
//! it is made impossible by construction: the worst a bug in here can do is keep
//! a group we could have skipped (a performance miss), never drop a live one.
//!
//! The Python reader additionally re-applies the full predicate post-decode, so
//! row-level correctness never rests on this module at all — this is purely an
//! IO/decode-avoidance optimization on top of an already-correct result.

use std::cmp::Ordering;
use std::collections::HashMap;

use serde::Deserialize;

/// A scalar literal or a statistic value, in the few types Parquet statistics
/// and pushed predicates actually use. Comparisons across numeric types promote
/// to f64; every other cross-type comparison is defined as incomparable
/// (`partial_cmp` -> `None`), which the pruning logic treats as "keep".
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "vt", content = "v", rename_all = "snake_case")]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

impl Value {
    /// Partial order used for pruning. Returns `None` (incomparable) for NaN,
    /// null operands, and any cross-type pair that isn't int/float — every such
    /// case makes the caller keep the row group.
    pub fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        // Integers with |v| above this lose precision when promoted to f64
        // (f64 has a 53-bit mantissa), so a mixed int/float comparison could
        // silently flip. Treat those as incomparable (None) instead — the
        // caller keeps the row group, which is always sound.
        const MAX_SAFE_INT: i64 = 9_007_199_254_740_991; // 2^53 - 1
        const MIN_SAFE_INT: i64 = -9_007_199_254_740_991;
        let in_safe_range = |v: i64| (MIN_SAFE_INT..=MAX_SAFE_INT).contains(&v);
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            // Mixed int/float: promote to f64 (e.g. `float_col > 5`), but only
            // when the integer is exactly representable in f64.
            (Value::Int(a), Value::Float(b)) if in_safe_range(*a) => {
                (*a as f64).partial_cmp(b)
            }
            (Value::Float(a), Value::Int(b)) if in_safe_range(*b) => {
                a.partial_cmp(&(*b as f64))
            }
            (Value::Int(_), Value::Float(_)) | (Value::Float(_), Value::Int(_)) => None,
            (Value::Str(a), Value::Str(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// Comparison operator in a `cmp` predicate atom.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CmpOp {
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
    Ne,
}

/// The predicate IR. `unknown` is the explicit catch-all the Python translator
/// emits for anything it can't lower; it always keeps the row group.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Pred {
    And {
        preds: Vec<Pred>,
    },
    Or {
        preds: Vec<Pred>,
    },
    // `can_match` treats any negation conservatively (keep) without inspecting
    // the child, so the field is unread today; retained for a future sound
    // NOT-pushdown and to keep the wire format stable.
    Not {
        #[allow(dead_code)]
        pred: Box<Pred>,
    },
    Cmp {
        col: String,
        op: CmpOp,
        value: Value,
    },
    IsNull {
        col: String,
    },
    IsNotNull {
        col: String,
    },
    In {
        col: String,
        values: Vec<Value>,
        negated: bool,
    },
    Unknown,
}

impl Pred {
    /// Parse the IR from the JSON string passed across the FFI boundary. On any
    /// parse error we fall back to the always-keep predicate so a malformed IR
    /// degrades to "no pruning", never to an error or a wrong result.
    pub fn from_json(s: &str) -> Pred {
        serde_json::from_str(s).unwrap_or(Pred::Unknown)
    }
}

/// Per-column statistics for one row group, in `Value` terms. `min`/`max` are
/// `None` when the column has no statistics (Parquet may omit them). `null_count`
/// is `None` when unknown. `num_rows` is the row group's row count.
#[derive(Debug, Clone)]
pub struct ColStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: Option<i64>,
    pub num_rows: i64,
}

impl ColStats {
    fn all_null(&self) -> bool {
        self.num_rows > 0 && self.null_count == Some(self.num_rows)
    }
}

/// True if the row group described by `stats` *could* contain a row satisfying
/// `pred`. Conservative: any uncertainty returns true. See the module contract.
pub fn can_match(pred: &Pred, stats: &HashMap<String, ColStats>) -> bool {
    match pred {
        // A AND B can match only if every conjunct can match: if no row can
        // satisfy one conjunct, none can satisfy the whole.
        Pred::And { preds } => preds.iter().all(|p| can_match(p, stats)),
        // A OR B can match if any disjunct can match.
        Pred::Or { preds } => preds.iter().any(|p| can_match(p, stats)),
        // Negation over ranges isn't soundly prunable from min/max alone; keep.
        Pred::Not { .. } => true,
        Pred::Unknown => true,
        Pred::Cmp { col, op, value } => match stats.get(col) {
            None => true, // column absent from this group's stats -> keep
            Some(cs) => cmp_can_match(cs, *op, value),
        },
        Pred::IsNull { col } => match stats.get(col) {
            None => true,
            // Keep unless we know there are zero nulls.
            Some(cs) => cs.null_count != Some(0),
        },
        Pred::IsNotNull { col } => match stats.get(col) {
            None => true,
            // Keep unless every row is null.
            Some(cs) => !cs.all_null(),
        },
        Pred::In {
            col,
            values,
            negated,
        } => {
            if *negated {
                return true; // NOT IN can't be pruned from min/max; keep.
            }
            match stats.get(col) {
                None => true,
                Some(cs) => {
                    if cs.all_null() {
                        return false; // all rows null -> none in the set
                    }
                    // Keep if any listed value could fall within [min, max].
                    values.iter().any(|v| value_in_range(cs, v))
                }
            }
        }
    }
}

/// Whether `v` is possibly within `[min, max]` (used for IN and EQ). Unknown
/// bounds or incomparable values -> possible (keep).
fn value_in_range(cs: &ColStats, v: &Value) -> bool {
    if v.is_null() {
        return true; // null-in-set semantics are subtle; stay conservative.
    }
    let below_min = match &cs.min {
        Some(mn) => matches!(v.partial_cmp(mn), Some(Ordering::Less)),
        None => false,
    };
    let above_max = match &cs.max {
        Some(mx) => matches!(v.partial_cmp(mx), Some(Ordering::Greater)),
        None => false,
    };
    !(below_min || above_max)
}

/// Row-group verdict for a single `col OP value` atom. Returns true to keep.
fn cmp_can_match(cs: &ColStats, op: CmpOp, v: &Value) -> bool {
    // A comparison against a null literal is null (=> false) for every row;
    // but this shape is unusual, so keep rather than reason about it.
    if v.is_null() {
        return true;
    }
    // Every row is null => every comparison is null => false for all => prune.
    if cs.all_null() {
        return false;
    }
    match op {
        // Keep iff some value can exceed / reach the bound.
        CmpOp::Gt => keep_if(&cs.max, v, |o| o == Ordering::Greater),
        CmpOp::Ge => keep_if(&cs.max, v, |o| o != Ordering::Less),
        CmpOp::Lt => keep_if(&cs.min, v, |o| o == Ordering::Less),
        CmpOp::Le => keep_if(&cs.min, v, |o| o != Ordering::Greater),
        // v in [min, max] is a necessary condition for equality to be possible.
        CmpOp::Eq => value_in_range(cs, v),
        // col != v is false for all only if the column is the constant v with no
        // nulls; that needs min==max==v && null_count==0. Otherwise keep.
        CmpOp::Ne => {
            let is_constant_v = matches!(
                (&cs.min, &cs.max),
                (Some(mn), Some(mx))
                    if matches!(mn.partial_cmp(v), Some(Ordering::Equal))
                        && matches!(mx.partial_cmp(v), Some(Ordering::Equal))
            );
            !(is_constant_v && cs.null_count == Some(0))
        }
    }
}

/// Keep the group iff `bound` is known and `pred(bound.cmp(v))` holds. An unknown
/// bound or an incomparable pair (`partial_cmp` -> None, e.g. NaN) keeps it.
fn keep_if(bound: &Option<Value>, v: &Value, pred: impl Fn(Ordering) -> bool) -> bool {
    match bound {
        None => true,
        Some(b) => match b.partial_cmp(v) {
            Some(ord) => pred(ord),
            None => true,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cs(min: i64, max: i64, nulls: i64, rows: i64) -> ColStats {
        ColStats {
            min: Some(Value::Int(min)),
            max: Some(Value::Int(max)),
            null_count: Some(nulls),
            num_rows: rows,
        }
    }

    fn stats(pairs: &[(&str, ColStats)]) -> HashMap<String, ColStats> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn cmp(col: &str, op: CmpOp, v: i64) -> Pred {
        Pred::Cmp {
            col: col.into(),
            op,
            value: Value::Int(v),
        }
    }

    // ---- the sorted-id / row-group example from the Python pruning test ----
    #[test]
    fn ge_prunes_groups_below_threshold() {
        // Four row groups of a sorted id column: [0,999] [1000,1999] ...
        let groups = [(0, 999), (1000, 1999), (2000, 2999), (3000, 3999)];
        let pred = cmp("id", CmpOp::Ge, 3000);
        let kept: Vec<usize> = groups
            .iter()
            .enumerate()
            .filter(|(_, (lo, hi))| can_match(&pred, &stats(&[("id", cs(*lo, *hi, 0, 1000))])))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            kept,
            vec![3],
            "only the [3000,3999] group can satisfy id>=3000"
        );
    }

    #[test]
    fn ge_prunes_everything_when_out_of_range() {
        let pred = cmp("id", CmpOp::Ge, 1_000_000_000);
        assert!(!can_match(&pred, &stats(&[("id", cs(0, 3999, 0, 4000))])));
    }

    // ---- per-op boundary behavior ----
    #[test]
    fn gt_boundary() {
        // max == v: no row is strictly greater -> prune.
        assert!(!can_match(
            &cmp("x", CmpOp::Gt, 10),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
        assert!(can_match(
            &cmp("x", CmpOp::Gt, 9),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
    }

    #[test]
    fn lt_le_use_min() {
        assert!(!can_match(
            &cmp("x", CmpOp::Lt, 0),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
        assert!(can_match(
            &cmp("x", CmpOp::Lt, 1),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
        assert!(!can_match(
            &cmp("x", CmpOp::Le, -1),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
        assert!(can_match(
            &cmp("x", CmpOp::Le, 0),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
    }

    #[test]
    fn eq_outside_range_prunes() {
        assert!(!can_match(
            &cmp("x", CmpOp::Eq, 11),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
        assert!(!can_match(
            &cmp("x", CmpOp::Eq, -1),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
        assert!(can_match(
            &cmp("x", CmpOp::Eq, 5),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
    }

    #[test]
    fn ne_only_prunes_constant_column() {
        // min==max==v, no nulls -> col != v false for all -> prune.
        assert!(!can_match(
            &cmp("x", CmpOp::Ne, 7),
            &stats(&[("x", cs(7, 7, 0, 3))])
        ));
        // a null present -> keep (null != v is null, but rows differ anyway keep)
        assert!(can_match(
            &cmp("x", CmpOp::Ne, 7),
            &stats(&[("x", cs(7, 7, 1, 3))])
        ));
        // range wider than {v} -> keep
        assert!(can_match(
            &cmp("x", CmpOp::Ne, 7),
            &stats(&[("x", cs(0, 10, 0, 3))])
        ));
    }

    // ---- conservative fallbacks: every uncertainty keeps ----
    #[test]
    fn missing_column_keeps() {
        assert!(can_match(
            &cmp("absent", CmpOp::Gt, 100),
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
    }

    #[test]
    fn unknown_bounds_keep() {
        let s = stats(&[(
            "x",
            ColStats {
                min: None,
                max: None,
                null_count: None,
                num_rows: 5,
            },
        )]);
        assert!(can_match(&cmp("x", CmpOp::Gt, 10_000), &s));
        assert!(can_match(&cmp("x", CmpOp::Eq, 10_000), &s));
    }

    #[test]
    fn cross_type_compare_keeps() {
        // string literal against an int column -> incomparable -> keep.
        let pred = Pred::Cmp {
            col: "x".into(),
            op: CmpOp::Gt,
            value: Value::Str("abc".into()),
        };
        assert!(can_match(&pred, &stats(&[("x", cs(0, 10, 0, 5))])));
    }

    #[test]
    fn nan_bounds_keep() {
        let s = stats(&[(
            "f",
            ColStats {
                min: Some(Value::Float(f64::NAN)),
                max: Some(Value::Float(f64::NAN)),
                null_count: Some(0),
                num_rows: 5,
            },
        )]);
        let pred = Pred::Cmp {
            col: "f".into(),
            op: CmpOp::Gt,
            value: Value::Float(1.0),
        };
        assert!(can_match(&pred, &s));
    }

    #[test]
    fn int_float_promotion() {
        // float column, int literal.
        let s = stats(&[(
            "f",
            ColStats {
                min: Some(Value::Float(0.0)),
                max: Some(Value::Float(2.5)),
                null_count: Some(0),
                num_rows: 5,
            },
        )]);
        assert!(!can_match(
            &Pred::Cmp {
                col: "f".into(),
                op: CmpOp::Gt,
                value: Value::Int(3)
            },
            &s
        ));
        assert!(can_match(
            &Pred::Cmp {
                col: "f".into(),
                op: CmpOp::Gt,
                value: Value::Int(2)
            },
            &s
        ));
    }

    // ---- null-count driven ----
    #[test]
    fn all_null_group_prunes_comparisons_and_in() {
        let s = stats(&[("x", cs(0, 0, 5, 5))]); // all 5 rows null
        assert!(!can_match(&cmp("x", CmpOp::Gt, -100), &s));
        assert!(!can_match(&cmp("x", CmpOp::Eq, 0), &s));
        assert!(!can_match(
            &Pred::In {
                col: "x".into(),
                values: vec![Value::Int(0)],
                negated: false
            },
            &s
        ));
    }

    #[test]
    fn is_null_and_is_not_null() {
        // no nulls
        assert!(!can_match(
            &Pred::IsNull { col: "x".into() },
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
        assert!(can_match(
            &Pred::IsNotNull { col: "x".into() },
            &stats(&[("x", cs(0, 10, 0, 5))])
        ));
        // all null
        assert!(can_match(
            &Pred::IsNull { col: "x".into() },
            &stats(&[("x", cs(0, 0, 5, 5))])
        ));
        assert!(!can_match(
            &Pred::IsNotNull { col: "x".into() },
            &stats(&[("x", cs(0, 0, 5, 5))])
        ));
        // some null
        assert!(can_match(
            &Pred::IsNull { col: "x".into() },
            &stats(&[("x", cs(0, 10, 2, 5))])
        ));
        assert!(can_match(
            &Pred::IsNotNull { col: "x".into() },
            &stats(&[("x", cs(0, 10, 2, 5))])
        ));
    }

    // ---- IN ----
    #[test]
    fn in_prunes_when_no_value_in_range() {
        let s = stats(&[("x", cs(0, 10, 0, 5))]);
        assert!(!can_match(
            &Pred::In {
                col: "x".into(),
                values: vec![Value::Int(20), Value::Int(30)],
                negated: false
            },
            &s
        ));
        assert!(can_match(
            &Pred::In {
                col: "x".into(),
                values: vec![Value::Int(20), Value::Int(5)],
                negated: false
            },
            &s
        ));
        // NOT IN never prunes.
        assert!(can_match(
            &Pred::In {
                col: "x".into(),
                values: vec![Value::Int(20)],
                negated: true
            },
            &s
        ));
    }

    // ---- boolean composition ----
    #[test]
    fn and_prunes_if_any_conjunct_prunes() {
        let s = stats(&[("x", cs(0, 10, 0, 5)), ("y", cs(0, 10, 0, 5))]);
        // x > 5 (possible) AND y > 100 (impossible) -> prune
        let p = Pred::And {
            preds: vec![cmp("x", CmpOp::Gt, 5), cmp("y", CmpOp::Gt, 100)],
        };
        assert!(!can_match(&p, &s));
        // x > 5 AND y > 5 -> both possible -> keep
        let p2 = Pred::And {
            preds: vec![cmp("x", CmpOp::Gt, 5), cmp("y", CmpOp::Gt, 5)],
        };
        assert!(can_match(&p2, &s));
    }

    #[test]
    fn or_keeps_if_any_disjunct_possible() {
        let s = stats(&[("x", cs(0, 10, 0, 5))]);
        // x > 100 (impossible) OR x < 5 (possible) -> keep
        let p = Pred::Or {
            preds: vec![cmp("x", CmpOp::Gt, 100), cmp("x", CmpOp::Lt, 5)],
        };
        assert!(can_match(&p, &s));
        // x > 100 OR x > 200 -> both impossible -> prune
        let p2 = Pred::Or {
            preds: vec![cmp("x", CmpOp::Gt, 100), cmp("x", CmpOp::Gt, 200)],
        };
        assert!(!can_match(&p2, &s));
    }

    #[test]
    fn not_and_unknown_keep() {
        let s = stats(&[("x", cs(0, 10, 0, 5))]);
        assert!(can_match(
            &Pred::Not {
                pred: Box::new(cmp("x", CmpOp::Gt, 100))
            },
            &s
        ));
        assert!(can_match(&Pred::Unknown, &s));
    }

    // ---- JSON round-trip of the wire format ----
    #[test]
    fn parses_wire_json() {
        let j = r#"{"t":"and","preds":[
            {"t":"cmp","col":"id","op":"ge","value":{"vt":"int","v":3000}},
            {"t":"cmp","col":"x","op":"lt","value":{"vt":"float","v":1.5}},
            {"t":"is_not_null","col":"id"},
            {"t":"in","col":"g","values":[{"vt":"str","v":"a"}],"negated":false}
        ]}"#;
        let p = Pred::from_json(j);
        // sanity: with id in [0,999] the id>=3000 conjunct prunes.
        let s = stats(&[
            ("id", cs(0, 999, 0, 1000)),
            ("x", cs(0, 0, 0, 1000)),
            ("g", cs(0, 0, 0, 1000)),
        ]);
        assert!(!can_match(&p, &s));
    }

    #[test]
    fn malformed_json_becomes_keep_all() {
        assert!(matches!(Pred::from_json("not json"), Pred::Unknown));
        assert!(matches!(Pred::from_json(r#"{"t":"bogus"}"#), Pred::Unknown));
    }
}
