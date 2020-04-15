package pivot

import (
	"fmt"
	"math"
	"regexp"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	LogicXor = Function{nil, 2, 2, "XOR", func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(ConvertToBool(a) != ConvertToBool(b))
		return e, nil
	}}
	LogicAnd = Function{nil, 2, 2, "AND", func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(ConvertToBool(a) && ConvertToBool(b))
		return e, nil
	}}
	LogicOr = Function{nil, 2, 2, "OR", func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(ConvertToBool(a) || ConvertToBool(b))
		return e, nil
	}}

	Gt = Function{nil, 2, 2, "GT", func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) > 0)
		return e, nil
	}}
	Lt = Function{nil, 2, 2, "LT", func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) < 0)
		return e, nil
	}}
	Ne = Function{nil, 2, 2, "NE", func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) != 0)
		return e, nil
	}}
	Eq = Function{nil, 2, 2, "EQ", func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) == 0)
		return e, nil
	}}
	Ge = Function{nil, 2, 2, "GE", func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) >= 0)
		return e, nil
	}}
	Le = Function{nil, 2, 2, "LE", func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) <= 0)
		return e, nil
	}}
)

func init() {
	for _, f := range []*Function{&LogicXor, &LogicAnd, &LogicOr} {
		f.AcceptType = make([]map[int]int, 0)
		f.AcceptType = append(f.AcceptType, *f.NewAcceptTypeMap())
		f.AcceptType = append(f.AcceptType, *f.NewAcceptTypeMap())
	}

	for _, f := range []*Function{&Lt, &Gt, &Le, &Ge, &Ne, &Eq} {
		f.AcceptType = make([]map[int]int, 0)
		mArg := *f.NewAcceptTypeMap()
		mArg[DatetimeArg] = AnyArg ^ StringArg
		mArg[StringArg] = AnyArg ^ DatetimeArg
		f.AcceptType = append(f.AcceptType, mArg, mArg)
	}
}

func ConvertToBool(a parser_driver.ValueExpr) bool {
	switch a.Kind() {
	case types.KindNull:
		return false
	case types.KindInt64:
		return a.GetValue().(int64) != 0
	case types.KindUint64:
		return a.GetValue().(uint64) != 0
	case types.KindFloat32:
		return math.Abs(float64(a.GetValue().(float32))) >= 1
	case types.KindFloat64:
		return math.Abs(a.GetValue().(float64)) >= 1
	case types.KindString:
		s := a.GetValue().(string)
		match, _ := regexp.MatchString(`^\-{0,1}[1-9]+|^\-{0,1}0+[1-9]`, s)
		return match
	case types.KindMysqlDecimal:
		d := a.GetMysqlDecimal()
		return !d.IsZero()
	default:
		panic(fmt.Sprintf("unreachable kind: %d", a.Kind()))
	}
}

func compare(a, b parser_driver.ValueExpr) int {
	res, err := a.CompareDatum(&stmtctx.StatementContext{}, &b.Datum)
	zero := parser_driver.ValueExpr{}
	zero.SetInt64(0)
	if err != nil {
		switch a.Kind() {
		case types.KindFloat32, types.KindFloat64, types.KindInt64, types.KindUint64:
			switch b.Kind() {
			case types.KindString:
				if i, err := a.CompareDatum(&stmtctx.StatementContext{}, &zero.Datum); err == nil {
					return i
				}
			}
		case types.KindMysqlTime:
			switch b.Kind() {
			case types.KindString:
				if i, err := b.ConvertTo(&stmtctx.StatementContext{}, types.NewFieldType(mysql.TypeTimestamp)); err == nil {
					if r, err := a.CompareDatum(&stmtctx.StatementContext{}, &i); err == nil {
						return r
					}
				}
				if i, err := b.ConvertTo(&stmtctx.StatementContext{}, types.NewFieldType(mysql.TypeDate)); err == nil {
					if r, err := a.CompareDatum(&stmtctx.StatementContext{}, &i); err == nil {
						return r
					}
				}
				if i, err := b.ConvertTo(&stmtctx.StatementContext{}, types.NewFieldType(mysql.TypeFloat)); err == nil {
					if r, err := a.CompareDatum(&stmtctx.StatementContext{}, &i); err == nil {
						return r
					}
				}
				// TODO: if convert b to DATETIME failed, panic
				return 0
			}
		case types.KindString:
			switch b.Kind() {
			case types.KindFloat32, types.KindFloat64, types.KindInt64, types.KindUint64:
				if i, err := b.CompareDatum(&stmtctx.StatementContext{}, &zero.Datum); err == nil {
					return -i
				}
			case types.KindMysqlTime:
				if i, err := a.ConvertTo(&stmtctx.StatementContext{}, types.NewFieldType(mysql.TypeTimestamp)); err == nil {
					if r, err := b.CompareDatum(&stmtctx.StatementContext{}, &i); err == nil {
						return -r
					}
				}
				if i, err := a.ConvertTo(&stmtctx.StatementContext{}, types.NewFieldType(mysql.TypeDate)); err == nil {
					if r, err := b.CompareDatum(&stmtctx.StatementContext{}, &i); err == nil {
						return -r
					}
				}
				if i, err := a.ConvertTo(&stmtctx.StatementContext{}, types.NewFieldType(mysql.TypeFloat)); err == nil {
					if r, err := b.CompareDatum(&stmtctx.StatementContext{}, &i); err == nil {
						return -r
					}
				}
				// TODO: if convert b to DATETIME failed, panic
			}
		}
		panic(fmt.Sprintf("compare %v and %v err: %v", a, b, err))
	}
	return res
}
