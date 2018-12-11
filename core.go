package dbtesting

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/forsaken628/bsql"
	"github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

type ScanType struct {
	reflect.Type
}

func (t ScanType) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

func (t *ScanType) UnmarshalJSON(data []byte) error {
	typs := []reflect.Type{
		reflect.TypeOf(""),
		reflect.TypeOf(sql.NullString{}),

		reflect.TypeOf(int(0)),
		reflect.TypeOf(int8(0)),
		reflect.TypeOf(int16(0)),
		reflect.TypeOf(int32(0)),
		reflect.TypeOf(int64(0)),
		reflect.TypeOf(uint(0)),
		reflect.TypeOf(uint8(0)),
		reflect.TypeOf(uint16(0)),
		reflect.TypeOf(uint32(0)),
		reflect.TypeOf(uint64(0)),
		reflect.TypeOf(sql.NullInt64{}),

		reflect.TypeOf(float32(0)),
		reflect.TypeOf(float64(0)),
		reflect.TypeOf(sql.NullFloat64{}),

		reflect.TypeOf(time.Time{}),
		reflect.TypeOf(mysql.NullTime{}),

		reflect.TypeOf(sql.RawBytes{}),
	}

	for _, v := range typs {
		if string(data) == `"`+v.String()+`"` {
			t.Type = v
			return nil
		}
	}

	return errors.New("unsupported value: " + string(data))
}

type ColType struct {
	fullDatabaseType string

	name string

	hasNullable       bool
	hasLength         bool
	hasPrecisionScale bool

	nullable     bool
	length       int64
	databaseType string
	precision    int64
	scale        int64
	scanType     reflect.Type
}

func CompareColType(expect, actual *ColType) bool {
	if expect.name != actual.name {
		return false
	}
	if expect.databaseType != actual.databaseType {
		return false
	}
	if expect.scanType != actual.scanType {
		return false
	}
	return true
}

func (c ColType) MarshalJSON() ([]byte, error) {
	return json.Marshal(col4json{
		FullDatabaseType:  c.fullDatabaseType,
		Name:              c.name,
		HasNullable:       c.hasNullable,
		HasLength:         c.hasLength,
		HasPrecisionScale: c.hasPrecisionScale,
		Nullable:          c.nullable,
		Length:            c.length,
		DatabaseType:      c.databaseType,
		Precision:         c.precision,
		Scale:             c.scale,
		ScanType:          ScanType{c.scanType},
	})
}

func (c *ColType) UnmarshalJSON(data []byte) error {
	cc := col4json{}
	err := json.Unmarshal(data, &cc)
	if err != nil {
		return err
	}

	*c = ColType{
		fullDatabaseType:  cc.FullDatabaseType,
		name:              cc.Name,
		hasNullable:       cc.HasNullable,
		hasLength:         cc.HasLength,
		hasPrecisionScale: cc.HasPrecisionScale,
		nullable:          cc.Nullable,
		length:            cc.Length,
		databaseType:      cc.DatabaseType,
		precision:         cc.Precision,
		scale:             cc.Scale,
		scanType:          cc.ScanType.Type,
	}

	return nil
}

type col4json struct {
	FullDatabaseType string

	Name string

	HasNullable       bool
	HasLength         bool
	HasPrecisionScale bool

	Nullable     bool
	Length       int64
	DatabaseType string
	Precision    int64
	Scale        int64
	ScanType     ScanType
}

func NewColType(cTyp *sql.ColumnType) *ColType {
	c := &ColType{}
	c.name = cTyp.Name()
	c.databaseType = cTyp.DatabaseTypeName()
	c.length, c.hasLength = cTyp.Length()
	c.precision, c.scale, c.hasPrecisionScale = cTyp.DecimalSize()
	c.nullable, c.hasNullable = cTyp.Nullable()

	switch c.databaseType {
	case "CHAR", "VARCHAR", "TEXT", "DECIMAL":
		if c.nullable {
			c.scanType = reflect.TypeOf(sql.NullString{})
		} else {
			c.scanType = reflect.TypeOf("")
		}
	case "TIMESTAMP", "DATETIME":
		if c.nullable {
			c.scanType = reflect.TypeOf(mysql.NullTime{})
		} else {
			c.scanType = reflect.TypeOf(time.Time{})
		}
	default:
		c.scanType = cTyp.ScanType()
	}

	return c
}

type ResultType struct {
	name    string
	isTable bool
	colType []*ColType
}

func CompareResultType(expect, actual *ResultType) (string, bool) {
	if len(expect.colType) != len(actual.colType) {
		return fmt.Sprintf("%s has %d columns,but %s has %d columns", expect.name, len(expect.colType), actual.name, len(actual.colType)), false
	}

	for i, v := range expect.colType {
		if !CompareColType(v, actual.colType[i]) {
			return fmt.Sprintf("at index %d, %s different from %s", i, v.name, v.name), false
		}
	}

	return "", true
}

type Row struct {
	ResultType
	data []interface{}
}

type Result struct {
	ResultType
	query *Query
	data  [][]interface{}
}

func CompareResult(expect, actual *Result) (string, bool) {
	diff, same := CompareResultType(&expect.ResultType, &actual.ResultType)
	if !same {
		return diff, false
	}

	if len(expect.data) != len(actual.data) {
		return "check result fail: len(rows)", false
	}

	for i, row := range expect.data {
		diff, same := CompareRow(row, actual.data[i], expect.ResultType.colType, expect.query.comparators)
		if !same {
			return fmt.Sprintf("check result fail, row: %v\n%s", row, diff), false
		}
	}
	return "", true
}

func CompareRow(expect, actual []interface{}, colType []*ColType, m map[string]func(expect, actual interface{}) (string, bool)) (string, bool) {
	for j, val := range expect {

		fn, ok := m[colType[j].name]
		if ok {
			diff, same := fn(val, actual[j])
			if !same {
				return fmt.Sprintf("check row fail, col: %s, %s", colType[j].name, diff), false
			}
			continue
		}

		switch colType[j].scanType {
		default:
			if !(val == actual[j]) {
				return fmt.Sprintf("check row fail, col: %s, expect: %v, actual: %v", colType[j].name, val, actual[j]), false
			}
		case reflect.TypeOf(time.Time{}), reflect.TypeOf(sql.RawBytes{}):
			panic("todo")
		}
	}
	return "", true
}

func (r *Result) Len() int {
	return len(r.data)
}

func (r *Result) Index(i int) *Row {
	return &Row{
		ResultType: r.ResultType,
		data:       r.data[i],
	}
}

func (r *Result) Apply(db *sql.DB) error {
	if !r.isTable {
		return errors.New("not a table")
	}

	_, err := db.Exec("truncate " + r.name)
	if err != nil {
		return err
	}

	cols := make([]string, len(r.colType))
	for i, v := range r.colType {
		cols[i] = v.name
	}

	d := r.data
	buf := make([][]interface{}, 1000)
	for len(d) > 0 {
		n := copy(buf, d)
		d = d[n:]

		values, err := bsql.MakeValues(cols, buf[:n])
		if err != nil {
			return err
		}

		q, a := bsql.Insert{
			Table: bsql.Raw(r.name),
			Value: values,
		}.Build()

		_, err = db.Exec(q, a...)
		if err != nil {
			return err
		}
	}

	return nil
}

func Scan(r *sql.Rows) (*Result, error) {
	columns, err := r.ColumnTypes()
	if err != nil {
		return nil, err
	}

	cts := make([]*ColType, len(columns))

	for i := range cts {
		cts[i] = NewColType(columns[i])
	}

	data := make([][]interface{}, 0)
	for r.Next() {
		rowScan := make([]interface{}, len(columns))
		for i := range cts {
			rowScan[i] = reflect.New(cts[i].scanType).Interface()
		}

		err = r.Scan(rowScan...)
		if err != nil {
			return nil, err
		}

		row := make([]interface{}, len(columns))
		for i := range rowScan {
			row[i] = reflect.ValueOf(rowScan[i]).Elem().Interface()
		}

		data = append(data, row)
	}

	return &Result{
		ResultType: ResultType{
			colType: cts,
		},
		data: data,
	}, r.Err()
}

func Marshal(result *Result) ([]byte, error) {
	// {cols:[],result:[[]]}

	//return json.Marshal()
	return json.MarshalIndent(map[string]interface{}{
		"name":    result.name,
		"isTable": result.isTable,
		"cols":    result.colType,
		"data":    result.data,
	}, "", "  ")
}

func Unmarshal(data []byte) (*Result, error) {
	var tmp struct {
		Name    string
		IsTable bool
		Cols    []*ColType
		Data    []json.RawMessage
	}

	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return nil, err
	}

	rows := &Result{
		ResultType: ResultType{
			name:    tmp.Name,
			isTable: tmp.IsTable,
			colType: tmp.Cols,
		},
		data: make([][]interface{}, len(tmp.Data)),
	}

	lsScan := make([]interface{}, len(tmp.Cols))
	for i, v := range tmp.Cols {
		lsScan[i] = reflect.New(v.scanType).Interface()
	}

	for i := range rows.data {
		err = json.Unmarshal(tmp.Data[i], &lsScan)
		if err != nil {
			return nil, err
		}

		rows.data[i] = make([]interface{}, len(lsScan))
		for j := range lsScan {
			rows.data[i][j] = reflect.ValueOf(lsScan[j]).Elem().Interface()
		}
	}

	return rows, nil
}

func Load(path string) (*Result, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return Unmarshal(data)
}

type Snapshot struct {
	name     string
	testName string
	results  []*Result
}

func (s *Snapshot) Save(overWrite bool) error {
	path := filepath.Join("testdata/snapshot", s.testName, s.name)
	_, err := os.Stat(path)
	if !os.IsNotExist(err) {
		if !overWrite {
			return os.ErrExist
		}
		err = os.RemoveAll(path)
		if err != nil {
			return err
		}
	}

	err = os.MkdirAll(path, 0755)
	if err != nil {
		return err
	}

	for _, v := range s.results {
		data, err := Marshal(v)
		if err != nil {
			return err
		}

		err = ioutil.WriteFile(filepath.Join(path, v.name), data, 0644)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Snapshot) Apply(db *sql.DB) error {
	for _, v := range s.results {
		err := v.Apply(db)
		if err != nil {
			return err
		}
	}

	return nil
}

func CompareSnapshot(expect, actual *Snapshot) (string, bool) {
	if len(expect.results) != len(actual.results) {
		return "len(results)", false
	}

	for i, r := range expect.results {
		diff, same := CompareResult(r, actual.results[i])
		if !same {
			return fmt.Sprintf("\ncheck snapshot fail, result name: %s\n%s", r.name, diff), false
		}
	}

	return "", true
}
