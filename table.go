package dbtesting

import (
	"database/sql"
	"errors"
	"github.com/go-sql-driver/mysql"
)

type ColumnDB struct {
	TableCatalog           string         `db:"TABLE_CATALOG"`
	TableSchema            string         `db:"TABLE_SCHEMA"`
	TableName              string         `db:"TABLE_NAME"`
	ColumnName             string         `db:"COLUMN_NAME"`
	OrdinalPosition        int            `db:"ORDINAL_POSITION"`
	ColumnDefault          sql.NullString `db:"COLUMN_DEFAULT"`
	IsNullable             IsNullable     `db:"IS_NULLABLE"`
	DataType               string         `db:"DATA_TYPE"`
	CharacterMaximumLength sql.NullInt64  `db:"CHARACTER_MAXIMUM_LENGTH"`
	CharacterOctetLength   sql.NullInt64  `db:"CHARACTER_OCTET_LENGTH"`
	NumericPrecision       sql.NullInt64  `db:"NUMERIC_PRECISION"`
	NumericScale           sql.NullInt64  `db:"NUMERIC_SCALE"`
	DatetimePrecision      sql.NullInt64  `db:"DATETIME_PRECISION"`
	CharacterSetName       sql.NullString `db:"CHARACTER_SET_NAME"`
	CollationName          sql.NullString `db:"COLLATION_NAME"`
	ColumnType             string         `db:"COLUMN_TYPE"`
	ColumnKey              string         `db:"COLUMN_KEY"`
	Extra                  string         `db:"EXTRA"`
	Privileges             string         `db:"PRIVILEGES"`
	ColumnComment          string         `db:"COLUMN_COMMENT"`
	GenerationExpression   string         `db:"GENERATION_EXPRESSION"`
}

type IsNullable bool

func (b *IsNullable) Scan(value interface{}) error {
	if value == nil {
		return errors.New("invalid value")
	}

	var v string
	switch value.(type) {
	case []byte:
		v = string(value.([]byte))
	case string:
		v = value.(string)
	default:
		return errors.New("invalid value")
	}

	switch v {
	case "YES":
		*b = true
	case "NO":
		*b = false
	default:
		return errors.New("invalid value")
	}

	return nil
}

type TableDB struct {
	TableCatalog   string         `db:"TABLE_CATALOG"`
	TableSchema    string         `db:"TABLE_SCHEMA"`
	TableName      string         `db:"TABLE_NAME"`
	TableType      string         `db:"TABLE_TYPE"`
	Engine         sql.NullString `db:"ENGINE"`
	Version        sql.NullInt64  `db:"VERSION"`
	RowFormat      sql.NullString `db:"ROW_FORMAT"`
	TableRows      sql.NullInt64  `db:"TABLE_ROWS"`
	AvgRowLength   sql.NullInt64  `db:"AVG_ROW_LENGTH"`
	DataLength     sql.NullInt64  `db:"DATA_LENGTH"`
	MaxDataLength  sql.NullInt64  `db:"MAX_DATA_LENGTH"`
	IndexLength    sql.NullInt64  `db:"INDEX_LENGTH"`
	DataFree       sql.NullInt64  `db:"DATA_FREE"`
	AutoIncrement  sql.NullInt64  `db:"AUTO_INCREMENT"`
	CreateTime     mysql.NullTime `db:"CREATE_TIME"`
	UpdateTime     mysql.NullTime `db:"UPDATE_TIME"`
	CheckTime      mysql.NullTime `db:"CHECK_TIME"`
	TableCollation sql.NullString `db:"TABLE_COLLATION"`
	Checksum       sql.NullInt64  `db:"CHECKSUM"`
	CreateOptions  sql.NullString `db:"CREATE_OPTIONS"`
	TableComment   string         `db:"TABLE_COMMENT"`
}

//type Table struct {
//	T TableDB
//	C []ColumnDB
//}

func CompareColumn(a, b *ColumnDB, unsafe bool) (diff []string, same bool) {
	same = true
	if a.TableCatalog != b.TableCatalog {
		diff, same = append(diff, "TableCatalog"), false
	}
	//if a.TableSchema != b.TableSchema {
	//	diff, same = append(diff, "TableSchema"), false
	//}
	//if a.TableName != b.TableName {
	//	diff, same = append(diff, "TableName"), false
	//}
	if a.ColumnName != b.ColumnName {
		diff, same = append(diff, "ColumnName"), false
	}
	if a.OrdinalPosition != b.OrdinalPosition {
		diff, same = append(diff, "OrdinalPosition"), false
	}
	if a.ColumnDefault != b.ColumnDefault {
		diff, same = append(diff, "ColumnDefault"), false
	}
	if a.IsNullable != b.IsNullable {
		diff, same = append(diff, "IsNullable"), false
	}
	if a.DataType != b.DataType {
		diff, same = append(diff, "DataType"), false
	}
	if a.CharacterMaximumLength != b.CharacterMaximumLength {
		diff, same = append(diff, "CharacterMaximumLength"), false
	}
	if a.CharacterOctetLength != b.CharacterOctetLength {
		diff, same = append(diff, "CharacterOctetLength"), false
	}
	if a.NumericPrecision != b.NumericPrecision {
		diff, same = append(diff, "NumericPrecision"), false
	}
	if a.NumericScale != b.NumericScale {
		diff, same = append(diff, "NumericScale"), false
	}
	if a.DatetimePrecision != b.DatetimePrecision {
		diff, same = append(diff, "DatetimePrecision"), false
	}
	if a.CharacterSetName != b.CharacterSetName {
		diff, same = append(diff, "CharacterSetName"), false
	}
	if a.CollationName != b.CollationName {
		diff, same = append(diff, "CollationName"), false
	}
	if a.ColumnType != b.ColumnType {
		diff, same = append(diff, "ColumnType"), false
	}
	if a.ColumnKey != b.ColumnKey {
		diff, same = append(diff, "ColumnKey"), false
	}
	if a.Extra != b.Extra {
		diff, same = append(diff, "Extra"), false
	}
	//if a.Privileges != b.Privileges {
	//	diff, same = append(diff, "Privileges"), false
	//}
	if !unsafe && a.ColumnComment != b.ColumnComment {
		diff, same = append(diff, "ColumnComment"), false
	}
	if a.GenerationExpression != b.GenerationExpression {
		diff, same = append(diff, "GenerationExpression"), false
	}
	return
}

//func CompareTable(a, b *Table, unsafe bool) (diff string, same bool) {
//	if a.T.TableCatalog != b.T.TableCatalog {
//		return "TableCatalog", false
//	}
//	//if a.T.TableSchema != b.T.TableSchema {
//	//	return false
//	//}
//	if a.T.TableName != b.T.TableName {
//		return "TableName", false
//	}
//	if a.T.TableType != b.T.TableType {
//		return "TableType", false
//	}
//	if a.T.Engine != b.T.Engine {
//		return "Engine", false
//	}
//	if a.T.Version != b.T.Version {
//		return "Version", false
//	}
//	if a.T.RowFormat != b.T.RowFormat {
//		return "RowFormat", false
//	}
//	//if a.T.TableRows != b.T.TableRows {
//	//	return false
//	//}
//	//if a.T.AvgRowLength != b.T.AvgRowLength {
//	//	return false
//	//}
//	//if a.T.DataLength != b.T.DataLength {
//	//	return false
//	//}
//	//if a.T.MaxDataLength != b.T.MaxDataLength {
//	//	return false
//	//}
//	//if a.T.IndexLength != b.T.IndexLength {
//	//	return false
//	//}
//	if a.T.DataFree != b.T.DataFree {
//		return "DataFree", false
//	}
//	if !unsafe && a.T.AutoIncrement != b.T.AutoIncrement {
//		return "AutoIncrement", false
//	}
//	//if a.T.CreateTime != b.T.CreateTime {
//	//	return false
//	//}
//	//if a.T.UpdateTime != b.T.UpdateTime {
//	//	return false
//	//}
//	//if a.T.CheckTime != b.T.CheckTime {
//	//	return false
//	//}
//	if a.T.TableCollation != b.T.TableCollation {
//		return "TableCollation", false
//	}
//	//if a.T.Checksum != b.T.Checksum {
//	//	return "Checksum", false
//	//}
//	//if a.T.CreateOptions != b.T.CreateOptions {
//	//	return "CreateOptions", false
//	//}
//	if !unsafe && a.T.TableComment != b.T.TableComment {
//		return "TableComment", false
//	}
//	if len(a.C) != len(b.C) {
//		return "len", false
//	}
//	for i := range a.C {
//		diff, same := CompareColumn(&a.C[i], &b.C[i], unsafe)
//		if !same {
//			return strings.Join(diff, ","), false
//		}
//	}
//
//	return "", true
//}
//
//func FetchTable(db *sqlx.TT, schema string, name string) (*Table, error) {
//	tab := Table{}
//
//	err := db.Get(&tab.T, "select * from information_schema.TABLES where TABLE_SCHEMA = ? and TABLE_NAME = ?", schema, name)
//	if err != nil {
//		return nil, err
//	}
//
//	err = db.Select(&tab.C, "select * from information_schema.COLUMNS where TABLE_SCHEMA = ? and TABLE_NAME = ?", schema, name)
//	if err != nil {
//		return nil, err
//	}
//
//	return &tab, nil
//}

//func SliceScan(r *sql.Result) ([]Value, error) {
//	columns, err := r.ColumnTypes()
//	if err != nil {
//		return nil, err
//	}
//
//	values := make([]interface{}, len(columns))
//	for i := range values {
//		switch columns[i].DatabaseTypeName() {
//		case "VARCHAR", "DECIMAL":
//			if nullable, _ := columns[i].Nullable(); nullable {
//				values[i] = new(sql.NullString)
//			} else {
//				values[i] = new(string)
//			}
//		case "TIMESTAMP", "DATETIME":
//			if nullable, _ := columns[i].Nullable(); nullable {
//				values[i] = new(mysql.NullTime)
//			} else {
//				values[i] = new(time.Time)
//			}
//		default:
//			values[i] = reflect.New(columns[i].ScanType()).Interface()
//		}
//	}
//
//	err = r.Scan(values...)
//	if err != nil {
//		return nil, err
//	}
//
//	vals := make([]Value, len(columns))
//	for i := range columns {
//		vals[i] = Value{
//			val:          reflect.ValueOf(values[i]).Elem().Interface(),
//			name:         columns[i].Name(),
//			databaseType: columns[i].DatabaseTypeName(),
//		}
//
//	}
//
//	return vals, r.Err()
//}
//
//type Value struct {
//	val              interface{}
//	name             string
//	databaseType     string
//	fullDatabaseType string
//}

//type Col struct {
//	vals []interface{}
//
//	fullDatabaseType string
//
//	name string
//
//	hasNullable       bool
//	hasLength         bool
//	hasPrecisionScale bool
//
//	nullable     bool
//	length       int64
//	databaseType string
//	precision    int64
//	scale        int64
//	scanType     reflect.Type
//
//	new func() interface{}
//}
//
//func NewCol(cTyp *sql.ColumnType) *Col {
//	c := &Col{}
//	c.name = cTyp.Name()
//	c.databaseType = cTyp.DatabaseTypeName()
//	c.length, c.hasLength = cTyp.Length()
//	c.precision, c.scale, c.hasPrecisionScale = cTyp.DecimalSize()
//
//	switch c.databaseType {
//	case "VARCHAR", "DECIMAL":
//		if c.nullable {
//			c.scanType = reflect.TypeOf(sql.NullString{})
//			c.new = func() interface{} {
//				vv := new(sql.NullString)
//				c.vals = append(c.vals, vv)
//				return vv
//			}
//		} else {
//			c.scanType = reflect.TypeOf("")
//			c.new = func() interface{} {
//				vv := new(string)
//				c.vals = append(c.vals, vv)
//				return vv
//			}
//		}
//	case "TIMESTAMP", "DATETIME":
//		if c.nullable {
//			c.scanType = reflect.TypeOf(mysql.NullTime{})
//			c.new = func() interface{} {
//				vv := new(mysql.NullTime)
//				c.vals = append(c.vals, vv)
//				return vv
//			}
//		} else {
//			c.scanType = reflect.TypeOf(time.Time{})
//			c.new = func() interface{} {
//				vv := new(time.Time)
//				c.vals = append(c.vals, vv)
//				return vv
//			}
//		}
//	default:
//		c.scanType = cTyp.ScanType()
//		c.new = func() interface{} {
//			vv := reflect.New(c.scanType).Interface()
//			c.vals = append(c.vals, vv)
//			return vv
//		}
//	}
//
//	return c
//}
//
//func (c *Col) Index(i int) interface{} {
//	return c.vals[i]
//}
//
//func (c *Col) Len() int {
//	return len(c.vals)
//}

//func ColScan(r *sql.Result) ([]*Col, error) {
//	columns, err := r.ColumnTypes()
//	if err != nil {
//		return nil, err
//	}
//
//	cols := make([]*Col, len(columns))
//
//	for i := range cols {
//		cols[i] = NewCol(columns[i])
//	}
//
//	for r.Next() {
//		results := make([]interface{}, len(columns))
//		for i := range cols {
//			results[i] = cols[i].new()
//		}
//
//		err = r.Scan(results...)
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	return cols, r.Err()
//}

//type col struct {
//	Col struct {
//		FullDatabaseType string
//
//		Name string
//
//		HasNullable       bool
//		HasLength         bool
//		HasPrecisionScale bool
//
//		Nullable     bool
//		Length       int64
//		DatabaseType string
//		Precision    int64
//		Scale        int64
//		ScanType     ScanType
//	}
//	Vals json.RawMessage
//}
//
//func Marshal(v []*Col) ([]byte, error) {
//	// [ {col:{},val:[]},...  ]
//
//	ls := make([]col, len(v))
//
//	for i := range v {
//		ls[i] = col{
//			Col: struct {
//				FullDatabaseType  string
//				Name              string
//				HasNullable       bool
//				HasLength         bool
//				HasPrecisionScale bool
//				Nullable          bool
//				Length            int64
//				DatabaseType      string
//				Precision         int64
//				Scale             int64
//				ScanType          ScanType
//			}{
//				FullDatabaseType:  v[i].fullDatabaseType,
//				Name:              v[i].name,
//				HasNullable:       v[i].hasNullable,
//				HasLength:         v[i].hasLength,
//				HasPrecisionScale: v[i].hasPrecisionScale,
//				Nullable:          v[i].nullable,
//				Length:            v[i].length,
//				DatabaseType:      v[i].databaseType,
//				Precision:         v[i].precision,
//				Scale:             v[i].scale,
//				ScanType:          ScanType{v[i].scanType},
//			},
//		}
//
//		j, err := json.Marshal(v[i].vals)
//		if err != nil {
//			return nil, err
//		}
//
//		ls[i].Vals = j
//	}
//
//	return json.Marshal(ls)
//}
//
//func Unmarshal(data []byte) ([]*Col, error) {
//	var v []col
//	err := json.Unmarshal(data, &v)
//	if err != nil {
//		return nil, err
//	}
//
//	cols := make([]*Col, len(v))
//	for i := range v {
//		c := &Col{
//			fullDatabaseType:  v[i].Col.FullDatabaseType,
//			name:              v[i].Col.Name,
//			hasNullable:       v[i].Col.HasNullable,
//			hasLength:         v[i].Col.HasLength,
//			hasPrecisionScale: v[i].Col.HasPrecisionScale,
//
//			nullable:     v[i].Col.Nullable,
//			length:       v[i].Col.Length,
//			databaseType: v[i].Col.DatabaseType,
//			precision:    v[i].Col.Precision,
//			scale:        v[i].Col.Scale,
//			scanType:     v[i].Col.ScanType.Type,
//		}
//
//		val := reflect.New(reflect.SliceOf(c.scanType)).Elem().Interface()
//
//		err = json.Unmarshal(v[i].Vals, &val)
//		if err != nil {
//			return nil, err
//		}
//
//		reflect.ValueOf(&c.vals).Elem().Set(reflect.ValueOf(val))
//
//		cols[i] = c
//	}
//
//	return cols, nil
//}

//func AAAAA(v []*Col) error {
//	//db.Exec("truncate args")
//
//	cols := make([]string, len(v))
//	values := make([][]interface{}, v[0].Len())
//
//	for i := range v {
//		cols[i] = v[i].name
//	}
//
//	for i := range v[0].vals {
//		row := make([]interface{}, len(v))
//		for j := range v {
//			row[j] = v[j].vals[i]
//		}
//		values[i] = row
//	}
//
//	vvv, err := bsql.MakeValues(cols, values)
//	if err != nil {
//		return err
//	}
//
//	q, a := bsql.Insert{
//		Table: bsql.Raw("args"),
//		Value: vvv,
//	}.Build()
//
//	fmt.Println(q, a)
//
//	return nil
//}
