package dbtesting

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/forsaken628/bsql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

type active int

const (
	ActiveSkip = active(iota)
	ActiveRecord
	ActiveApply
	ActiveCheck
)

var (
	ErrInvalidActive = errors.New("invalid active")
	ErrRecordSuccess = errors.New("record success")
	ErrOverWriteOn   = errors.New("overwrite should off")
)

type TT struct {
	db      *sql.DB
	testing *testing.T
}

func NewTT(db *sql.DB, t *testing.T) *TT {
	return &TT{db: db, testing: t}
}

func (t *TT) FetchResultFromTable(tabName string) (*Result, error) {
	q, a := bsql.Select{
		Table: bsql.Raw(tabName),
	}.Build()

	r, err := t.db.Query(q, a...)
	if err != nil {
		return nil, err
	}

	rows, err := Scan(r)
	if err != nil {
		return nil, err
	}

	rows.name = tabName
	rows.isTable = true

	return rows, nil
}

func (t *TT) NewSnapshotFromTables(name string, tables []string) (*Snapshot, error) {
	qs := make([]*Query, len(tables))
	for i, tn := range tables {
		qs[i] = NewQueryTable(tn)
	}

	return t.NewSnapshotFromQuery(name, qs)
}

func (t *TT) NewSnapshotFromQuery(name string, queries []*Query) (*Snapshot, error) {
	name = clearName(name)

	s := &Snapshot{
		name:     name,
		testName: t.testing.Name(),
		results:  make(map[string]*Result, len(queries)),
	}

	for _, q := range queries {
		rows, err := t.db.Query(q.query)
		if err != nil {
			return nil, err
		}

		s.results[q.name], err = Scan(rows)
		if err != nil {
			return nil, err
		}

		s.results[q.name].name = q.name
		s.results[q.name].isTable = q.isTable
		s.results[q.name].query = q
	}

	return s, nil
}

func (t *TT) ApplySnapshot(name string) error {
	s, err := t.LoadSnapshot(name)
	if err != nil {
		return err
	}

	return s.Apply(t.db)
}

func (t *TT) LoadSnapshot(name string) (*Snapshot, error) {
	name = clearName(name)

	s := &Snapshot{
		name:     name,
		testName: t.testing.Name(),
		results:  make(map[string]*Result),
	}

	f, err := os.Open(filepath.Join("testdata/snapshot", s.testName, name))
	if err != nil {
		return nil, err
	}

	fis, err := f.Readdir(0)
	f.Close()
	if err != nil {
		return nil, err
	}

	for _, v := range fis {
		if !v.IsDir() {
			rows, err := Load(filepath.Join("testdata/snapshot", s.testName, name, v.Name()))
			if err != nil {
				return nil, err
			}

			s.results[name] = rows
		}
	}

	return s, nil
}

func (t *TT) DB() *sql.DB {
	return t.db
}

type InitialArgs struct {
	Active    active
	Name      string
	Tables    []string
	OverWrite bool
}

func (t *TT) Initial(args *InitialArgs) bool {
	t.testing.Helper()
	if args.Active != ActiveRecord && args.OverWrite {
		t.testing.Error(ErrOverWriteOn)
		return true
	}

	if args.Name == "" {
		args.Name = "initial"
	}
	args.Name = clearName(args.Name)

	switch args.Active {

	case ActiveSkip:
		t.testing.Log("skip initial")
		return false

	case ActiveRecord:
		s, err := t.NewSnapshotFromTables(args.Name, args.Tables)
		if err != nil {
			t.testing.Error(err)
			return true
		}
		err = s.Save(args.OverWrite)
		if err != nil {
			t.testing.Error(err)
			return true
		}
		t.testing.Fatal(ErrRecordSuccess)
		return true

	case ActiveApply:
		err := t.ApplySnapshot(args.Name)
		if err != nil {
			t.testing.Error(err)
			return true
		}

		return false

	default:
		t.testing.Error(ErrInvalidActive)
		return true
	}
}

type Query struct {
	name        string
	isTable     bool
	query       string
	comparators map[string]Comparator
}

func (q *Query) RegisterComparator(col string, fn Comparator) {
	if q.comparators == nil {
		q.comparators = map[string]Comparator{col: fn}
		return
	}
	q.comparators[col] = fn
}

func NewQuery(name, q string) *Query {
	return &Query{name: name, query: q}
}

func NewQueryTable(name string) *Query {
	return &Query{
		name:    name,
		isTable: true,
		query:   fmt.Sprintf("select * from %s", name),
	}
}

type CheckQueryArgs struct {
	Active    active
	Name      string
	Queries   []*Query
	OverWrite bool
}

func (t *TT) CheckQuery(args *CheckQueryArgs) bool {
	t.testing.Helper()
	if args.Active != ActiveRecord && args.OverWrite {
		t.testing.Error(ErrOverWriteOn)
		return true
	}

	args.Name = clearName(args.Name)

	switch args.Active {

	case ActiveSkip:
		t.testing.Log("skip check")
		return false

	case ActiveRecord:
		s, err := t.NewSnapshotFromQuery(args.Name, args.Queries)
		if err != nil {
			t.testing.Error(err)
			return true
		}
		err = s.Save(args.OverWrite)
		if err != nil {
			t.testing.Error(err)
			return true
		}
		t.testing.Fatal(ErrRecordSuccess)
		return true

	case ActiveCheck:
		s0, err := t.LoadSnapshot(args.Name)
		if err != nil {
			t.testing.Error(err)
			return true
		}

		for _, q := range args.Queries {
			s0.results[q.name].query = q
		}

		s1, err := t.NewSnapshotFromQuery(args.Name, args.Queries)
		if err != nil {
			t.testing.Error(err)
			return true
		}

		diff, same := CompareSnapshot(s0, s1)
		if !same {
			t.testing.Error(diff)
			return true
		}
		return false

	default:
		t.testing.Error(ErrInvalidActive)
		return true
	}
}

var nameReg = regexp.MustCompile(`^[\w_-]+$`)

func clearName(name string) string {
	if nameReg.MatchString(name) {
		return strings.ToLower(name)
	}
	panic("invalid name")
}
