package dbtesting

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/forsaken628/bsql"
	"os"
	"path/filepath"
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
	s := Snapshot{
		name:     name,
		testName: t.testing.Name(),
		results:  make([]*Result, len(tables)),
	}

	for i, v := range tables {
		rows, err := t.FetchResultFromTable(v)
		if err != nil {
			return nil, err
		}

		s.results[i] = rows
	}

	return &s, nil
}

func (t *TT) NewSnapshotFromQuery(name string, query []string) (*Snapshot, error) {
	s := &Snapshot{
		name:     name,
		testName: t.testing.Name(),
		results:  make([]*Result, len(query)),
	}

	for i, q := range query {
		rows, err := t.db.Query(q)
		if err != nil {
			return nil, err
		}

		s.results[i], err = Scan(rows)
		if err != nil {
			return nil, err
		}

		s.results[i].name = fmt.Sprint("query", i)
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
	s := &Snapshot{
		name:     name,
		testName: t.testing.Name(),
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

			s.results = append(s.results, rows)
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

	switch args.Active {

	case ActiveSkip:
		t.testing.Log("skip initial")
		return false

	case ActiveRecord:
		s, err := t.NewSnapshotFromTables("initial", args.Tables)
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
		err := t.ApplySnapshot("initial")
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

type CheckQueryArgs struct {
	Active    active
	Name      string
	Queries   []string
	OverWrite bool
}

func (t *TT) CheckQuery(args *CheckQueryArgs) bool {
	t.testing.Helper()
	if args.Active != ActiveRecord && args.OverWrite {
		t.testing.Error(ErrOverWriteOn)
		return true
	}

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

type CheckTableArgs struct {
	Active    active
	Name      string
	Tables    []string
	OverWrite bool
}

func (t *TT) CheckTable(args *CheckTableArgs) bool {
	t.testing.Helper()
	if args.Active != ActiveRecord && args.OverWrite {
		t.testing.Error(ErrOverWriteOn)
		return true
	}

	switch args.Active {

	case ActiveSkip:
		t.testing.Log("skip check")
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

	case ActiveCheck:
		s0, err := t.LoadSnapshot(args.Name)
		if err != nil {
			t.testing.Error(err)
			return true
		}

		s1, err := t.NewSnapshotFromTables(args.Name, args.Tables)
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
