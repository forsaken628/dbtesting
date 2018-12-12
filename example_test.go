package dbtesting_test

import (
	"database/sql"
	"github.com/forsaken628/dbtesting"
	"testing"
)

func getDB(t *testing.T, fn func(tt *dbtesting.TT)) {
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1)/test?charset=utf8mb4&parseTime=true&loc=Local")
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Error(err)
		return
	}

	fn(dbtesting.NewTT(db, t))
}

func TestAA(t *testing.T) {
	getDB(t, func(tt *dbtesting.TT) {
		if tt.Initial(&dbtesting.InitialArgs{
			Active: dbtesting.ActiveApply,
			Tables: []string{"tab1", "tab2"},
		}) {
			return
		}

		if tt.CheckQuery(&dbtesting.CheckQueryArgs{
			Active: dbtesting.ActiveCheck,
			Name:   "aa",
			Queries: []*dbtesting.Query{
				dbtesting.NewQueryTable("tab1"),
				dbtesting.NewQueryTable("tab2"),
			},
		}) {
			return
		}

		if tt.CheckQuery(&dbtesting.CheckQueryArgs{
			Active: dbtesting.ActiveCheck,
			Name:   "bb",
			Queries: []*dbtesting.Query{
				dbtesting.NewQuery("selectMax", "select max(col1) from tab1"),
			},
		}) {
			return
		}
	})
}

func TestAA2(t *testing.T) {
	getDB(t, func(tt *dbtesting.TT) {

		q := dbtesting.NewQueryTable("tab3")
		q.RegisterComparator("time", dbtesting.TimeShouldAfter)

		if tt.CheckQuery(&dbtesting.CheckQueryArgs{
			Active: dbtesting.ActiveCheck,
			//OverWrite: true,
			Name:    "bb",
			Queries: []*dbtesting.Query{q},
		}) {
			return
		}

	})
}
