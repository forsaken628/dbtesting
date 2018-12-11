package dbtesting

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"testing"
)

func getDB(t *testing.T, fn func(tt *TT)) {
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

	fn(NewTT(db, t))
}

func TestMarshal(t *testing.T) {
	getDB(t, func(tt *TT) {
		rows, err := tt.FetchResultFromTable("number")
		if err != nil {
			t.Error(err)
			return
		}

		data, err := Marshal(rows)
		if err != nil {
			t.Error(err)
			return
		}

		vvv, err := Unmarshal(data)
		if err != nil {
			t.Error(err)
			return
		}

		_ = vvv
	})
}

//func TestAAAAA2(testing *testing.T) {
//	getDB(testing, func(db *sql.TT) {
//		results, err := Load("asset_0")
//		if err != nil {
//			testing.Error(err)
//			return
//		}
//
//		err = ApplyRows(db, "asset", []*Result{results})
//		if err != nil {
//			testing.Error(err)
//			return
//		}
//	})
//}

func TestCompareColumn(t *testing.T) {
	f, err := os.Open(".")
	if err != nil {
		t.Error(err)
		return
	}

	dd, err := f.Readdir(-1)
	if err != nil {
		t.Error(err)
		return
	}

	for _, d := range dd {

		fmt.Printf("%#v", d.Sys())

		fmt.Println(d.Name())
	}

	//fmt.Println(fi.Sys())
	//
	//fmt.Println(fi.Name(), fi.IsDir())

}
