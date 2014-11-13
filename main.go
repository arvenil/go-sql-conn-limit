package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"runtime"
)

const (
	maxIdleConn = 5
	maxOpenConn = 5
	dsn         = "root:@/"
)

func main() {

	log.Println("starting up")

	/**
	 * Proper usage of db.Query() without any limits yet
	 */
	log.Printf("# proper usage of db.Query() and rows.Close() doesn't increase opened connections")
	log.Printf("Opening db handle")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}

	printThreadsConnected()
	for i := 1; i <= 5; i++ {
		rows, err := db.Query("show databases")
		rows.Close() // note, we should close rows after each query
		if err == nil {
			log.Printf("Success running query number %d", i)
		} else {
			log.Printf("Error running query number %d: %s", i, err)
		}
		printThreadsConnected()
	}
	db.Close()
	log.Printf("Closing db")

	/**
	 * SetMaxIdleConns
	 */
	log.Printf("# SetMaxIdleConns(%d) doesn't limit max connections", maxIdleConn)
	log.Printf("# Running %d queries without rows.Close()", maxIdleConn+1)
	log.Printf("Opening db handle")
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}

	log.Printf("# Setting max idle connections to %d", maxIdleConn)
	db.SetMaxIdleConns(maxIdleConn)
	printThreadsConnected()
	for i := 1; i <= maxIdleConn+1; i++ {
		_, err := db.Query("show databases")
		if err == nil {
			log.Printf("Success running query number %d", i)
		} else {
			log.Printf("Error running query number %d: %s", i, err)
		}
		printThreadsConnected()
	}
	log.Printf("Closing db")
	db.Close()
	printThreadsConnected()
	log.Printf("# that's interesting, looks like db.Close() doesn't release taken connections imiedietly")
	log.Printf("# running runtime.GC()")
	runtime.GC()
	printThreadsConnected()
	log.Printf("# for me running GC helped")

	/**
	 * SetMaxOpenConns
	 */
	log.Printf("# SetMaxOpenConns(%d) limits max connections but it hangs when no conn is available, waiting for one to be released", maxOpenConn)
	log.Printf("# Running %d queries without rows.Close()", maxOpenConn+1)
	log.Printf("Opening db handle")
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}

	log.Printf("# Setting max open connections to %d", maxOpenConn)
	db.SetMaxOpenConns(maxOpenConn)
	printThreadsConnected()
	for i := 1; i <= maxOpenConn+1; i++ {
		if i == maxOpenConn+1 {
			log.Printf("# It will hang now because of MaxOpenConns, use ctrl+c to kill it")
		}
		_, err := db.Query("show databases")
		if err == nil {
			log.Printf("Success running query number %d", i)
		} else {
			log.Printf("Error running query number %d: %s", i, err)
		}
		printThreadsConnected()
	}
	db.Close()

	printThreadsConnected()
}

func printThreadsConnected() {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	var varName string
	var varValue int
	err = db.QueryRow("SHOW STATUS WHERE `variable_name` = 'Threads_connected'").Scan(
		&varName,
		&varValue,
	)
	if err != nil {
		log.Printf("Unable to get number of threads connected: %s", err)
	} else {
		log.Printf("Threads connected: %d", varValue)
	}
}
