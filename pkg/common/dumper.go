/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"context"
	"fmt"
	"github.com/harryge00/go-mydumper/pkg/config"
	"github.com/harryge00/go-mydumper/pkg/storage"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// writer writes file to external storage.
var writer storage.ExternalStorage

func writeMetaData(args *config.Args) error {
	return writer.WriteFile("metadata", "")
}

func dumpDatabaseSchema(log *xlog.Log, conn *Connection, args *config.Args, database string) error {
	err := conn.Execute(fmt.Sprintf("USE `%s`", database))
	AssertNil(err)

	schema := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", database)
	file := fmt.Sprintf("%s-schema-create.sql", database)
	err = writer.WriteFile(file, schema)
	log.Info("dumping.database[%s].schema...", database)
	return err
}

func dumpTableSchema(log *xlog.Log, conn *Connection, args *config.Args, database string, table string) error {
	qr, err := conn.Fetch(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, table))
	AssertNil(err)
	schema := qr.Rows[0][1].String() + ";\n"

	file := fmt.Sprintf("%s.%s-schema.sql", database, table)
	err = writer.WriteFile(file, schema)
	log.Info("dumping.table[%s.%s].schema...", database, table)
	return err
}

func dumpTable(log *xlog.Log, conn *Connection, args *config.Args, database string, table string) {
	var allBytes uint64
	var allRows uint64
	var where string
	var selfields []string

	fields := make([]string, 0, 16)
	{
		cursor, err := conn.StreamFetch(fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 1", database, table))
		AssertNil(err)

		flds := cursor.Fields()
		for _, fld := range flds {
			log.Debug("dump -- %#v, %s, %s", args.Filters, table, fld.Name)
			if _, ok := args.Filters[table][fld.Name]; ok {
				continue
			}

			fields = append(fields, fmt.Sprintf("`%s`", fld.Name))
			replacement, ok := args.Selects[table][fld.Name]
			if ok {
				selfields = append(selfields, fmt.Sprintf("%s AS `%s`", replacement, fld.Name))
			} else {
				selfields = append(selfields, fmt.Sprintf("`%s`", fld.Name))
			}
		}
		err = cursor.Close()
		AssertNil(err)
	}

	if v, ok := args.Wheres[table]; ok {
		where = fmt.Sprintf(" WHERE %v", v)
	}

	cursor, err := conn.StreamFetch(fmt.Sprintf("SELECT %s FROM `%s`.`%s` %s", strings.Join(selfields, ", "), database, table, where))
	AssertNil(err)

	fileNo := 1
	stmtsize := 0
	chunkbytes := 0
	rows := make([]string, 0, 256)
	inserts := make([]string, 0, 256)
	for cursor.Next() {
		row, err := cursor.RowValues()
		AssertNil(err)

		values := make([]string, 0, 16)
		for _, v := range row {
			if v.Raw() == nil {
				values = append(values, "NULL")
			} else {
				str := v.String()
				switch {
				case v.IsSigned(), v.IsUnsigned(), v.IsFloat(), v.IsIntegral(), v.Type() == querypb.Type_DECIMAL:
					values = append(values, str)
				default:
					values = append(values, fmt.Sprintf("\"%s\"", EscapeBytes(v.Raw())))
				}
			}
		}
		r := "(" + strings.Join(values, ",") + ")"
		rows = append(rows, r)

		allRows++
		stmtsize += len(r)
		chunkbytes += len(r)
		allBytes += uint64(len(r))
		atomic.AddUint64(&args.Allbytes, uint64(len(r)))
		atomic.AddUint64(&args.Allrows, 1)

		if stmtsize >= args.StmtSize {
			insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table, strings.Join(fields, ","), strings.Join(rows, ",\n"))
			inserts = append(inserts, insertone)
			rows = rows[:0]
			stmtsize = 0
		}

		if (chunkbytes / 1024 / 1024) >= args.ChunksizeInMB {
			query := strings.Join(inserts, ";\n") + ";\n"
			file := fmt.Sprintf("%s.%s.%05d.sql", database, table, fileNo)
			writer.WriteFile(file, query)

			log.Info("dumping.table[%s.%s].rows[%v].bytes[%vMB].part[%v].thread[%d]", database, table, allRows, (allBytes / 1024 / 1024), fileNo, conn.ID)
			inserts = inserts[:0]
			chunkbytes = 0
			fileNo++
		}
	}
	if chunkbytes > 0 {
		if len(rows) > 0 {
			insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table, strings.Join(fields, ","), strings.Join(rows, ",\n"))
			inserts = append(inserts, insertone)
		}

		query := strings.Join(inserts, ";\n") + ";\n"
		file := fmt.Sprintf("%s.%s.%05d.sql", database, table, fileNo)
		writer.WriteFile(file, query)
	}
	err = cursor.Close()
	AssertNil(err)

	log.Info("dumping.table[%s.%s].done.allrows[%v].allbytes[%vMB].thread[%d]...", database, table, allRows, (allBytes / 1024 / 1024), conn.ID)
}

func allTables(log *xlog.Log, conn *Connection, database string) []string {
	qr, err := conn.Fetch(fmt.Sprintf("SHOW TABLES FROM `%s`", database))
	AssertNil(err)

	tables := make([]string, 0, 128)
	for _, t := range qr.Rows {
		tables = append(tables, t[0].String())
	}
	return tables
}

func allDatabases(log *xlog.Log, conn *Connection) []string {
	qr, err := conn.Fetch("SHOW DATABASES")
	AssertNil(err)

	databases := make([]string, 0, 128)
	for _, t := range qr.Rows {
		databases = append(databases, t[0].String())
	}
	return databases
}

func filterDatabases(log *xlog.Log, conn *Connection, filter *regexp.Regexp, invert bool) []string {
	qr, err := conn.Fetch("SHOW DATABASES")
	AssertNil(err)

	databases := make([]string, 0, 128)
	for _, t := range qr.Rows {
		if (!invert && filter.MatchString(t[0].String())) || (invert && !filter.MatchString(t[0].String())) {
			databases = append(databases, t[0].String())
		}
	}
	return databases
}

// Dumper used to start the dumper worker.
func Dumper(log *xlog.Log, args *config.Args) {
	var err error
	// Decide storage type:
	switch args.StorageType {
	case config.LocaltorageType:
		writer, err = storage.NewLocalStorage(args.Outdir)
		if err != nil {
			log.Panicf("Failed to initialize local storage: %v", err)
		}
	case config.MinioStorageType:
		var err error
		writer, err = storage.NewMinioStorage(context.Background(), args.MinioEndpoint, args.MinioBucket,
			args.MinioAccessKey, args.MinioSecretKey, args.UseSSL)
		log.Info("Minio config: %v %v %v %v", args.MinioBucket, args.MinioEndpoint, args.MinioAccessKey, args.UseSSL)
		if err != nil {
			log.Panicf("Failed to initialize minio storage: %v", err)
		}
	default:
		// use local storage as default
		writer, err = storage.NewLocalStorage(args.Outdir)
		if err != nil {
			log.Panicf("Failed to initialize local storage: %v", err)
		}
	}

	pool, err := NewPool(log, args.Threads, args.Address, args.User, args.Password, args.SessionVars)
	AssertNil(err)
	defer pool.Close()

	// Meta data.
	err = writeMetaData(args)
	if err != nil {
		log.Error("Failed to writeMetaData: %v", err)
	}
	// database.
	var wg sync.WaitGroup
	conn := pool.Get()
	var databases []string
	t := time.Now()
	if args.DatabaseRegexp != "" {
		r := regexp.MustCompile(args.DatabaseRegexp)
		databases = filterDatabases(log, conn, r, args.DatabaseInvertRegexp)
	} else {
		if args.Database != "" {
			databases = strings.Split(args.Database, ",")
		} else {
			databases = allDatabases(log, conn)
		}
	}
	for _, database := range databases {
		err = dumpDatabaseSchema(log, conn, args, database)
		if err != nil {
			log.Error("Failed to dumpDatabaseSchema: %v", err)
		}
	}

	// tables.
	tables := make([][]string, len(databases))
	for i, database := range databases {
		if args.Table != "" {
			tables[i] = strings.Split(args.Table, ",")
		} else {
			tables[i] = allTables(log, conn, database)
		}
	}
	pool.Put(conn)

	for i, database := range databases {
		for _, table := range tables[i] {
			conn := pool.Get()
			dumpTableSchema(log, conn, args, database, table)

			wg.Add(1)
			go func(conn *Connection, database string, table string) {
				defer func() {
					wg.Done()
					pool.Put(conn)
				}()
				log.Info("dumping.table[%s.%s].datas.thread[%d]...", database, table, conn.ID)
				dumpTable(log, conn, args, database, table)
				log.Info("dumping.table[%s.%s].datas.thread[%d].done...", database, table, conn.ID)
			}(conn, database, table)
		}
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(args.IntervalMs))
	defer tick.Stop()
	go func() {
		for range tick.C {
			diff := time.Since(t).Seconds()
			allbytesMB := float64(atomic.LoadUint64(&args.Allbytes) / 1024 / 1024)
			allrows := atomic.LoadUint64(&args.Allrows)
			rates := allbytesMB / diff
			log.Info("dumping.allbytes[%vMB].allrows[%v].time[%.2fsec].rates[%.2fMB/sec]...", allbytesMB, allrows, diff, rates)
		}
	}()

	wg.Wait()
	elapsed := time.Since(t).Seconds()
	log.Info("dumping.all.done.cost[%.2fsec].allrows[%v].allbytes[%v].rate[%.2fMB/s]", elapsed, args.Allrows, args.Allbytes, (float64(args.Allbytes/1024/1024) / elapsed))
}
