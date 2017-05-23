package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
	_ "github.com/kshvakov/clickhouse"
)

type Synchronizer struct {
	config      *Config
	connections []*sql.DB
	database    string
	fix         bool
	dropColumns bool
	debug       bool
}

func NewSynchronizer(config *Config) *Synchronizer {
	return &Synchronizer{config: config}
}

func (sync *Synchronizer) SetFix(fix bool) {
	sync.fix = fix
}

func (sync *Synchronizer) SetDropColumns(dropColumns bool) {
	sync.dropColumns = dropColumns
}

func (sync *Synchronizer) SetupLogger(debug bool) {
	sync.debug = debug
	if debug {
		log.SetLevel(log.DebugLevel)
	}
}

func (sync *Synchronizer) Connect() error {
	var errs []error
	sync.connections = make([]*sql.DB, 0, len(sync.config.Servers))

	for _, server := range sync.config.Servers {
		dsn := fmt.Sprintf(
			"tcp://%s:%d?username=%s&password=%s&debug=%d",
			server.Host,
			server.Port,
			server.User,
			server.Pass,
			map[bool]int{true: 0, false: 0}[sync.debug],
		)
		conn, err := sql.Open("clickhouse", dsn)

		if err != nil {
			errs = append(errs, err)
			continue
		}

		err = conn.Ping()
		if err != nil {
			errs = append(errs, err)
			continue
		}

		sync.connections = append(sync.connections, conn)
	}

	if len(errs) == 0 {
		return nil
	}

	var text string
	for _, err := range errs {
		text += err.Error() + "\n"
	}
	err := errors.New(text)
	return err
}

func (sync *Synchronizer) Close() error {
	var errs []error

	for _, conn := range sync.connections {
		err := conn.Close()
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}

	if len(errs) == 0 {
		return nil
	}

	var text string
	for _, err := range errs {
		text += err.Error() + "\n"
	}
	err := errors.New(text)
	return err
}

type ExecResult struct {
	sql.Result
	error
}

type ExecResults []ExecResult

func (e ExecResults) HasError() bool {
	for _, r := range e {
		if r.error != nil {
			return true
		}
	}
	return false
}

func (e ExecResults) Error() string {
	var text string

	for _, r := range e {
		if r.error != nil {
			text += r.Error() + "\n"
		}
	}

	return text
}

type QueryResult struct {
	Rows *sql.Rows
	error
}

type QueryResults []QueryResult

func (e QueryResults) HasError() bool {
	for _, r := range e {
		if r.Rows != nil && r.Rows.Err() != nil {
			return true
		}
	}
	return false
}

func (e QueryResults) Error() string {
	var text string

	for _, r := range e {
		if r.Rows != nil && r.Rows.Err() != nil {
			text += r.Error() + "\n"
		}
	}

	return text
}

func (e QueryResults) Close() {
	for _, r := range e {
		if r.Rows != nil {
			r.Rows.Close()
		}
	}
}

func (sync *Synchronizer) Exec(query string, args ...interface{}) ExecResults {
	var results ExecResults

	for _, conn := range sync.connections {
		r, e := conn.Exec(query, args...)
		results = append(results, ExecResult{r, e})
	}

	return results
}

func (sync *Synchronizer) Query(query string, args ...interface{}) QueryResults {
	var results QueryResults

	for _, conn := range sync.connections {
		r, e := conn.Query(query, args...)
		results = append(results, QueryResult{r, e})
	}

	return results
}

func (sync *Synchronizer) CreateTable(i int, name string, table Table) {
	conn := sync.connections[i]
	l := log.WithFields(log.Fields{
		"host":     sync.config.Servers[i].Host,
		"database": sync.database,
		"table":    name,
	})

	createSQL := "CREATE TABLE IF NOT EXISTS " + name + " "
	if len(table.Columns) != 0 {
		var typesSQL []string
		for columnName, columnType := range table.Columns {
			typesSQL = append(typesSQL, columnName+" "+columnType)
		}

		createSQL += "(" + strings.Join(typesSQL, ", ") + ") "
	} else if table.AsAnotherTable != "" {
		createSQL += "AS " + table.AsAnotherTable + " "
	}

	createSQL += "ENGINE = " + table.Engine

	l.Debug(createSQL)

	_, err := conn.Exec(createSQL)

	if err != nil {
		l.WithError(err).Error("Failed to create table")
	} else {
		l.Info("Created table")
	}
}

func (sync *Synchronizer) CreateView(i int, name string, view Table) {
	conn := sync.connections[i]
	l := log.WithFields(log.Fields{
		"host":     sync.config.Servers[i].Host,
		"database": sync.database,
		"view":     name,
	})

	if view.AsSelect == "" {
		l.Error("as_select is not defined")
		return
	}

	var createSQL string
	if view.Materialized {
		createSQL = "CREATE MATERIALIZED VIEW IF NOT EXISTS "
	} else {
		createSQL = "CREATE VIEW IF NOT EXISTS "
	}

	createSQL += name + " "

	if len(view.Columns) != 0 {
		var typesSQL []string
		for columnName, columnType := range view.Columns {
			typesSQL = append(typesSQL, columnName+" "+columnType)
		}

		createSQL += "(" + strings.Join(typesSQL, ", ") + ") "
	} else if view.AsAnotherTable != "" {

	}

	if view.Engine != "" {
		createSQL += "ENGINE = " + view.Engine + " "
	}

	if view.Populate {
		createSQL += "POPULATE "
	}

	createSQL += "AS " + view.AsSelect

	l.Debug(createSQL)

	_, err := conn.Exec(createSQL)

	if err != nil {
		l.WithError(err).Error("Failed to create view")
	} else {
		l.Info("Created view")
	}
}

func (sync *Synchronizer) ModifyColumn(i int, name, columnName, columnType string) {
	conn := sync.connections[i]
	l := log.WithFields(log.Fields{
		"host":     sync.config.Servers[i].Host,
		"database": sync.database,
		"view":     name,
		"column":   columnName,
		"type":     columnType,
	})

	modifySQL := "ALTER TABLE " + name + " MODIFY COLUMN " + columnName + " " + columnType

	l.Debug(modifySQL)

	_, err := conn.Exec(modifySQL)

	if err != nil {
		l.WithError(err).Error("Failed to modify column type")
	} else {
		l.Info("Modified column type")
	}
}

func (sync *Synchronizer) AddColumn(i int, name, columnName, columnType string) {
	conn := sync.connections[i]
	l := log.WithFields(log.Fields{
		"host":     sync.config.Servers[i].Host,
		"database": sync.database,
		"view":     name,
		"column":   columnName,
		"type":     columnType,
	})

	modifySQL := "ALTER TABLE " + name + " ADD COLUMN " + columnName + " " + columnType

	l.Debug(modifySQL)

	_, err := conn.Exec(modifySQL)

	if err != nil {
		l.WithError(err).Error("Failed to add column")
	} else {
		l.Info("Added column")
	}
}

func (sync *Synchronizer) DropColumn(i int, name, columnName string) {
	conn := sync.connections[i]
	l := log.WithFields(log.Fields{
		"host":     sync.config.Servers[i].Host,
		"database": sync.database,
		"view":     name,
		"column":   columnName,
	})

	modifySQL := "ALTER TABLE " + name + " DROP COLUMN " + columnName

	l.Debug(modifySQL)

	_, err := conn.Exec(modifySQL)

	if err != nil {
		l.WithError(err).Error("Failed to drop column")
	} else {
		l.Info("Dropped column")
	}
}

func (sync *Synchronizer) CheckTable(name string, table Table) {
	l := log.WithFields(log.Fields{
		"database": sync.database,
		"table":    name,
	})

	r := sync.Query("SELECT name, type FROM system.columns WHERE database = ? AND table = ?", sync.database, name)
	if r.HasError() {
		l.Error(r.Error())
	}
	defer r.Close()

	for i, e := range r {
		l = l.WithField("host", sync.config.Servers[i].Host)

		exists := false
		existColumns := make(map[string]bool)

		for e.Rows != nil && e.Rows.Next() {
			exists = true

			var columnName, columnType, needType string
			var ok bool

			e.Rows.Scan(&columnName, &columnType)
			existColumns[columnName] = true

			if len(table.Columns) == 0 {
				continue
			}

			if needType, ok = table.Columns[columnName]; !ok {
				l.WithField("column", columnName).Error("Table has excess column")

				if sync.fix && sync.dropColumns {
					go sync.DropColumn(i, name, columnName)
				}
				continue
			}

			needType = strings.Fields(needType)[0]
			if needType != columnType {
				l.WithFields(log.Fields{
					"column":    columnName,
					"need type": needType,
					"has type":  columnType,
				}).Error("Column type mismatch")

				if sync.fix {
					go sync.ModifyColumn(i, name, columnName, needType)
				}
			}
		}

		if !exists {
			l.Error("Table does not exist")

			if sync.fix {
				if table.View {
					go sync.CreateView(i, name, table)
				} else {
					go sync.CreateTable(i, name, table)
				}
			}

			continue
		}

		for columnName, needType := range table.Columns {
			if _, exists := existColumns[columnName]; exists {
				continue
			}

			l.WithField("column", columnName).Error("Table has not enough columns")

			if sync.fix {
				go sync.AddColumn(i, name, columnName, needType)
			}
		}
	}
}

func (sync *Synchronizer) CheckDatabase(db Database) {
	r := sync.Exec(fmt.Sprintf("USE %s", db.Name))
	if r.HasError() {
		panic(r)
	}

	sync.database = db.Name

	for name, table := range db.Tables {
		sync.CheckTable(name, table)
	}
}

func (sync *Synchronizer) Check() {
	for _, db := range sync.config.Databases {
		sync.CheckDatabase(db)
	}
}
