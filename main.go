package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	bolt "go.etcd.io/bbolt"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"time"
)

type pgEngine struct {
	db     *bolt.DB
	bucket []byte
}

func newPgEngine(db *bolt.DB) *pgEngine {
	return &pgEngine{db, []byte("data")}
}

type table struct {
	Name        string
	ColumnNames []string
	ColumnTypes []string
}

func (p *pgEngine) executeCreate(stmt *pgquery.CreateStmt) error {
	tbl := table{}
	tbl.Name = stmt.Relation.Relname

	for _, c := range stmt.TableElts {
		cd := c.GetColumnDef()
		tbl.ColumnNames = append(tbl.ColumnNames, cd.Colname)
		var columnType string
		for _, n := range cd.TypeName.Names {
			if columnType != "" {
				columnType += "."
			}
			columnType += n.GetString_().Str
		}
		tbl.ColumnTypes = append(tbl.ColumnTypes, columnType)

	}
	tableBytes, err := json.Marshal(tbl)
	if err != nil {
		return fmt.Errorf("could not marshal table: %s", err)
	}

	err = p.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(p.bucket)
		if err != nil {
			return err
		}
		return bkt.Put([]byte("tables_"+tbl.Name), tableBytes)
	})

	if err != nil {
		return fmt.Errorf("could not set key-value: %s", err)
	}

	return nil

}

func (p *pgEngine) executeInsert(stmt *pgquery.InsertStmt) error {
	tbl := stmt.Relation.Relname
	slct := stmt.GetSelectStmt().GetSelectStmt()
	for _, values := range slct.ValuesLists {
		var rowData []any
		for _, value := range values.GetList().Items {
			if c := value.GetAConst(); c != nil {
				if s := c.Val.GetString_(); s != nil {
					rowData = append(rowData, s.Str)
					continue
				}

				if s := c.Val.GetInteger(); s != nil {
					rowData = append(rowData, s.Ival)
					continue
				}
			}
			return fmt.Errorf("unknown value type: %s", value)

		}

		rowBytes, err := json.Marshal(rowData)
		if err != nil {
			return fmt.Errorf("could not marshal row: %s", err)
		}
		id := uuid.New().String()
		err = p.db.Update(func(tx *bolt.Tx) error {
			bkt, err := tx.CreateBucketIfNotExists(p.bucket)
			if err != nil {
				return err
			}
			return bkt.Put([]byte("rows_"+tbl+"_"+id), rowBytes)
		})

		if err != nil {
			return fmt.Errorf("could not store row: %s", err)
		}

	}

	return nil

}

type Result struct {
	fieldNames []string
	fieldTypes []string
	rows       [][]any
}

func (pe *pgEngine) getTableDef(name string) (*table, error) {
	var tbl table

	err := pe.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pe.bucket)
		if bkt == nil {
			return fmt.Errorf("Table does not exist")
		}

		valBytes := bkt.Get([]byte("tables_" + name))
		err := json.Unmarshal(valBytes, &tbl)
		if err != nil {
			return fmt.Errorf("Could not unmarshal table: %s", err)
		}

		return nil
	})

	return &tbl, err
}

func (p *pgEngine) executeSelect(stmt *pgquery.SelectStmt) (*Result, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	tbl, err := p.getTableDef(tblName)
	if err != nil {
		return nil, err
	}
	results := &Result{}
	for _, c := range stmt.TargetList {
		fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str
		results.fieldNames = append(results.fieldNames, fieldName)
		fieldType := ""
		for i, cn := range tbl.ColumnNames {
			if cn == fieldName {
				fieldType = tbl.ColumnTypes[i]
			}
		}
		if fieldType == "" {
			return nil, fmt.Errorf("unknown field: %s", fieldName)
		}
		results.fieldTypes = append(results.fieldTypes, fieldType)
	}
	prefix := []byte("rows_" + tblName + "_")

	p.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(p.bucket).Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var row []any
			err = json.Unmarshal(v, &row)
			if err != nil {
				return fmt.Errorf("unable to unmarshal row: %s", err)
			}
			var targetRow []any
			for _, target := range results.fieldNames {
				for i, field := range tbl.ColumnNames {
					if target == field {
						targetRow = append(targetRow, row[i])
					}
				}
			}
			results.rows = append(results.rows, targetRow)

		}
		return nil
	})
	return results, nil

}

func (p *pgEngine) execute(tree *pgquery.ParseResult) error {
	for _, stmt := range tree.GetStmts() {
		n := stmt.GetStmt()
		if c := n.GetCreateStmt(); c != nil {
			return p.executeCreate(c)
		}
		if c := n.GetInsertStmt(); c != nil {
			return p.executeInsert(c)
		}
		if c := n.GetSelectStmt(); c != nil {
			_, err := p.executeSelect(c)
			return err
		}

		// fmt.Errorf("unknown statement type: %s", stmt)

	}
	return nil
}

func (p *pgEngine) executeDelete() error {
	return p.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(p.bucket)
		if bkt != nil {
			return tx.DeleteBucket(p.bucket)
		}
		return nil

	})

}

type pg struct {
	p *pgEngine
}

func (p *pg) Apply(log *raft.Log) any {
	switch log.Type {
	case raft.LogCommand:
		query := string(log.Data)
		ast, err := pgquery.Parse(query)
		if err != nil {
			panic(fmt.Errorf("Could not parse payload: %s", err))
		}
		err = p.p.execute(ast)
		if err != nil {
			panic(err)
		}
	default:
		panic(fmt.Errorf("unknown raft log type: %#v", log.Type))

	}
	return nil
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(sink raft.SnapshotSink) error {
	return sink.Cancel()
}

func (sn snapshotNoop) Release() {}

func (pf *pg) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

func (pf *pg) Restore(rc io.ReadCloser) error {
	return fmt.Errorf("Nothing to restore")
}

func setupRaft(dir, nodeId, raftAddress string, pf *pg) (*raft.Raft, error) {
	os.MkdirAll(dir, os.ModePerm)
	store, err := raftboltdb.NewBoltStore(path.Join(dir, "bolt"))
	if err != nil {
		return nil, fmt.Errorf("could not create bolt store: %s", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create snapshot store: %s", err)
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, fmt.Errorf("could not resolve address: %s", err)
	}
	transport, err := raft.NewTCPTransport(raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create tcp transport: %s", err)
	}
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeId)
	r, err := raft.NewRaft(raftCfg, pf, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("could not create raft instance: %s", err)
	}
	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeId),
				Address: transport.LocalAddr(),
			},
		},
	})

	return r, nil

}

type httpServer struct {
	r *raft.Raft
}

func main() {
	r, err := pgquery.Parse("CREATE TABLE x (name TEXT, age INT); INSERT INTO x VALUES ('what', 12); SELECT name, id FROM x")

	fmt.Println("============================", r)

	if err != nil {
		panic(err)
	}

	for i, stmt := range r.GetStmts() {

		if i > 0 {
			fmt.Println("error")
		}

		n := stmt.GetStmt()
		// fmt.Println("________________________", n.GetCreateStmt())
		// fmt.Println("________________________", n.GetInsertStmt())
		// fmt.Println("________________________", n.GetSelectStmt())
		if c := n.GetCreateStmt(); c != nil {
			fmt.Println("CREATE", c.Relation.Relname)
			for _, c := range c.TableElts {
				cd := c.GetColumnDef()
				fmt.Println("CREATE FIELD", cd.Colname)
				var fullName []string
				for _, n := range cd.TypeName.Names {
					fmt.Println("________________", n)
					fullName = append(fullName, n.GetString_().Str)
				}

				fmt.Println("CREATE FIELD type", strings.Join(fullName, "."))
			}
			continue
		}

		if i := n.GetInsertStmt(); i != nil {
			fmt.Println("INSERT", i.Relation.Relname)
			slct := i.GetSelectStmt().GetSelectStmt()
			for _, values := range slct.ValuesLists {
				for _, value := range values.GetList().Items {
					if c := value.GetAConst(); c != nil {
						if s := c.Val.GetString_(); s != nil {
							fmt.Println("A STRING", s.Str)
							continue
						}

						if i := c.Val.GetInteger(); i != nil {
							fmt.Println("AN INT", i.Ival)
							continue
						}

						fmt.Println("Unknown value type", value)
						continue
					}

					fmt.Println("unknown value type", value)
				}
			}

			continue
		}

		if s := n.GetSelectStmt(); s != nil {
			for _, c := range s.TargetList {
				fmt.Println("SELECT", c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str)
			}
			fmt.Println("SELECT FROM", s.FromClause[0].GetRangeVar().Relname)
			continue
		}

	}
}
