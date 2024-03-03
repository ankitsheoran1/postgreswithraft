package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	"github.com/jackc/pgproto3/v2"
	bolt "go.etcd.io/bbolt"
	"io"
	"log"
	"net"
	"net/http"
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

func (hs httpServer) addFollowerHandler(w http.ResponseWriter, r *http.Request) {
	followerId := r.URL.Query().Get("id")
	followerAddr := r.URL.Query().Get("addr")

	if hs.r.State() != raft.Leader {
		json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{
			"Not the leader",
		})
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	err := hs.r.AddVoter(raft.ServerID(followerId), raft.ServerAddress(followerAddr), 0, 0).Error()
	if err != nil {
		log.Printf("Failed to add follower: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	w.WriteHeader(http.StatusOK)
}

var dataTypeOIDMap = map[string]uint32{
	"text":            25,
	"pg_catalog.int4": 23,
}

type pgConn struct {
	conn net.Conn
	db   *bolt.DB
	r    *raft.Raft
}

func (pc pgConn) done(buf []byte, msg string) {
	buf = (&pgproto3.CommandComplete{CommandTag: []byte(msg)}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	_, err := pc.conn.Write(buf)
	if err != nil {
		log.Printf("Failed to write query response: %s", err)
	}
}

// func (pc pgConn) writePgResult(res *Result) {
// 	rd := &pgproto3.RowDescription{}
// 	for i, field := range res.fieldNames {
// 		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
// 			Name:        []byte(field),
// 			DataTypeOID: dataTypeOIDMap[res.fieldTypes[i]],
// 		})
// 	}
// 	buf := rd.Encode(nil)
// 	for _, row := range res.rows {
// 		dr := &pgproto3.DataRow{}
// 		for _, value := range row {
// 			bs, err := json.Marshal(value)
// 			if err != nil {
// 				log.Printf("Failed to marshal cell: %s\n", err)
// 				return
// 			}

// 			dr.Values = append(dr.Values, bs)
// 		}

// 		buf = dr.Encode(buf)
// 	}

// 	pc.done(buf, fmt.Sprintf("SELECT %d", len(res.rows)))
// }

func (pc pgConn) writePgResult(res *Result) {
	rd := &pgproto3.RowDescription{}
	for i, field := range res.fieldNames {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:        []byte(field),
			DataTypeOID: dataTypeOIDMap[res.fieldTypes[i]],
		})
	}
	buf := rd.Encode(nil)
	for _, row := range res.rows {
		dr := &pgproto3.DataRow{}
		for _, value := range row {
			bs, err := json.Marshal(value)
			if err != nil {
				log.Printf("Failed to marshal cell: %s\n", err)
				return
			}

			dr.Values = append(dr.Values, bs)
		}

		buf = dr.Encode(buf)
	}

	pc.done(buf, fmt.Sprintf("SELECT %d", len(res.rows)))
}

func (pc pgConn) handleStartupMessage(pgconn *pgproto3.Backend) error {
	startupMessage, err := pgconn.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("Error receiving startup message: %s", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = pc.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("Error sending ready for query: %s", err)
		}

		return nil
	case *pgproto3.SSLRequest:
		_, err = pc.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("Error sending deny SSL request: %s", err)
		}

		return pc.handleStartupMessage(pgconn)
	default:
		return fmt.Errorf("Unknown startup message: %#v", startupMessage)
	}
}

func (pc pgConn) handleMessage(pgc *pgproto3.Backend) error {
	msg, err := pgc.Receive()
	if err != nil {
		return fmt.Errorf("Error receiving message: %s", err)
	}

	switch t := msg.(type) {
	case *pgproto3.Query:
		stmts, err := pgquery.Parse(t.String)
		if err != nil {
			return fmt.Errorf("Error parsing query: %s", err)
		}

		if len(stmts.GetStmts()) > 1 {
			return fmt.Errorf("Only make one request at a time.")
		}

		stmt := stmts.GetStmts()[0]

		// Handle SELECTs here
		s := stmt.GetStmt().GetSelectStmt()
		if s != nil {
			pe := newPgEngine(pc.db)
			res, err := pe.executeSelect(s)
			if err != nil {
				return err
			}

			pc.writePgResult(res)
			return nil
		}

		// Otherwise it's DDL/DML, raftify
		future := pc.r.Apply([]byte(t.String), 500*time.Millisecond)
		if err := future.Error(); err != nil {
			return fmt.Errorf("Could not apply: %s", err)
		}

		e := future.Response()
		if e != nil {
			return fmt.Errorf("Could not apply (internal): %s", e)
		}

		pc.done(nil, strings.ToUpper(strings.Split(t.String, " ")[0])+" ok")
	case *pgproto3.Terminate:
		return nil
	default:
		return fmt.Errorf("Received message other than Query from client: %s", msg)
	}

	return nil
}

func (pc pgConn) handle() {
	pgc := pgproto3.NewBackend(pgproto3.NewChunkReader(pc.conn), pc.conn)
	defer pc.conn.Close()

	err := pc.handleStartupMessage(pgc)
	if err != nil {
		log.Println(err)
		return
	}

	for {
		err := pc.handleMessage(pgc)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func runPgServer(port string, db *bolt.DB, r *raft.Raft) {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		pc := pgConn{conn, db, r}
		go pc.handle()
	}
}

type config struct {
	id       string
	httpPort string
	raftPort string
	pgPort   string
}

func getConfig() config {
	cfg := config{}
	for i, arg := range os.Args[1:] {
		if arg == "--node-id" {
			cfg.id = os.Args[i+2]
			i++
			continue
		}

		if arg == "--http-port" {
			cfg.httpPort = os.Args[i+2]
			i++
			continue
		}

		if arg == "--raft-port" {
			cfg.raftPort = os.Args[i+2]
			i++
			continue
		}

		if arg == "--pg-port" {
			cfg.pgPort = os.Args[i+2]
			i++
			continue
		}
	}

	if cfg.id == "" {
		log.Fatal("Missing required parameter: --node-id")
	}

	if cfg.raftPort == "" {
		log.Fatal("Missing required parameter: --raft-port")
	}

	if cfg.httpPort == "" {
		log.Fatal("Missing required parameter: --http-port")
	}

	if cfg.pgPort == "" {
		log.Fatal("Missing required parameter: --pg-port")
	}

	return cfg
}


func main() {
	cfg := getConfig()

	dataDir := "data"
	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		log.Fatalf("Could not create data directory: %s", err)
	}

	db, err := bolt.Open(path.Join(dataDir, "/data"+cfg.id), 0600, nil)
	if err != nil {
		log.Fatalf("Could not open bolt db: %s", err)
	}
	defer db.Close()

	pe := newPgEngine(db)
	// Start off in clean state
	pe.executeDelete()
	pf := &pg{pe}

	r, err := setupRaft(path.Join(dataDir, "raft"+cfg.id), cfg.id, "localhost:"+cfg.raftPort, pf)
	if err != nil {
		log.Fatal(err)
	}

	hs := httpServer{r}
	http.HandleFunc("/add-follower", hs.addFollowerHandler)
	go func() {
		err := http.ListenAndServe(":"+cfg.httpPort, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	runPgServer(cfg.pgPort, db, r)
}
