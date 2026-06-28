// Registry Server for the room/relay architecture.
//
// Dependencies:
//   go mod init registry-server
//   go get modernc.org/sqlite
//
// Run:
//   go run . -addr :8080 -db registry.db
//
// This server is intentionally lightweight:
// - SQLite persistence only
// - no accounts
// - no messages
// - no media
// - only relay registry + room-to-relay mapping

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultHeartbeatTimeout = 60 * time.Second
	defaultRoomMaxUsers     = 9999
)

type Server struct {
	db               *sql.DB
	heartbeatTimeout time.Duration
	serverStartedAt  time.Time
}

type Relay struct {
	RelayID                  string `json:"relayId"`
	RelayName                string `json:"relayName"`
	PublicURL                string `json:"publicUrl"`
	Region                   string `json:"region,omitempty"`
	IsOnline                 bool   `json:"isOnline"`
	CurrentRooms             int    `json:"currentRooms"`
	CurrentUsers             int    `json:"currentUsers"`
	MaxRooms                 int    `json:"maxRooms"`
	MaxUsers                 int    `json:"maxUsers"`
	OfflineMessagesSupported bool   `json:"offlineMessagesSupported"`
	LastHeartbeat            int64  `json:"lastHeartbeat"`
	CreatedAt                int64  `json:"createdAt"`
	UpdatedAt                int64  `json:"updatedAt"`
}

type Room struct {
	RoomID                 string `json:"roomId"`
	RelayID                string `json:"relayId"`
	PinHash                string `json:"-"`
	HasPin                 bool   `json:"hasPin"`
	MaxUsers               int    `json:"maxUsers"`
	OfflineMessagesEnabled bool   `json:"offlineMessagesEnabled"`
	CreatedAt              int64  `json:"createdAt"`
	UpdatedAt              int64  `json:"updatedAt"`
}

type registerRelayRequest struct {
	RelayID                  string `json:"relayId"`
	RelayName                string `json:"relayName"`
	PublicPort               int    `json:"publicPort"`
	PublicURL                string `json:"publicUrl"`
	Region                   string `json:"region"`
	MaxRooms                 int    `json:"maxRooms"`
	MaxUsers                 int    `json:"maxUsers"`
	OfflineMessagesEnabled   bool   `json:"offlineMessagesEnabled,omitempty"`
	OfflineMessagesSupported bool   `json:"offlineMessagesSupported,omitempty"`
}

type heartbeatRequest struct {
	RelayID                  string `json:"relayId"`
	CurrentRooms             int    `json:"currentRooms"`
	CurrentUsers             int    `json:"currentUsers"`
	IsOnline                 *bool  `json:"isOnline,omitempty"`
	Region                   string `json:"region,omitempty"`
	OfflineMessagesEnabled   *bool  `json:"offlineMessagesEnabled,omitempty"`
	OfflineMessagesSupported *bool  `json:"offlineMessagesSupported,omitempty"`
}

type createRoomRequest struct {
	RoomID                 string `json:"roomId,omitempty"`
	RelayID                string `json:"relayId,omitempty"`
	Region                 string `json:"region,omitempty"`
	Pin                    string `json:"pin,omitempty"`
	MaxUsers               int    `json:"maxUsers"`
	OfflineMessagesEnabled bool   `json:"offlineMessagesEnabled"`
}

type apiError struct {
	Error string `json:"error"`
}

type createRoomResponse struct {
	RoomID                 string `json:"roomId"`
	RelayID                string `json:"relayId"`
	PublicURL              string `json:"publicUrl"`
	MaxUsers               int    `json:"maxUsers"`
	OfflineMessagesEnabled bool   `json:"offlineMessagesEnabled"`
	CreatedAt              int64  `json:"createdAt"`
}

type chooseRelayResponse struct {
	Relay Relay `json:"relay"`
}

var roomMigrationLocks sync.Map

func (s *Server) handleRoomCheckRelay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}

	roomID := strings.TrimPrefix(r.URL.Path, "/api/room/checkrelay/")
	roomID = strings.TrimSpace(roomID)

	if roomID == "" || strings.Contains(roomID, "/") {
		writeJSONError(w, http.StatusBadRequest, "room_id_required")
		return
	}

	lockAny, _ := roomMigrationLocks.LoadOrStore(roomID, &sync.Mutex{})
	lock := lockAny.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	room, relay, err := s.getRoomWithRelay(r.Context(), roomID)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "room_not_found")
		return
	}

	if relay.IsOnline {
		writeJSON(w, http.StatusOK, map[string]any{
			"roomId":                 room.RoomID,
			"offlineMessagesEnabled": room.OfflineMessagesEnabled,
			"relayChanged":           false,
			"relay":                  relay,
		})
		return
	}

	newRelay, err := s.chooseRelay(r.Context(), relay.Region, room.OfflineMessagesEnabled)
	if err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, "no_available_relay")
		return
	}

	if err := registerRoomOnRelay(newRelay.PublicURL, room.RoomID, room.PinHash, room.MaxUsers, room.OfflineMessagesEnabled, newRelay.OfflineMessagesSupported); err != nil {
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}

	_, err = s.db.ExecContext(r.Context(),
		`UPDATE rooms SET relay_id = ?, updated_at = ? WHERE room_id = ?`,
		newRelay.RelayID, time.Now().UTC().Unix(), room.RoomID,
	)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"roomId":                 room.RoomID,
		"offlineMessagesEnabled": room.OfflineMessagesEnabled,
		"relayChanged":           true,
		"oldRelayId":             relay.RelayID,
		"relay":                  newRelay,
	})
}

func main() {
	var (
		addr      = flag.String("addr", ":80", "HTTP listen address")
		dbPath    = flag.String("db", "registry.db", "SQLite database file")
		heartbeat = flag.Duration("heartbeat-timeout", defaultHeartbeatTimeout, "relay heartbeat timeout")
	)
	flag.Parse()

	if err := os.MkdirAll(".", 0o755); err != nil {
		log.Fatalf("mkdir: %v", err)
	}

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := initSchema(ctx, db); err != nil {
		log.Fatalf("init schema: %v", err)
	}

	srv := &Server{
		db:               db,
		heartbeatTimeout: *heartbeat,
		serverStartedAt:  time.Now().UTC(),
	}

	go srv.maintenanceLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", srv.handleHealth)
	mux.HandleFunc("/api/relays", srv.handleRelays)
	mux.HandleFunc("/api/relay/register", srv.handleRelayRegister)
	mux.HandleFunc("/api/relay/heartbeat", srv.handleRelayHeartbeat)
	mux.HandleFunc("/api/relay/choose", srv.handleRelayChoose)
	mux.HandleFunc("/api/room/create", srv.handleRoomCreate)
	mux.HandleFunc("/api/room/", srv.handleRoomByID)
	mux.HandleFunc("/api/room/checkrelay/", srv.handleRoomCheckRelay)

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           withMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("registry server listening on %s", *addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func initSchema(ctx context.Context, db *sql.DB) error {
	schema := []string{
		`PRAGMA foreign_keys = ON;`,
		`CREATE TABLE IF NOT EXISTS relays (
			relay_id       TEXT PRIMARY KEY,
			relay_name     TEXT NOT NULL,
			public_url     TEXT NOT NULL UNIQUE,
			region         TEXT NOT NULL DEFAULT 'other',
			is_online      INTEGER NOT NULL DEFAULT 1,
			current_rooms  INTEGER NOT NULL DEFAULT 0,
			current_users  INTEGER NOT NULL DEFAULT 0,
			max_rooms      INTEGER NOT NULL DEFAULT 1000,
			max_users      INTEGER NOT NULL DEFAULT 10000,
			offline_messages_supported INTEGER NOT NULL DEFAULT 0,
			last_heartbeat INTEGER NOT NULL,
			created_at     INTEGER NOT NULL,
			updated_at     INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS rooms (
			room_id     TEXT PRIMARY KEY,
			relay_id    TEXT NOT NULL,
			pin_hash TEXT NOT NULL DEFAULT '',
			max_users   INTEGER NOT NULL DEFAULT 4,
			offline_messages_enabled INTEGER NOT NULL DEFAULT 0,
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL,
			FOREIGN KEY (relay_id) REFERENCES relays(relay_id)
				ON UPDATE CASCADE
				ON DELETE RESTRICT
		);`,
		`CREATE INDEX IF NOT EXISTS idx_relays_online_load ON relays (is_online, current_rooms, current_users, last_heartbeat);`,
		`CREATE INDEX IF NOT EXISTS idx_relays_region ON relays (region);`,
		`CREATE INDEX IF NOT EXISTS idx_rooms_relay_id ON rooms (relay_id);`,
		`CREATE INDEX IF NOT EXISTS idx_rooms_updated_at ON rooms (updated_at);`,
	}

	for _, stmt := range schema {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if err := ensureRelaysOfflineMessagesSupportedColumn(ctx, db); err != nil {
		return err
	}
	return ensureRoomsOfflineMessagesColumn(ctx, db)
}

func ensureRelaysOfflineMessagesSupportedColumn(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(relays)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid     int
			name    string
			colType string
			notNull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(name, "offline_messages_supported") {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `ALTER TABLE relays ADD COLUMN offline_messages_supported INTEGER NOT NULL DEFAULT 0`)
	return err
}

func ensureRoomsOfflineMessagesColumn(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(rooms)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid     int
			name    string
			colType string
			notNull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(name, "offline_messages_enabled") {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `ALTER TABLE rooms ADD COLUMN offline_messages_enabled INTEGER NOT NULL DEFAULT 0`)
	return err
}

func withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                      true,
		"startedAt":               s.serverStartedAt.Unix(),
		"heartbeatTimeoutSeconds": int(s.heartbeatTimeout.Seconds()),
	})
}

func (s *Server) handleRelays(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		relays, err := s.listRelays(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"relays": relays})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
	}
}

func (s *Server) handleRelayRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}

	var req registerRelayRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_json")
		return
	}

	if req.PublicPort <= 0 ||
		strings.TrimSpace(req.RelayName) == "" {
		writeJSONError(w, http.StatusBadRequest, "relay_name_and_public_url_required")
		return
	}

	if req.MaxRooms <= 0 {
		req.MaxRooms = 1000
	}
	if req.MaxUsers <= 0 {
		req.MaxUsers = 10000
	}
	if strings.TrimSpace(req.RelayID) == "" {
		req.RelayID = newID("relay")
	}

	//	host, _, err := net.SplitHostPort(r.RemoteAddr)
	//#if err != nil {
	//    writeJSONError(w, http.StatusBadRequest, "invalid_remote_addr")
	//    return
	//}

	//publicURL := fmt.Sprintf("%s:%d", host, req.PublicPort)
	publicURL := strings.TrimSpace(req.PublicURL)
	now := time.Now().UTC().Unix()
	offlineMessagesSupported := boolToInt(req.OfflineMessagesSupported)

	_, err := s.db.ExecContext(r.Context(), `
		INSERT INTO relays (
			relay_id, relay_name, public_url, region, is_online,
			current_rooms, current_users, max_rooms, max_users,
			offline_messages_supported, last_heartbeat, created_at, updated_at
		) VALUES (?, ?, ?, ?, 1, 0, 0, ?, ?, ?, ?, ?, ?)
ON CONFLICT(public_url) DO UPDATE SET
    relay_id = excluded.relay_id,
    relay_name = excluded.relay_name,
    public_url = excluded.public_url,
    region = excluded.region,
    is_online = 1,
    current_rooms = 0,
    current_users = 0,
    max_rooms = excluded.max_rooms,
    max_users = excluded.max_users,
    offline_messages_supported = excluded.offline_messages_supported,
    last_heartbeat = excluded.last_heartbeat,
    updated_at = excluded.updated_at
	`, req.RelayID, req.RelayName, publicURL, normalizeRegion(req.Region), req.MaxRooms, req.MaxUsers, offlineMessagesSupported, now, now, now)
	if err != nil {
		writeJSONError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"relayId":                  req.RelayID,
		"relayName":                req.RelayName,
		"publicUrl":                publicURL,
		"region":                   req.Region,
		"offlineMessagesSupported": req.OfflineMessagesSupported,
		"registeredAt":             now,
	})
}

func (s *Server) handleRelayHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}

	var req heartbeatRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_json")
		return
	}

	if strings.TrimSpace(req.RelayID) == "" {
		writeJSONError(w, http.StatusBadRequest, "relay_id_required")
		return
	}

	now := time.Now().UTC().Unix()
	isOnline := 1
	if req.IsOnline != nil && !*req.IsOnline {
		isOnline = 0
	}
	offlineMessagesSupported := 0
	var offlineMessagesSupportedParam any
	if req.OfflineMessagesSupported != nil && *req.OfflineMessagesSupported {
		offlineMessagesSupported = 1
		offlineMessagesSupportedParam = offlineMessagesSupported
	} else if req.OfflineMessagesSupported != nil {
		offlineMessagesSupportedParam = offlineMessagesSupported
	}

	res, err := s.db.ExecContext(r.Context(), `
		UPDATE relays
		SET current_rooms = ?,
		    current_users = ?,
		    is_online = ?,
		    last_heartbeat = ?,
		    updated_at = ?,
		    region = COALESCE(NULLIF(?, ''), region),
		    offline_messages_supported = CASE WHEN ? IS NULL THEN offline_messages_supported ELSE ? END
		WHERE relay_id = ?
	`, req.CurrentRooms, req.CurrentUsers, isOnline, now, now, req.Region, offlineMessagesSupportedParam, offlineMessagesSupported, req.RelayID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		writeJSONError(w, http.StatusNotFound, "relay_not_found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"relayId":   req.RelayID,
		"ok":        true,
		"updatedAt": now,
	})
}

func (s *Server) handleRelayChoose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	region := strings.TrimSpace(r.URL.Query().Get("region"))
	offlineMessagesRequired, _ := strconv.ParseBool(strings.TrimSpace(r.URL.Query().Get("offlineMessagesEnabled")))

	relay, err := s.chooseRelay(r.Context(), region, offlineMessagesRequired)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, chooseRelayResponse{Relay: relay})
}

func (s *Server) handleRoomCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}

	var req createRoomRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_json")
		return
	}

	if req.MaxUsers <= 0 {
		req.MaxUsers = defaultRoomMaxUsers
	}

	if strings.TrimSpace(req.RoomID) == "" {
		req.RoomID = newID("room")
	}

	tx, err := s.db.BeginTx(r.Context(), nil)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer func() { _ = tx.Rollback() }()

	var relay Relay
	if strings.TrimSpace(req.RelayID) != "" {
		relay, err = relayByIDTx(r.Context(), tx, req.RelayID)
		if err != nil {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		if req.OfflineMessagesEnabled && !relay.OfflineMessagesSupported {
			writeJSONError(w, http.StatusConflict, "relay_offline_messages_unavailable")
			return
		}
	} else {
		relay, err = s.chooseRelayTx(r.Context(), tx, strings.TrimSpace(req.Region), req.OfflineMessagesEnabled)
		if err != nil {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
	}

	if relay.CurrentRooms >= relay.MaxRooms {
		writeJSONError(w, http.StatusConflict, "relay_room_capacity_reached")
		return
	}
	if relay.CurrentUsers+req.MaxUsers > relay.MaxUsers {
		writeJSONError(w, http.StatusConflict, "relay_user_capacity_reached")
		return
	}
	//pinHash := ""
	now := time.Now().UTC().Unix()

	pinHash := ""

	if strings.TrimSpace(req.Pin) != "" {
		pinHash = hashPin(req.Pin)
	}

	_, err = tx.ExecContext(r.Context(), `
    INSERT INTO rooms (
        room_id,
        relay_id,
        pin_hash,
        max_users,
        offline_messages_enabled,
        created_at,
        updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
`,
		req.RoomID,
		relay.RelayID,
		pinHash,
		req.MaxUsers,
		boolToInt(req.OfflineMessagesEnabled),
		now,
		now,
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			writeJSONError(w, http.StatusConflict, "room_already_exists")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	_, err = tx.ExecContext(r.Context(), `
		UPDATE relays
		SET current_rooms = current_rooms + 1,
		    updated_at = ?
		WHERE relay_id = ?
	`, now, relay.RelayID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := tx.Commit(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	err = registerRoomOnRelay(
		relay.PublicURL,
		req.RoomID,
		pinHash,
		req.MaxUsers,
		req.OfflineMessagesEnabled,
		relay.OfflineMessagesSupported,
	)

	if err != nil {
		tx.Rollback()
		writeJSONError(
			w,
			http.StatusBadGateway,
			err.Error(),
		)
		return
	}

	writeJSON(w, http.StatusCreated, createRoomResponse{
		RoomID:                 req.RoomID,
		RelayID:                relay.RelayID,
		PublicURL:              relay.PublicURL,
		MaxUsers:               req.MaxUsers,
		OfflineMessagesEnabled: req.OfflineMessagesEnabled,
		CreatedAt:              now,
	})
}

func (s *Server) handleRoomByID(w http.ResponseWriter, r *http.Request) {
	roomID := strings.TrimPrefix(r.URL.Path, "/api/room/")
	roomID = strings.TrimSpace(roomID)

	if roomID == "" || strings.Contains(roomID, "/") {
		writeJSONError(w, http.StatusBadRequest, "room_id_required")
		return
	}

	switch r.Method {

	case http.MethodGet:

		room, relay, err := s.getRoomWithRelay(r.Context(), roomID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSONError(w, http.StatusNotFound, "room_not_found")
			} else {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
			}
			return
		}

		// Ensure the room exists on the assigned relay.
		err = registerRoomOnRelay(
			relay.PublicURL,
			room.RoomID,
			room.PinHash,
			room.MaxUsers,
			room.OfflineMessagesEnabled,
			relay.OfflineMessagesSupported,
		)

		if err != nil {

			// Assigned relay is unavailable. Pick another relay.
			newRelay, err := s.chooseRelay(r.Context(), relay.Region, room.OfflineMessagesEnabled)
			if err != nil {
				writeJSONError(w, http.StatusServiceUnavailable, "no_available_relay")
				return
			}

			// Register room on the new relay.
			if err := registerRoomOnRelay(
				newRelay.PublicURL,
				room.RoomID,
				room.PinHash,
				room.MaxUsers,
				room.OfflineMessagesEnabled,
				newRelay.OfflineMessagesSupported,
			); err != nil {
				writeJSONError(w, http.StatusBadGateway, err.Error())
				return
			}

			// Update room ownership.
			_, err = s.db.ExecContext(
				r.Context(),
				`UPDATE rooms
				 SET relay_id = ?, updated_at = ?
				 WHERE room_id = ?`,
				newRelay.RelayID,
				time.Now().UTC().Unix(),
				room.RoomID,
			)
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}

			room.RelayID = newRelay.RelayID
			relay = newRelay
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"room":  room,
			"relay": relay,
		})

	case http.MethodDelete:

		if err := s.deleteRoom(r.Context(), roomID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSONError(w, http.StatusNotFound, "room_not_found")
			} else {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
			}
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"ok":     true,
			"roomId": roomID,
		})

	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
	}
}

func (s *Server) listRelays(ctx context.Context) ([]Relay, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT relay_id, relay_name, public_url, region, is_online, current_rooms, current_users, max_rooms, max_users, offline_messages_supported, last_heartbeat, created_at, updated_at
		FROM relays
		ORDER BY is_online DESC, current_users ASC, current_rooms ASC, last_heartbeat DESC, relay_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var relays []Relay
	for rows.Next() {
		var r Relay
		var online int
		var offlineMessagesSupported int
		if err := rows.Scan(&r.RelayID, &r.RelayName, &r.PublicURL, &r.Region, &online, &r.CurrentRooms, &r.CurrentUsers, &r.MaxRooms, &r.MaxUsers, &offlineMessagesSupported, &r.LastHeartbeat, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.IsOnline = online == 1
		r.OfflineMessagesSupported = offlineMessagesSupported == 1
		relays = append(relays, r)
	}
	return relays, rows.Err()
}

func (s *Server) chooseRelay(ctx context.Context, region string, offlineMessagesRequired ...bool) (Relay, error) {
	requireOfflineMessages := len(offlineMessagesRequired) > 0 && offlineMessagesRequired[0]
	rows, err := s.db.QueryContext(ctx, `
		SELECT relay_id, relay_name, public_url, region, is_online, current_rooms, current_users, max_rooms, max_users, offline_messages_supported, last_heartbeat, created_at, updated_at
		FROM relays
		WHERE is_online = 1
		ORDER BY current_users ASC, current_rooms ASC, last_heartbeat DESC, relay_id ASC
	`)
	if err != nil {
		return Relay{}, err
	}
	defer rows.Close()

	var candidates []Relay
	now := time.Now().UTC().Unix()

	for rows.Next() {
		var r Relay
		var online int
		var offlineMessagesSupported int
		if err := rows.Scan(&r.RelayID, &r.RelayName, &r.PublicURL, &r.Region, &online, &r.CurrentRooms, &r.CurrentUsers, &r.MaxRooms, &r.MaxUsers, &offlineMessagesSupported, &r.LastHeartbeat, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return Relay{}, err
		}
		if now-r.LastHeartbeat > int64(s.heartbeatTimeout.Seconds()) {
			continue
		}
		r.IsOnline = online == 1
		r.OfflineMessagesSupported = offlineMessagesSupported == 1
		if requireOfflineMessages && !r.OfflineMessagesSupported {
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return Relay{}, err
	}
	if len(candidates) == 0 {
		if requireOfflineMessages {
			return Relay{}, errors.New("no_offline_message_relay")
		}
		return Relay{}, errors.New("no_available_relay")
	}

	if region != "" {
		var filtered []Relay
		for _, r := range candidates {
			if strings.EqualFold(strings.TrimSpace(r.Region), region) {
				filtered = append(filtered, r)
			}
		}
		if len(filtered) > 0 {
			candidates = filtered
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		ai := relayScore(candidates[i])
		aj := relayScore(candidates[j])
		if ai != aj {
			return ai < aj
		}
		if candidates[i].CurrentUsers != candidates[j].CurrentUsers {
			return candidates[i].CurrentUsers < candidates[j].CurrentUsers
		}
		if candidates[i].CurrentRooms != candidates[j].CurrentRooms {
			return candidates[i].CurrentRooms < candidates[j].CurrentRooms
		}
		return candidates[i].RelayID < candidates[j].RelayID
	})

	return candidates[0], nil
}

func (s *Server) chooseRelayTx(ctx context.Context, tx *sql.Tx, region string, requireOfflineMessages bool) (Relay, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT relay_id, relay_name, public_url, region, is_online, current_rooms, current_users, max_rooms, max_users, offline_messages_supported, last_heartbeat, created_at, updated_at
		FROM relays
		WHERE is_online = 1
		ORDER BY current_users ASC, current_rooms ASC, last_heartbeat DESC, relay_id ASC
	`)
	if err != nil {
		return Relay{}, err
	}
	defer rows.Close()

	var candidates []Relay
	now := time.Now().UTC().Unix()

	for rows.Next() {
		var r Relay
		var online int
		var offlineMessagesSupported int
		if err := rows.Scan(&r.RelayID, &r.RelayName, &r.PublicURL, &r.Region, &online, &r.CurrentRooms, &r.CurrentUsers, &r.MaxRooms, &r.MaxUsers, &offlineMessagesSupported, &r.LastHeartbeat, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return Relay{}, err
		}
		if now-r.LastHeartbeat > int64(s.heartbeatTimeout.Seconds()) {
			continue
		}
		r.IsOnline = online == 1
		r.OfflineMessagesSupported = offlineMessagesSupported == 1
		if requireOfflineMessages && !r.OfflineMessagesSupported {
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return Relay{}, err
	}
	if len(candidates) == 0 {
		if requireOfflineMessages {
			return Relay{}, errors.New("no_offline_message_relay")
		}
		return Relay{}, errors.New("no_available_relay")
	}

	if region != "" {
		var filtered []Relay
		for _, r := range candidates {
			if strings.EqualFold(strings.TrimSpace(r.Region), region) {
				filtered = append(filtered, r)
			}
		}
		if len(filtered) > 0 {
			candidates = filtered
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return relayScore(candidates[i]) < relayScore(candidates[j])
	})

	return candidates[0], nil
}

func relayScore(r Relay) int {
	// Lower is better.
	// Rooms are weighted more because fewer rooms usually means a cleaner relay.
	return (r.CurrentRooms * 10) + r.CurrentUsers
}

func relayByIDTx(ctx context.Context, tx *sql.Tx, relayID string) (Relay, error) {
	var r Relay
	var online int
	var offlineMessagesSupported int
	err := tx.QueryRowContext(ctx, `
		SELECT relay_id, relay_name, public_url, region, is_online, current_rooms, current_users, max_rooms, max_users, offline_messages_supported, last_heartbeat, created_at, updated_at
		FROM relays
		WHERE relay_id = ?
	`, relayID).Scan(&r.RelayID, &r.RelayName, &r.PublicURL, &r.Region, &online, &r.CurrentRooms, &r.CurrentUsers, &r.MaxRooms, &r.MaxUsers, &offlineMessagesSupported, &r.LastHeartbeat, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return Relay{}, err
	}
	r.IsOnline = online == 1
	r.OfflineMessagesSupported = offlineMessagesSupported == 1
	return r, nil
}

func (s *Server) getRoomWithRelay(ctx context.Context, roomID string) (Room, Relay, error) {
	var room Room
	var offlineMessagesEnabled int
	err := s.db.QueryRowContext(ctx, `
		SELECT room_id, relay_id, COALESCE(pin_hash,''), max_users, offline_messages_enabled, created_at, updated_at
		FROM rooms
		WHERE room_id = ?
	`, roomID).Scan(&room.RoomID, &room.RelayID, &room.PinHash, &room.MaxUsers, &offlineMessagesEnabled, &room.CreatedAt, &room.UpdatedAt)
	if err != nil {
		return Room{}, Relay{}, err
	}
	room.OfflineMessagesEnabled = offlineMessagesEnabled == 1

	var relay Relay
	var online int
	var relayOfflineMessagesSupported int
	err = s.db.QueryRowContext(ctx, `
		SELECT relay_id, relay_name, public_url, region, is_online, current_rooms, current_users, max_rooms, max_users, offline_messages_supported, last_heartbeat, created_at, updated_at
		FROM relays
		WHERE relay_id = ?
	`, room.RelayID).Scan(&relay.RelayID, &relay.RelayName, &relay.PublicURL, &relay.Region, &online, &relay.CurrentRooms, &relay.CurrentUsers, &relay.MaxRooms, &relay.MaxUsers, &relayOfflineMessagesSupported, &relay.LastHeartbeat, &relay.CreatedAt, &relay.UpdatedAt)
	if err != nil {
		return Room{}, Relay{}, err
	}
	room.HasPin = strings.TrimSpace(room.PinHash) != ""
	relay.IsOnline = online == 1
	relay.OfflineMessagesSupported = relayOfflineMessagesSupported == 1
	return room, relay, nil
}

func (s *Server) deleteRoom(ctx context.Context, roomID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var relayID string
	if err := tx.QueryRowContext(ctx, `SELECT relay_id FROM rooms WHERE room_id = ?`, roomID).Scan(&relayID); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM rooms WHERE room_id = ?`, roomID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE relays
		SET current_rooms = CASE WHEN current_rooms > 0 THEN current_rooms - 1 ELSE 0 END,
		    updated_at = ?
		WHERE relay_id = ?
	`, time.Now().UTC().Unix(), relayID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Server) maintenanceLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.markStaleRelaysOffline()
	}
}

func (s *Server) markStaleRelaysOffline() {
	now := time.Now().UTC().Unix()
	cutoff := now - int64(s.heartbeatTimeout.Seconds())

	_, err := s.db.Exec(`UPDATE relays SET is_online = 0, updated_at = ? WHERE last_heartbeat < ? AND is_online = 1`, now, cutoff)
	if err != nil {
		log.Printf("maintenance error: %v", err)
	}
}

func decodeJSON(bodyReader io.ReadCloser, dst any) error {
	defer bodyReader.Close()
	dec := json.NewDecoder(bodyReader)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, apiError{Error: msg})
}

func nullIfEmpty(v string) any {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	return v
}

func normalizeRegion(v string) string {
	v = strings.TrimSpace(v)

	if v == "" {
		return "other"
	}

	return v
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func generateRoomCode() string {
	const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

	b := make([]byte, 6)

	randomBytes := make([]byte, 6)
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}

	for i := range b {
		b[i] = chars[int(randomBytes[i])%len(chars)]
	}

	return string(b)
}

func newID(prefix string) string {
	switch prefix {

	case "room":
		return generateRoomCode()

	case "relay":
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return fmt.Sprintf("relay-%d", time.Now().UnixNano())
		}
		return "relay-" + hex.EncodeToString(b[:])

	default:
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
		}
		return prefix + "-" + hex.EncodeToString(b[:])
	}
}

func hashPin(pin string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(pin)))
	return hex.EncodeToString(sum[:])
}

func registerRoomOnRelay(
	relayURL string,
	roomID string,
	pinHash string,
	maxUsers int,
	offlineMessagesEnabled bool,
	offlineMessagesSupported bool,
) error {

	payload := map[string]any{
		"roomId":   roomID,
		"pinHash":  pinHash,
		"maxUsers": maxUsers,
	}
	if offlineMessagesEnabled || offlineMessagesSupported {
		payload["offlineMessagesEnabled"] = offlineMessagesEnabled
	}

	body, _ := json.Marshal(payload)

	relayAPIURL := "https://" + strings.TrimSpace(relayURL)

	resp, err := http.Post(
		relayAPIURL+"/internal/rooms/register",
		"application/json",
		bytes.NewReader(body),
	)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		var relayErr apiError
		if err := json.Unmarshal(body, &relayErr); err == nil && strings.TrimSpace(relayErr.Error) != "" {
			return errors.New(relayErr.Error)
		}
		var genericErr struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &genericErr); err == nil && strings.TrimSpace(genericErr.Error) != "" {
			return errors.New(genericErr.Error)
		}
		return fmt.Errorf("relay register failed with status %d", resp.StatusCode)
	}

	return nil
}
