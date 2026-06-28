// Registry Server for the room/relay architecture.
//
// Dependencies:
//   go mod init registry-server
//   go get modernc.org/sqlite
//   go get go.mongodb.org/mongo-driver/v2
//
// Run:
//   go run . -addr :8080 -db registry.db
//
// This server is intentionally lightweight:
// - SQLite persistence by default, optional MongoDB persistence
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

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	_ "modernc.org/sqlite"
)

const (
	defaultHeartbeatTimeout = 60 * time.Second
	defaultRoomMaxUsers     = 9999
	defaultSQLiteDBPath     = "registry.db"
	defaultMongoDatabase    = "openchat_registry"
	defaultRelaysCollection = "relays"
	defaultRoomsCollection  = "rooms"
	storageSQLite           = "sqlite"
	storageMongoDB          = "mongodb"
)

var (
	errStoreNotFound  = errors.New("not_found")
	errStoreDuplicate = errors.New("duplicate")
)

type Server struct {
	store            registryStore
	storageBackend   string
	heartbeatTimeout time.Duration
	serverStartedAt  time.Time
}

type registryStore interface {
	Close(ctx context.Context) error
	RegisterRelay(ctx context.Context, req registerRelayRequest, publicURL string, now int64) error
	UpdateRelayHeartbeat(ctx context.Context, req heartbeatRequest, now int64) error
	ListRelays(ctx context.Context) ([]Relay, error)
	ChooseRelay(ctx context.Context, region string, requireOfflineMessages bool, heartbeatTimeout time.Duration) (Relay, error)
	GetRelayByID(ctx context.Context, relayID string) (Relay, error)
	CreateRoom(ctx context.Context, req createRoomRequest, relay Relay, pinHash string, now int64) error
	UpdateRoomRelay(ctx context.Context, roomID, relayID string, updatedAt int64) error
	GetRoomWithRelay(ctx context.Context, roomID string) (Room, Relay, error)
	DeleteRoom(ctx context.Context, roomID string) error
	MarkStaleRelaysOffline(ctx context.Context, cutoff int64, now int64) error
}

type Config struct {
	Addr                    string `json:"addr"`
	DBPath                  string `json:"dbPath"`
	StorageBackend          string `json:"storageBackend"`
	HeartbeatTimeoutSeconds int    `json:"heartbeatTimeoutSeconds"`
	MongoURI                string `json:"mongoUri"`
	MongoDatabase           string `json:"mongoDatabase"`
	MongoRelaysCollection   string `json:"mongoRelaysCollection"`
	MongoRoomsCollection    string `json:"mongoRoomsCollection"`
}

type sqliteStore struct {
	db *sql.DB
}

type mongoRegistryStore struct {
	client *mongo.Client
	relays *mongo.Collection
	rooms  *mongo.Collection
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

	err = s.store.UpdateRoomRelay(r.Context(), room.RoomID, newRelay.RelayID, time.Now().UTC().Unix())
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
	configPath := flag.String("config", getenv("REGISTRY_CONFIG", "registry_config.json"), "optional registry config json")
	addrFlag := flag.String("addr", "", "HTTP listen address")
	dbPathFlag := flag.String("db", "", "SQLite database file")
	storageFlag := flag.String("storage", "", "storage backend: sqlite or mongodb")
	mongoURIFlag := flag.String("mongo-uri", "", "MongoDB URI")
	mongoDatabaseFlag := flag.String("mongo-database", "", "MongoDB database")
	mongoRelaysCollectionFlag := flag.String("mongo-relays-collection", "", "MongoDB relays collection")
	mongoRoomsCollectionFlag := flag.String("mongo-rooms-collection", "", "MongoDB rooms collection")
	heartbeatFlag := flag.Duration("heartbeat-timeout", 0, "relay heartbeat timeout")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	applyEnvConfig(cfg)
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "addr":
			cfg.Addr = *addrFlag
		case "db":
			cfg.DBPath = *dbPathFlag
		case "storage":
			cfg.StorageBackend = *storageFlag
		case "mongo-uri":
			cfg.MongoURI = *mongoURIFlag
		case "mongo-database":
			cfg.MongoDatabase = *mongoDatabaseFlag
		case "mongo-relays-collection":
			cfg.MongoRelaysCollection = *mongoRelaysCollectionFlag
		case "mongo-rooms-collection":
			cfg.MongoRoomsCollection = *mongoRoomsCollectionFlag
		case "heartbeat-timeout":
			cfg.HeartbeatTimeoutSeconds = int(heartbeatFlag.Seconds())
		}
	})
	cfg.normalize()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	store, err := openRegistryStore(ctx, cfg)
	if err != nil {
		log.Fatalf("open %s store: %v", cfg.StorageBackend, err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.Close(ctx); err != nil {
			log.Printf("close store: %v", err)
		}
	}()

	srv := &Server{
		store:            store,
		storageBackend:   cfg.StorageBackend,
		heartbeatTimeout: time.Duration(cfg.HeartbeatTimeoutSeconds) * time.Second,
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
		Addr:              cfg.Addr,
		Handler:           withMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("registry server listening on %s using %s storage", cfg.Addr, cfg.StorageBackend)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func defaultConfig() *Config {
	return &Config{
		Addr:                    ":80",
		DBPath:                  defaultSQLiteDBPath,
		StorageBackend:          storageSQLite,
		HeartbeatTimeoutSeconds: int(defaultHeartbeatTimeout / time.Second),
		MongoDatabase:           defaultMongoDatabase,
		MongoRelaysCollection:   defaultRelaysCollection,
		MongoRoomsCollection:    defaultRoomsCollection,
	}
}

func loadConfig(path string) (*Config, error) {
	cfg := defaultConfig()
	path = strings.TrimSpace(path)
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func applyEnvConfig(cfg *Config) {
	cfg.Addr = getenv("REGISTRY_ADDR", cfg.Addr)
	cfg.DBPath = getenv("REGISTRY_DB_PATH", getenv("DB_PATH", cfg.DBPath))
	cfg.StorageBackend = getenv("REGISTRY_STORAGE_BACKEND", getenv("STORAGE_BACKEND", cfg.StorageBackend))
	cfg.MongoURI = getenv("REGISTRY_MONGO_URI", getenv("MONGO_URI", cfg.MongoURI))
	cfg.MongoDatabase = getenv("REGISTRY_MONGO_DATABASE", getenv("MONGO_DATABASE", cfg.MongoDatabase))
	cfg.MongoRelaysCollection = getenv("REGISTRY_MONGO_RELAYS_COLLECTION", getenv("MONGO_RELAYS_COLLECTION", cfg.MongoRelaysCollection))
	cfg.MongoRoomsCollection = getenv("REGISTRY_MONGO_ROOMS_COLLECTION", getenv("MONGO_ROOMS_COLLECTION", cfg.MongoRoomsCollection))
	if v := getenv("REGISTRY_HEARTBEAT_TIMEOUT_SECONDS", getenv("HEARTBEAT_TIMEOUT_SECONDS", "")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.HeartbeatTimeoutSeconds = n
		}
	}
}

func (cfg *Config) normalize() {
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	if cfg.Addr == "" {
		cfg.Addr = ":80"
	}
	cfg.DBPath = strings.TrimSpace(cfg.DBPath)
	if cfg.DBPath == "" {
		cfg.DBPath = defaultSQLiteDBPath
	}
	cfg.StorageBackend = strings.ToLower(strings.TrimSpace(cfg.StorageBackend))
	if cfg.StorageBackend == "" {
		cfg.StorageBackend = storageSQLite
	}
	cfg.MongoURI = strings.TrimSpace(cfg.MongoURI)
	cfg.MongoDatabase = strings.TrimSpace(cfg.MongoDatabase)
	if cfg.MongoDatabase == "" {
		cfg.MongoDatabase = defaultMongoDatabase
	}
	cfg.MongoRelaysCollection = strings.TrimSpace(cfg.MongoRelaysCollection)
	if cfg.MongoRelaysCollection == "" {
		cfg.MongoRelaysCollection = defaultRelaysCollection
	}
	cfg.MongoRoomsCollection = strings.TrimSpace(cfg.MongoRoomsCollection)
	if cfg.MongoRoomsCollection == "" {
		cfg.MongoRoomsCollection = defaultRoomsCollection
	}
	if cfg.HeartbeatTimeoutSeconds <= 0 {
		cfg.HeartbeatTimeoutSeconds = int(defaultHeartbeatTimeout / time.Second)
	}
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func openRegistryStore(ctx context.Context, cfg *Config) (registryStore, error) {
	switch cfg.StorageBackend {
	case storageSQLite:
		return openSQLiteStore(ctx, cfg.DBPath)
	case storageMongoDB, "mongo":
		cfg.StorageBackend = storageMongoDB
		return openMongoRegistryStore(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported storage backend %q", cfg.StorageBackend)
	}
}

func openSQLiteStore(ctx context.Context, dbPath string) (*sqliteStore, error) {
	if err := os.MkdirAll(".", 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)
	if err := initSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &sqliteStore{db: db}, nil
}

func (s *sqliteStore) Close(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *sqliteStore) RegisterRelay(ctx context.Context, req registerRelayRequest, publicURL string, now int64) error {
	_, err := s.db.ExecContext(ctx, `
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
	`, req.RelayID, req.RelayName, publicURL, normalizeRegion(req.Region), req.MaxRooms, req.MaxUsers, boolToInt(req.OfflineMessagesSupported), now, now, now)
	return err
}

func (s *sqliteStore) UpdateRelayHeartbeat(ctx context.Context, req heartbeatRequest, now int64) error {
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

	res, err := s.db.ExecContext(ctx, `
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
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return errStoreNotFound
	}
	return nil
}

func (s *sqliteStore) ListRelays(ctx context.Context) ([]Relay, error) {
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
		r, err := scanRelay(rows.Scan)
		if err != nil {
			return nil, err
		}
		relays = append(relays, r)
	}
	return relays, rows.Err()
}

func (s *sqliteStore) ChooseRelay(ctx context.Context, region string, requireOfflineMessages bool, heartbeatTimeout time.Duration) (Relay, error) {
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
		r, err := scanRelay(rows.Scan)
		if err != nil {
			return Relay{}, err
		}
		if now-r.LastHeartbeat > int64(heartbeatTimeout.Seconds()) {
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return Relay{}, err
	}
	return chooseBestRelay(candidates, region, requireOfflineMessages)
}

func (s *sqliteStore) GetRelayByID(ctx context.Context, relayID string) (Relay, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT relay_id, relay_name, public_url, region, is_online, current_rooms, current_users, max_rooms, max_users, offline_messages_supported, last_heartbeat, created_at, updated_at
		FROM relays
		WHERE relay_id = ?
	`, relayID)
	r, err := scanRelay(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Relay{}, errStoreNotFound
	}
	return r, err
}

func (s *sqliteStore) CreateRoom(ctx context.Context, req createRoomRequest, relay Relay, pinHash string, now int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO rooms (
			room_id, relay_id, pin_hash, max_users, offline_messages_enabled, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, req.RoomID, relay.RelayID, pinHash, req.MaxUsers, boolToInt(req.OfflineMessagesEnabled), now, now)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return errStoreDuplicate
		}
		return err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE relays
		SET current_rooms = current_rooms + 1,
		    updated_at = ?
		WHERE relay_id = ?
	`, now, relay.RelayID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *sqliteStore) UpdateRoomRelay(ctx context.Context, roomID, relayID string, updatedAt int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE rooms SET relay_id = ?, updated_at = ? WHERE room_id = ?`, relayID, updatedAt, roomID)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return errStoreNotFound
	}
	return nil
}

func (s *sqliteStore) GetRoomWithRelay(ctx context.Context, roomID string) (Room, Relay, error) {
	var room Room
	var offlineMessagesEnabled int
	err := s.db.QueryRowContext(ctx, `
		SELECT room_id, relay_id, COALESCE(pin_hash,''), max_users, offline_messages_enabled, created_at, updated_at
		FROM rooms
		WHERE room_id = ?
	`, roomID).Scan(&room.RoomID, &room.RelayID, &room.PinHash, &room.MaxUsers, &offlineMessagesEnabled, &room.CreatedAt, &room.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Room{}, Relay{}, errStoreNotFound
	}
	if err != nil {
		return Room{}, Relay{}, err
	}
	room.OfflineMessagesEnabled = offlineMessagesEnabled == 1
	room.HasPin = strings.TrimSpace(room.PinHash) != ""

	relay, err := s.GetRelayByID(ctx, room.RelayID)
	if err != nil {
		return Room{}, Relay{}, err
	}
	return room, relay, nil
}

func (s *sqliteStore) DeleteRoom(ctx context.Context, roomID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var relayID string
	if err := tx.QueryRowContext(ctx, `SELECT relay_id FROM rooms WHERE room_id = ?`, roomID).Scan(&relayID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errStoreNotFound
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM rooms WHERE room_id = ?`, roomID); err != nil {
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

func (s *sqliteStore) MarkStaleRelaysOffline(ctx context.Context, cutoff int64, now int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE relays SET is_online = 0, updated_at = ? WHERE last_heartbeat < ? AND is_online = 1`, now, cutoff)
	return err
}

type relayScanner func(dest ...any) error

func scanRelay(scan relayScanner) (Relay, error) {
	var r Relay
	var online int
	var offlineMessagesSupported int
	if err := scan(&r.RelayID, &r.RelayName, &r.PublicURL, &r.Region, &online, &r.CurrentRooms, &r.CurrentUsers, &r.MaxRooms, &r.MaxUsers, &offlineMessagesSupported, &r.LastHeartbeat, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return Relay{}, err
	}
	r.IsOnline = online == 1
	r.OfflineMessagesSupported = offlineMessagesSupported == 1
	return r, nil
}

type mongoRelayDoc struct {
	RelayID                  string `bson:"relayId"`
	RelayName                string `bson:"relayName"`
	PublicURL                string `bson:"publicUrl"`
	Region                   string `bson:"region"`
	IsOnline                 bool   `bson:"isOnline"`
	CurrentRooms             int    `bson:"currentRooms"`
	CurrentUsers             int    `bson:"currentUsers"`
	MaxRooms                 int    `bson:"maxRooms"`
	MaxUsers                 int    `bson:"maxUsers"`
	OfflineMessagesSupported bool   `bson:"offlineMessagesSupported"`
	LastHeartbeat            int64  `bson:"lastHeartbeat"`
	CreatedAt                int64  `bson:"createdAt"`
	UpdatedAt                int64  `bson:"updatedAt"`
}

type mongoRoomDoc struct {
	RoomID                 string `bson:"roomId"`
	RelayID                string `bson:"relayId"`
	PinHash                string `bson:"pinHash"`
	MaxUsers               int    `bson:"maxUsers"`
	OfflineMessagesEnabled bool   `bson:"offlineMessagesEnabled"`
	CreatedAt              int64  `bson:"createdAt"`
	UpdatedAt              int64  `bson:"updatedAt"`
}

func openMongoRegistryStore(ctx context.Context, cfg *Config) (*mongoRegistryStore, error) {
	if strings.TrimSpace(cfg.MongoURI) == "" {
		return nil, errors.New("mongoUri is required when storageBackend is mongodb")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.MongoURI).SetConnectTimeout(10 * time.Second))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	store := &mongoRegistryStore{
		client: client,
		relays: client.Database(cfg.MongoDatabase).Collection(cfg.MongoRelaysCollection),
		rooms:  client.Database(cfg.MongoDatabase).Collection(cfg.MongoRoomsCollection),
	}
	if err := store.ensureIndexes(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return store, nil
}

func (s *mongoRegistryStore) ensureIndexes(ctx context.Context) error {
	if _, err := s.relays.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{"relayId", 1}}, Options: options.Index().SetName("relay_id_unique").SetUnique(true)},
		{Keys: bson.D{{"publicUrl", 1}}, Options: options.Index().SetName("public_url_unique").SetUnique(true)},
		{Keys: bson.D{{"isOnline", 1}, {"currentUsers", 1}, {"currentRooms", 1}, {"lastHeartbeat", -1}}, Options: options.Index().SetName("relay_online_load")},
		{Keys: bson.D{{"region", 1}}, Options: options.Index().SetName("relay_region")},
	}); err != nil {
		return err
	}
	_, err := s.rooms.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{"roomId", 1}}, Options: options.Index().SetName("room_id_unique").SetUnique(true)},
		{Keys: bson.D{{"relayId", 1}}, Options: options.Index().SetName("room_relay_id")},
		{Keys: bson.D{{"updatedAt", 1}}, Options: options.Index().SetName("room_updated_at")},
	})
	return err
}

func (s *mongoRegistryStore) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Disconnect(ctx)
}

func (s *mongoRegistryStore) RegisterRelay(ctx context.Context, req registerRelayRequest, publicURL string, now int64) error {
	_, err := s.relays.UpdateOne(ctx,
		bson.M{"publicUrl": publicURL},
		bson.M{
			"$set": bson.M{
				"relayId":                  req.RelayID,
				"relayName":                req.RelayName,
				"publicUrl":                publicURL,
				"region":                   normalizeRegion(req.Region),
				"isOnline":                 true,
				"currentRooms":             0,
				"currentUsers":             0,
				"maxRooms":                 req.MaxRooms,
				"maxUsers":                 req.MaxUsers,
				"offlineMessagesSupported": req.OfflineMessagesSupported,
				"lastHeartbeat":            now,
				"updatedAt":                now,
			},
			"$setOnInsert": bson.M{"createdAt": now},
		},
		options.UpdateOne().SetUpsert(true),
	)
	return mapMongoWriteError(err)
}

func (s *mongoRegistryStore) UpdateRelayHeartbeat(ctx context.Context, req heartbeatRequest, now int64) error {
	isOnline := true
	if req.IsOnline != nil {
		isOnline = *req.IsOnline
	}
	set := bson.M{
		"currentRooms":  req.CurrentRooms,
		"currentUsers":  req.CurrentUsers,
		"isOnline":      isOnline,
		"lastHeartbeat": now,
		"updatedAt":     now,
	}
	if strings.TrimSpace(req.Region) != "" {
		set["region"] = normalizeRegion(req.Region)
	}
	if req.OfflineMessagesSupported != nil {
		set["offlineMessagesSupported"] = *req.OfflineMessagesSupported
	}
	res, err := s.relays.UpdateOne(ctx, bson.M{"relayId": req.RelayID}, bson.M{"$set": set})
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return errStoreNotFound
	}
	return nil
}

func (s *mongoRegistryStore) ListRelays(ctx context.Context) ([]Relay, error) {
	cursor, err := s.relays.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{"isOnline", -1}, {"currentUsers", 1}, {"currentRooms", 1}, {"lastHeartbeat", -1}, {"relayId", 1}}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var docs []mongoRelayDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	relays := make([]Relay, 0, len(docs))
	for _, doc := range docs {
		relays = append(relays, relayFromMongo(doc))
	}
	return relays, nil
}

func (s *mongoRegistryStore) ChooseRelay(ctx context.Context, region string, requireOfflineMessages bool, heartbeatTimeout time.Duration) (Relay, error) {
	cursor, err := s.relays.Find(ctx, bson.M{"isOnline": true}, options.Find().SetSort(bson.D{{"currentUsers", 1}, {"currentRooms", 1}, {"lastHeartbeat", -1}, {"relayId", 1}}))
	if err != nil {
		return Relay{}, err
	}
	defer cursor.Close(ctx)
	var docs []mongoRelayDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return Relay{}, err
	}
	now := time.Now().UTC().Unix()
	candidates := make([]Relay, 0, len(docs))
	for _, doc := range docs {
		r := relayFromMongo(doc)
		if now-r.LastHeartbeat > int64(heartbeatTimeout.Seconds()) {
			continue
		}
		candidates = append(candidates, r)
	}
	return chooseBestRelay(candidates, region, requireOfflineMessages)
}

func (s *mongoRegistryStore) GetRelayByID(ctx context.Context, relayID string) (Relay, error) {
	var doc mongoRelayDoc
	if err := s.relays.FindOne(ctx, bson.M{"relayId": relayID}).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return Relay{}, errStoreNotFound
		}
		return Relay{}, err
	}
	return relayFromMongo(doc), nil
}

func (s *mongoRegistryStore) CreateRoom(ctx context.Context, req createRoomRequest, relay Relay, pinHash string, now int64) error {
	doc := mongoRoomDoc{
		RoomID:                 req.RoomID,
		RelayID:                relay.RelayID,
		PinHash:                pinHash,
		MaxUsers:               req.MaxUsers,
		OfflineMessagesEnabled: req.OfflineMessagesEnabled,
		CreatedAt:              now,
		UpdatedAt:              now,
	}
	if _, err := s.rooms.InsertOne(ctx, doc); err != nil {
		return mapMongoWriteError(err)
	}
	res, err := s.relays.UpdateOne(ctx, bson.M{"relayId": relay.RelayID}, bson.M{"$inc": bson.M{"currentRooms": 1}, "$set": bson.M{"updatedAt": now}})
	if err != nil {
		_, _ = s.rooms.DeleteOne(ctx, bson.M{"roomId": req.RoomID})
		return err
	}
	if res.MatchedCount == 0 {
		_, _ = s.rooms.DeleteOne(ctx, bson.M{"roomId": req.RoomID})
		return errStoreNotFound
	}
	return nil
}

func (s *mongoRegistryStore) UpdateRoomRelay(ctx context.Context, roomID, relayID string, updatedAt int64) error {
	res, err := s.rooms.UpdateOne(ctx, bson.M{"roomId": roomID}, bson.M{"$set": bson.M{"relayId": relayID, "updatedAt": updatedAt}})
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return errStoreNotFound
	}
	return nil
}

func (s *mongoRegistryStore) GetRoomWithRelay(ctx context.Context, roomID string) (Room, Relay, error) {
	var roomDoc mongoRoomDoc
	if err := s.rooms.FindOne(ctx, bson.M{"roomId": roomID}).Decode(&roomDoc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return Room{}, Relay{}, errStoreNotFound
		}
		return Room{}, Relay{}, err
	}
	relay, err := s.GetRelayByID(ctx, roomDoc.RelayID)
	if err != nil {
		return Room{}, Relay{}, err
	}
	return roomFromMongo(roomDoc), relay, nil
}

func (s *mongoRegistryStore) DeleteRoom(ctx context.Context, roomID string) error {
	var roomDoc mongoRoomDoc
	if err := s.rooms.FindOne(ctx, bson.M{"roomId": roomID}).Decode(&roomDoc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return errStoreNotFound
		}
		return err
	}
	res, err := s.rooms.DeleteOne(ctx, bson.M{"roomId": roomID})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return errStoreNotFound
	}
	_, err = s.relays.UpdateOne(ctx, bson.M{"relayId": roomDoc.RelayID}, bson.M{"$inc": bson.M{"currentRooms": -1}, "$set": bson.M{"updatedAt": time.Now().UTC().Unix()}})
	return err
}

func (s *mongoRegistryStore) MarkStaleRelaysOffline(ctx context.Context, cutoff int64, now int64) error {
	_, err := s.relays.UpdateMany(ctx, bson.M{"lastHeartbeat": bson.M{"$lt": cutoff}, "isOnline": true}, bson.M{"$set": bson.M{"isOnline": false, "updatedAt": now}})
	return err
}

func relayFromMongo(doc mongoRelayDoc) Relay {
	return Relay{
		RelayID:                  doc.RelayID,
		RelayName:                doc.RelayName,
		PublicURL:                doc.PublicURL,
		Region:                   doc.Region,
		IsOnline:                 doc.IsOnline,
		CurrentRooms:             doc.CurrentRooms,
		CurrentUsers:             doc.CurrentUsers,
		MaxRooms:                 doc.MaxRooms,
		MaxUsers:                 doc.MaxUsers,
		OfflineMessagesSupported: doc.OfflineMessagesSupported,
		LastHeartbeat:            doc.LastHeartbeat,
		CreatedAt:                doc.CreatedAt,
		UpdatedAt:                doc.UpdatedAt,
	}
}

func roomFromMongo(doc mongoRoomDoc) Room {
	return Room{
		RoomID:                 doc.RoomID,
		RelayID:                doc.RelayID,
		PinHash:                doc.PinHash,
		HasPin:                 strings.TrimSpace(doc.PinHash) != "",
		MaxUsers:               doc.MaxUsers,
		OfflineMessagesEnabled: doc.OfflineMessagesEnabled,
		CreatedAt:              doc.CreatedAt,
		UpdatedAt:              doc.UpdatedAt,
	}
}

func mapMongoWriteError(err error) error {
	if err == nil {
		return nil
	}
	if mongo.IsDuplicateKeyError(err) {
		return errStoreDuplicate
	}
	return err
}

func chooseBestRelay(candidates []Relay, region string, requireOfflineMessages bool) (Relay, error) {
	filtered := candidates[:0]
	for _, r := range candidates {
		if requireOfflineMessages && !r.OfflineMessagesSupported {
			continue
		}
		filtered = append(filtered, r)
	}
	candidates = filtered
	if len(candidates) == 0 {
		if requireOfflineMessages {
			return Relay{}, errors.New("no_offline_message_relay")
		}
		return Relay{}, errors.New("no_available_relay")
	}
	if strings.TrimSpace(region) != "" {
		var byRegion []Relay
		for _, r := range candidates {
			if strings.EqualFold(strings.TrimSpace(r.Region), region) {
				byRegion = append(byRegion, r)
			}
		}
		if len(byRegion) > 0 {
			candidates = byRegion
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
		"storageBackend":          s.storageBackend,
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
	if err := s.store.RegisterRelay(r.Context(), req, publicURL, now); err != nil {
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
	if err := s.store.UpdateRelayHeartbeat(r.Context(), req, now); err != nil {
		if errors.Is(err, errStoreNotFound) {
			writeJSONError(w, http.StatusNotFound, "relay_not_found")
		} else {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
		}
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

	var relay Relay
	var err error
	if strings.TrimSpace(req.RelayID) != "" {
		relay, err = s.store.GetRelayByID(r.Context(), req.RelayID)
		if err != nil {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		if req.OfflineMessagesEnabled && !relay.OfflineMessagesSupported {
			writeJSONError(w, http.StatusConflict, "relay_offline_messages_unavailable")
			return
		}
	} else {
		relay, err = s.store.ChooseRelay(r.Context(), strings.TrimSpace(req.Region), req.OfflineMessagesEnabled, s.heartbeatTimeout)
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
	now := time.Now().UTC().Unix()
	pinHash := ""
	if strings.TrimSpace(req.Pin) != "" {
		pinHash = hashPin(req.Pin)
	}

	if err := s.store.CreateRoom(r.Context(), req, relay, pinHash, now); err != nil {
		if errors.Is(err, errStoreDuplicate) {
			writeJSONError(w, http.StatusConflict, "room_already_exists")
		} else {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
		}
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
		_ = s.store.DeleteRoom(r.Context(), req.RoomID)
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
			if errors.Is(err, errStoreNotFound) {
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
			err = s.store.UpdateRoomRelay(r.Context(), room.RoomID, newRelay.RelayID, time.Now().UTC().Unix())
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
			if errors.Is(err, errStoreNotFound) {
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
	return s.store.ListRelays(ctx)
}

func (s *Server) chooseRelay(ctx context.Context, region string, offlineMessagesRequired ...bool) (Relay, error) {
	requireOfflineMessages := len(offlineMessagesRequired) > 0 && offlineMessagesRequired[0]
	return s.store.ChooseRelay(ctx, region, requireOfflineMessages, s.heartbeatTimeout)
}

func relayScore(r Relay) int {
	// Lower is better.
	// Rooms are weighted more because fewer rooms usually means a cleaner relay.
	return (r.CurrentRooms * 10) + r.CurrentUsers
}

func (s *Server) getRoomWithRelay(ctx context.Context, roomID string) (Room, Relay, error) {
	return s.store.GetRoomWithRelay(ctx, roomID)
}

func (s *Server) deleteRoom(ctx context.Context, roomID string) error {
	return s.store.DeleteRoom(ctx, roomID)
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

	err := s.store.MarkStaleRelaysOffline(context.Background(), cutoff, now)
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
