package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

type CreateTableColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}
type CreateTablePayload struct {
	Name    string              `json:"name"`
	Columns []CreateTableColumn `json:"columns"`
}
type User struct {
	Id       string    `json:"id"`
	Username string    `json:"username"`
	Active   bool      `json:"active"`
	Created  time.Time `json:"created"`
}
type Payment struct {
	Id     string `json:"id"`
	Label  string `json:"label"`
	UserId string `json:"userId"`
}
type UserPayment struct {
	Id    string `json:"id"`
	Label string `json:"label"`
}
type UserPayments struct {
	Id       string        `json:"id"`
	Username string        `json:"username"`
	Payments []UserPayment `json:"payments"`
}

var connectionString string = os.Getenv("DATABSE_URL") // "postgres://gostgres_admin:123@localhost:5432/gostgres"

var pool *pgxpool.Pool // initialise once at startup
func create(payload CreateTablePayload) error {
	slog.Info("Starting table creation")
	if len(payload.Columns) == 0 {
		return fmt.Errorf("No columns supplied for table %s", payload.Name)
	}
	var sb strings.Builder
	for _, column := range payload.Columns {

		sb.WriteString(column.Name + " " + column.Type + " ")
		if column.Nullable {
			sb.WriteString("null")
		} else {
			sb.WriteString("not null")
		}
	}
	conn, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	sql := fmt.Sprintf(`create table %s (%s)`, payload.Name, sb.String())
	slog.Info("Executing sql", "sql", sql)
	_, err = conn.Exec(context.Background(), sql)
	if err != nil {
		return err
	}

	return nil
}
func createHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Starting create handler")
	var payload CreateTablePayload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = create(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
func testHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("Starting test...")
	conn, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		http.Error(w, fmt.Sprintf("Unable to connect to database: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	var number int
	err = conn.QueryRow(context.Background(), "select 1").Scan(&number)
	if err != nil {
		http.Error(w, fmt.Sprintf("Unable to query 1: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	w.Write([]byte("Got 1"))
}
func getUser(ctx context.Context, id string) (*User, error) {
	conn, err := pgx.Connect(ctx, connectionString)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	var user User
	err = conn.QueryRow(ctx, "select id, username, active, created from users where id = $1", id).
		Scan(&user.Id, &user.Username, &user.Active, &user.Created)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}
func getUserHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if len(id) == 0 {
		http.Error(w, "No id value found in path", http.StatusBadRequest)
		slog.Error("No id value found in path value", r.URL.Path)
		return
	}
	user, err := getUser(r.Context(), id)
	if user == nil && err == nil {
		http.Error(w, "No user found with matching id", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Error fetching user", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	err = json.NewEncoder(w).Encode(user)
	if err != nil {
		http.Error(w, "Error encoding user json", http.StatusInternalServerError)
		slog.Error("Error encoding user json", "error", err)
		return
	}
}
func helloHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello my dear"))
}
func getUserPayments(ctx context.Context, id string) (*User, *Payment, error) {
	var user User
	var payment Payment
	g, groupCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := pool.QueryRow(groupCtx, "select id, username, active, created from users where id = $1", id).
			Scan(&user.Id, &user.Username, &user.Active, &user.Created)
		if err != nil {
			return fmt.Errorf("Error fetching user for id %s: %w", id, err)
		}
		return err
	})
	g.Go(func() error {
		err := pool.QueryRow(groupCtx, "select id, label, user_id from payments where user_id = $1", id).
			Scan(&payment.Id, &payment.Label, &payment.UserId)
		if err != nil {
			return fmt.Errorf("Error fetching payment for id %s: %w", id, err)
		}
		return err
	})
	err := g.Wait()
	if err != nil {
		return nil, nil, err
	}
	return &user, &payment, nil
}
func getUserPaymentsStandard(ctx context.Context, id string) (*User, *Payment, error) {
	var user User
	var userErr error
	var payment Payment
	var paymentErr error
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		err := pool.QueryRow(ctx, "select id, username, active, created from users where id = $1", id).
			Scan(&user.Id, &user.Username, &user.Active, &user.Created)
		if err != nil {
			userErr = fmt.Errorf("Error fetching user for id %s: %w", id, err)
		}
	}()
	go func() {
		defer wg.Done()
		err := pool.QueryRow(ctx, "select id, label, user_id from payments where user_id = $1", id).
			Scan(&payment.Id, &payment.Label, &payment.UserId)
		if err != nil {
			paymentErr = fmt.Errorf("Error fetching payment for id %s: %w", id, err)
		}
	}()

	wg.Wait()
	if userErr != nil {
		return nil, nil, userErr
	}
	if paymentErr != nil {
		return nil, nil, paymentErr
	}
	return &user, &payment, nil
}
func getUserPaymentsHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	user, payment, err := getUserPaymentsStandard(r.Context(), id)
	if err != nil {
		http.Error(w, "Unable to get user payments", http.StatusInternalServerError)
		slog.Error("Unable to get user payments", "error", err)
		return
	}
	userPayment := UserPayments{
		Id:       user.Id,
		Username: user.Username,
		Payments: []UserPayment{
			UserPayment{
				Id:    payment.Id,
				Label: payment.Label,
			},
		},
	}
	w.Header().Set("content-type", "application/json")
	err = json.NewEncoder(w).Encode(userPayment)
	if err != nil {
		slog.Error("Error encoding user payment json", "error", err)
		http.Error(w, "Error encoding user payment response", http.StatusInternalServerError)
		return
	}
}
func createServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /db/create", createHandler)
	mux.HandleFunc("GET /db/test", testHandler)
	mux.HandleFunc("GET /hello", helloHandler)
	mux.HandleFunc("GET /users/{id}", getUserHandler)
	mux.HandleFunc("GET /user_payments/{id}", getUserPaymentsHandler)
	port := os.Getenv("PORT")
	address := ":8080"
	if len(port) > 0 {
		address = ":" + port
	}

	server := http.Server{
		Addr:    address,
		Handler: mux,
	}
	return &server
}
func main() {
	var err error
	pool, err = pgxpool.New(context.Background(), connectionString)
	if err != nil {
		slog.Error("Unable to create connection pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	quit := make(chan os.Signal, 1)
	server := createServer()
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			slog.Error("Error with http server", "error", err)
			os.Exit(1)
		}
	}()
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	err = server.Shutdown(ctx)
	if err != nil {
		slog.Error("Error shutting down server", "error", err)
	}
}
