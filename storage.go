package storage

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// DbStorage struct
type DbStorage struct {
	client *mongo.Client
	name   string
	ctx    context.Context
}

// NewMongoStorage initialize connection to storage
func NewMongoStorage(connection string) (*DbStorage, error) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(connection))
	if err != nil {
		return nil, errors.New("Unable connect to DB: " + err.Error())
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return nil, errors.New("Unable to ping DB: " + err.Error())
	}

	return &DbStorage{
		client: client,
		ctx:    context.TODO(),
	}, nil
}

// Close func
func (db *DbStorage) Close() {
	defer db.client.Disconnect(context.TODO())
}

// WithName adds db name to context
func (db *DbStorage) WithName(name string) *DbStorage {
	db.name = name
	return db
}

func (db *DbStorage) getDb(name string) (*mongo.Client, *mongo.Database) {
	if name == "" {
		panic("DB NAME IS NOT DEFINED")
	}
	return db.client, db.client.Database(db.name)
}

// GetDbCollection func
func (db *DbStorage) GetDbCollection(collection string) CRUD {
	c, d := db.getDb(db.name)
	return newCRUD(db.ctx, c, d.Collection(collection))
}

// EnsureIndex func
func (db *DbStorage) EnsureIndex(c *mongo.Collection, key string) {
	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
	index := yieldIndexModel(key)
	c.Indexes().CreateOne(context.Background(), index, opts)
}

func yieldIndexModel(key string) mongo.IndexModel {
	keys := bsonx.Doc{{Key: key, Value: bsonx.Int32(1)}}
	index := mongo.IndexModel{}
	index.Keys = keys
	index.Options = options.Index().SetUnique(true)
	return index
}
