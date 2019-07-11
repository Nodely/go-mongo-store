package storage

import (
	"context"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CRUD interface
type CRUD interface {
	GetItem(id primitive.ObjectID, t reflect.Type) (interface{}, error)
	SaveItem(id primitive.ObjectID, d interface{}) error

	Insert(d ...interface{}) ([]interface{}, error)

	Find(filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error)
	FindOne(filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult

	DeleteOne(filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)

	Count(filter interface{}) int64

	EnsureIndex(key string)
	EnsureIndexesRaw(idx mongo.IndexModel) error
}

func newCRUD(ctx context.Context, client *mongo.Client, col *mongo.Collection) CRUD {
	return &dbc{
		ctx:    ctx,
		client: client,
		c:      col,
	}
}

type dbc struct {
	ctx    context.Context
	client *mongo.Client
	c      *mongo.Collection
}

// GetItem func
func (db *dbc) GetItem(id primitive.ObjectID, t reflect.Type) (interface{}, error) {
	r := reflect.New(t.Elem()).Interface()
	err := db.c.FindOne(db.ctx, bson.M{"_id": id}).Decode(r)
	return r, err
}

// SaveItem func
func (db *dbc) SaveItem(id primitive.ObjectID, d interface{}) error {
	if id.IsZero() {
		res, err := db.c.InsertOne(db.ctx, d)
		if err != nil {
			return err
		}
		id = res.InsertedID.(primitive.ObjectID)
		db.c.FindOne(db.ctx, bson.M{"_id": id}).Decode(d)
	} else {
		res := db.c.FindOneAndUpdate(db.ctx, bson.M{"_id": id}, bson.M{"$set": d})
		return res.Err()
	}
	return nil
}

// Insert set of interfaces
func (db *dbc) Insert(d ...interface{}) ([]interface{}, error) {
	res, err := db.c.InsertMany(db.ctx, d)
	if err != nil {
		return nil, err
	}
	return res.InsertedIDs, nil
}

// Count func
func (db *dbc) Count(cond interface{}) int64 {
	cnt, _ := db.c.CountDocuments(db.ctx, cond)
	return cnt
}

// Find func
func (db *dbc) Find(filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
	return db.c.Find(db.ctx, filter, opts...)
}

// FindOne func
func (db *dbc) FindOne(filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult {
	return db.c.FindOne(db.ctx, filter, opts...)
}

// DeleteOne func
func (db *dbc) DeleteOne(filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	return db.c.DeleteOne(db.ctx, filter, opts...)
}

// EnsureIndex func
func (db *dbc) EnsureIndex(key string) {
	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
	index := yieldIndexModel(key)
	db.c.Indexes().CreateOne(context.Background(), index, opts)
}

// EnsureIndex func
func (db *dbc) EnsureIndexesRaw(idx mongo.IndexModel) error {
	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
	_, err := db.c.Indexes().CreateOne(context.Background(), idx, opts)
	return err
}
