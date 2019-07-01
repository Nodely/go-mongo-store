package storage

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/op/go-logging"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	conn = "mongodb://localhost/test-db"
	log  = logging.MustGetLogger("crud")
	db   *DbStorage
)

type record struct {
	ID      primitive.ObjectID `bson:"_id,omitempty"`
	Name    string             `bson:"name"`
	DateReg time.Time          `bson:"dater"`
}

func TestSaveItem(t *testing.T) {
	dbs, err := NewMongoStorage(conn)
	if err != nil {
		panic(fmt.Sprintf("Failed to init: %s", err.Error()))
	}
	// select collection
	crud := dbs.WithName("nodely").GetDbCollection("test-storage")

	// insert row into collection
	r := &record{
		Name: "Regular Item",
	}
	err = crud.SaveItem(primitive.NilObjectID, r)
	assert.Nil(t, err, "Unable to save test data")
	oriID := r.ID

	log.Debugf("Record: %+v", r)

	// check item
	cnt := crud.Count(bson.M{"_id": r.ID})
	log.Debugf("Count rows: ", cnt)
	assert.Equal(t, int64(1), cnt)

	// update row into collection
	r.Name = "Regular Item Updated"
	err = crud.SaveItem(r.ID, r)
	if err != nil {
		panic(fmt.Sprintf("Failed to update: %s", err.Error()))
	}
	assert.Equal(t, oriID.Hex(), r.ID.Hex())
	assert.Equal(t, "Regular Item Updated", r.Name)

	dbs.Close()
}

func TestGetNonExistsItem(t *testing.T) {
	dbs, err := NewMongoStorage(conn)
	if err != nil {
		panic(fmt.Sprintf("Failed to init: %s", err.Error()))
	}
	// select collection
	crud := dbs.WithName("nodely").GetDbCollection("test-storage")

	// check item
	_, err = crud.GetItem(primitive.NewObjectID(), reflect.TypeOf(new(record)))
	assert.NotNil(t, err)

	dbs.Close()
}

func TestGetItem(t *testing.T) {
	dbs, err := NewMongoStorage(conn)
	if err != nil {
		panic(fmt.Sprintf("Failed to init: %s", err.Error()))
	}
	// select collection
	crud := dbs.WithName("nodely").GetDbCollection("test-storage")

	// insert doc
	ids, err := crud.Insert(&record{
		Name: "Record 1",
	}, &record{
		Name: "Record 2",
	})
	assert.Nil(t, err, "Unable to insert test data")

	for _, id := range ids {
		// check item
		rec, err := crud.GetItem(id.(primitive.ObjectID), reflect.TypeOf(new(record)))
		assert.Nil(t, err, "Unable to get item")
		log.Debugf("Record: %v", rec)
		r := rec.(*record)
		assert.False(t, r.ID.IsZero(), "Record id is zero")
	}

	dbs.Close()
}
