package mydsl

import (
	"context"
	_ "fmt"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"log"
	//	"reflect"
	"os"
	"regexp"
	"time"
)

var mongoDbnamePattern = regexp.MustCompile(`^mongodb://(.+?):`)

func init() {
	mongodbUri := os.Getenv("MONGODB_URI")
	dbname := mongoDbnamePattern.FindStringSubmatch(mongodbUri)[1]
	client, _ := mongo.NewClient(mongodbUri)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Hour)
	client.Connect(ctx)

	DslFunctions["mongoGet"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		collectionName := args[0].rawArg.(string)
		collection := client.Database(dbname).Collection(collectionName)
		cur, err := collection.Find(ctx, bson.D{})
		if err != nil {
			log.Fatal(err)
		}
		records := []map[string]interface{}{}
		defer cur.Close(ctx)
		for cur.Next(ctx) {
			var result map[string]interface{}
			err := cur.Decode(&result)
			if err != nil {
				log.Fatal(err)
			}
			records = append(records, result)
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		return records, nil
	}

	DslFunctions["mongoInsert"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		collectionName := args[0].rawArg.(string)
		obj, err := args[1].Evaluate(container)
		if err != nil {
			return nil, err
		}
		collection := client.Database(dbname).Collection(collectionName)
		res, err := collection.InsertOne(ctx, obj)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	DslFunctions["mongoReplace"] = func(container map[string]interface{}, args ...Argument) (interface{}, error) {
		collectionName := args[0].rawArg.(string)
		obj, err := args[1].Evaluate(container)
		if err != nil {
			return nil, err
		}
		collection := client.Database(dbname).Collection(collectionName)
		res := collection.FindOneAndReplace(ctx, map[string]interface{}{"_id": (obj.(map[string]interface{}))["_id"]}, obj)
		return res, nil
	}
}
