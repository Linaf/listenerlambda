package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/sns"
	log "github.com/sirupsen/logrus"
	"os"
)


const (
	TOPIC_ARN_KEY = "TOPIC_ARN"
)

type Person struct {
	Id          string `json:"id"`
	FirstName      string `json:"firstName"`
	LastName int64  `json:"lastName"`
}

func handler(ctx context.Context, e events.DynamoDBEvent) {
	log.Infof("Called DynamoDB Listener Lambda ...")
	for _, record := range e.Records {
		log.Infof("Processing  request data for Event ID %v, type %v ", record.EventID, record.EventName)
		streamToMap, err := convertDynamoTypes(record.Change.NewImage)
		if err != nil {
			log.Info("Error mapping Dynamo attribute  to  Person.")
			return
		}
		log.Infof("streamToMap: %v", streamToMap)

		person := &Person{}
		err = dynamodbattribute.UnmarshalMap(streamToMap, &person)

		if err != nil {
			log.Infof("DynamoDb event fetch error: %v", err.Error())
			return
		}

		log.Infof("Values of Person Record:= ID: %s, FirstName:%s, LastName:%s\n", person.Id, person.FirstName, person.LastName)

		bsgDssDatastoreJson, err := json.Marshal(person)
		if err != nil {
			log.Infof("Failed Json Marshal error: %v", err.Error())
			return
		}

		svc := sns.New(session.New())
		params := &sns.PublishInput{
			Message: aws.String(string(bsgDssDatastoreJson)),
			MessageAttributes: map[string]*sns.MessageAttributeValue{
				"Firstname": {
					DataType:    aws.String("String"),
					StringValue: aws.String(person.FirstName),
				},
			},
			TopicArn: aws.String(os.Getenv(TOPIC_ARN_KEY)),
		}
		resp, err := svc.Publish(params)
		if err != nil {
			log.Errorf("SNS Publish error:  %v", err.Error())
			return
		}

		//:TODO to be removed
		log.Infof("SNS Message params : %v", params)

		// Pretty-print the response data.
		log.Infof("SNS Publish Response: %v", resp)
	}

}

func convertDynamoTypes(attrs map[string]events.DynamoDBAttributeValue) (map[string]*dynamodb.AttributeValue, error) {
	image := map[string]*dynamodb.AttributeValue{}
	for k, v := range attrs {
		var dbav dynamodb.AttributeValue
		bytes, merr := v.MarshalJSON()
		if merr != nil {
			return nil, merr
		}
		uerr := json.Unmarshal(bytes, &dbav)
		if uerr != nil {
			return nil, uerr
		}
		image[k] = &dbav
	}
	return image, nil
}

func main() {
	cloudenv := os.Getenv("CLOUD_ENVIRONMENT")
	log.Infof("Started executing DynamoDB listener lambda in %s", cloudenv)

	lambda.Start(handler)

}
