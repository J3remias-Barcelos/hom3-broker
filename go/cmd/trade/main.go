package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/J3remias-Barcelos/hom3-broker/tree/main/go/internal/market/dto"
	"github.com/J3remias-Barcelos/hom3-broker/tree/main/go/internal/market/entity"
	"github.com/J3remias-Barcelos/hom3-broker/tree/main/go/internal/market/transformer"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	ordersIn := make(chan *entity.Order)
	ordersOut := make(chan *entity.Order)
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	kafkaMsgChan := make(chan *kafka.Message)
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
		"group.id":          "output",
		"auto.offset.reset": "latest",
	}
	producer := kafka.NewKafkaProducer(configMap)
	consumer := confluentkafka.NewConsumer(configMap) // Renomeado para evitar conflito de nomes

	go consumer.Consume(kafkaMsgChan) // T2

	// recebe do canal do kafka, joga no input, processa joga no output e depois publica no kafka
	book := entity.NewBook(ordersIn, ordersOut, wg)
	go book.Trade() // T3

	go func() {
		for msg := range kafkaMsgChan {
			wg.Add(1)
			fmt.Println(string(msg.Value))
			tradeInput := dto.TradeInput{}
			err := json.Unmarshal(msg.Value, &tradeInput)
			if err != nil {
				panic(err)
			}
			order := transformer.TransformInput(tradeInput)
			ordersIn <- order
		}
	}()

	for res := range ordersOut {
		output := transformer.TransformOutput(res)
		outputJson, err := json.MarshalIndent(output, "", "  ")
		fmt.Println(string(outputJson))
		if err != nil {
			fmt.Println(err)
		}
		producer.Publish(outputJson, []byte("orders"), "output")
	}
}
