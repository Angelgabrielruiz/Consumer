package main

import (
	consumer "Consumer/Consumer"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

func main() {

    err := godotenv.Load()
    if err != nil {
        log.Fatalf("Error al cargar el archivo .env: %v", err)
    }

    rabbitMQURL := os.Getenv("RABBITMQ_URL")
    apiURL := os.Getenv("API_URL")

    if rabbitMQURL == "" || apiURL == "" {
        log.Fatal("Faltan variables de entorno necesarias: RABBITMQ_URL o API_URL")
    }

    // Conectar a RabbitMQ
    rabbitConn, err := amqp.Dial(rabbitMQURL)
    if err != nil {
        log.Fatalf("Error al conectar con RabbitMQ: %v", err)
    }
    defer rabbitConn.Close()

    // Crear el consumidor
    consumer := consumer.NewConsumer(rabbitConn, apiURL)

    // Comenzar a consumir mensajes de la cola
    err = consumer.StartConsuming("pago_created_queue")
    if err != nil {
        log.Fatalf("Error al comenzar a consumir: %v", err)
    }
}
