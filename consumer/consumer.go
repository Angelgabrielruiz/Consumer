package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"

	"github.com/streadway/amqp"
)

type Consumer struct {
	rabbitConn *amqp.Connection
	apiURL     string
}

func NewConsumer(rabbitConn *amqp.Connection, apiURL string) *Consumer {
	return &Consumer{
		rabbitConn: rabbitConn,
		apiURL:     apiURL,
	}
}

func (c *Consumer) StartConsuming(queueName string) error {
	// Crear un canal para comunicarse con RabbitMQ
	ch, err := c.rabbitConn.Channel()
	if err != nil {
		return fmt.Errorf("error al abrir canal: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		queueName, // nombre de la cola
		"",        // consumidor no exclusivo
		false,     // auto-acknowledge: false para ack manual
		false,     // sin cola de exclusividad
		false,     // sin consumir en paralelo
		false,     // no esperar conexiones de un solo consumidor
		nil,       // argumentos
	)
	if err != nil {
		return fmt.Errorf("error al comenzar a consumir: %v", err)
	}

	log.Printf("Esperando mensajes en la cola: %s", queueName)

	// Procesar los mensajes
	for msg := range msgs {
		// Convertir el mensaje en una cadena
		message := string(msg.Body)
		log.Printf("Mensaje recibido: %s", message)

		// Procesar el mensaje de texto y extraer los datos usando expresiones regulares
		pagoData, err := c.parsePagoMessage(message)
		if err != nil {
			log.Printf("Error al procesar el mensaje: %v", err)
			// Si hay error, rechazamos el mensaje para que se reencole (requeue true)
			msg.Nack(false, true)
			continue
		}

		// Enviar los datos a la API
		err = c.SendToAPI(pagoData)
		if err != nil {
			log.Printf("Error al enviar el mensaje a la API: %v", err)
			// Rechazamos el mensaje para que se reencole y se intente nuevamente
			msg.Nack(false, true)
			continue
		}

		// Si todo fue exitoso, confirmamos el mensaje para que se borre de la cola
		msg.Ack(false)
		log.Println("Mensaje enviado a la API con éxito")
	}

	return nil
}

// Función para procesar el mensaje usando expresiones regulares
func (c *Consumer) parsePagoMessage(message string) (map[string]interface{}, error) {
	// Usamos una expresión regular para extraer los valores
	re := regexp.MustCompile(`Monto (\d+), Pago (\d+), Cambio (\d+), Fecha ([\d-: ]+)`)
	matches := re.FindStringSubmatch(message)

	if len(matches) < 5 {
		return nil, fmt.Errorf("formato de mensaje no válido")
	}

	// Creamos un mapa con los valores extraídos
	pagoData := map[string]interface{}{
		"monto":  matches[1],
		"pago":   matches[2],
		"cambio": matches[3],
		"fecha":  matches[4],
	}

	return pagoData, nil
}

// Función para enviar los datos a la API
func (c *Consumer) SendToAPI(data map[string]interface{}) error {
	// Convertir los datos a JSON
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error al convertir datos a JSON para enviar a la API: %v", err)
	}

	// Realizamos una solicitud POST a la API
	resp, err := http.Post(c.apiURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("error al enviar el mensaje a la API: %v", err)
	}
	defer resp.Body.Close()

	// Verificar el estado de la respuesta de la API
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error en la respuesta de la API: %v", resp.Status)
	}

	return nil
}
