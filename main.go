package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
)

// функция обработки ошибок
func errorLoger(errLogreFile error, msgtoErrorLogerFile string) {
	fileWrite, err := os.OpenFile("error.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error Open or Read errorLog.log File", err)
	}
	log.SetOutput(fileWrite)
	log.Fatalf("%s:%s", msgtoErrorLogerFile, errLogreFile)
}

//структура Конфига
type ConfigYmal struct {
	Host          string `yaml:"host"`
	Port          string `yaml:"port"`
	Login         string `yaml:"login"`
	Password      string `yaml:"passwd"`
	QueueName     string `yaml:"queueName"`
	QueueMessages int    `yaml:"queueMessages"`
	QueueCount    int    `yaml:"queueCount"`
}

// функция парсинга Нфьд Файла
func inConfigParsingYmal(configFile string) (*ConfigYmal, error) {
	configFileOpen, err := ioutil.ReadFile(configFile)
	if err != nil {
		errorLoger(err, " НЕ МОГУ НАЙТИ ФАЙЛ КОНФИГУРАЦИИ")
	}
	c := &ConfigYmal{}
	err = yaml.Unmarshal(configFileOpen, c)
	if err != nil {
		errorLoger(err, "Cannot Parsing Ymal File")
	}
	return c, nil
}

func main() {

	runtime.GOMAXPROCS(2)
	configReader, err := inConfigParsingYmal("config.yml")
	if err != nil {
		errorLoger(err, "Error Reading Config")
	}

	conn, err := amqp.Dial("amqp://" + configReader.Login + ":" + configReader.Password + "@" + configReader.Host + ":" + configReader.Port)
	if err != nil {
		errorLoger(err, "Filed to connect to RabbitMQ")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		errorLoger(err, "Failed to open a channel")
	}
	defer ch.Close()
	for i := 1; i <= configReader.QueueCount; i++ {
		queueName := configReader.QueueName + strconv.Itoa(i)
		msgs, err := ch.Consume(
			queueName,
			"",
			true,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			errorLoger(err, "ВСЕ пропало!!")
		}
		fmt.Println(msgs)

		forever := make(chan bool)
		go func() {
			for d := range msgs {
				fmt.Printf("Recieved Message: %s\n", d.Body)
			}
		}()

		fmt.Println("Successfully Connected to our RabbitMQ Instance")
		fmt.Println(" [*] - Waiting for messages")
		<-forever
	}
}
