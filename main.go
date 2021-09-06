package main

import (
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

		queues, err := ch.QueueDeclare(
			queueName, //name
			true,      //durable
			false,     //delete when unused
			false,     // exclusive
			false,     // no-wait
			nil,       //arguments
		)
		if err != nil {
			errorLoger(err, "Failed to declare a queue")

		}

		msgs, err := ch.Consume(
			queues.Name,    // очередь
			"OUT ConSumer", //консумер
			true,           //авто -акк
			false,          //эксклюзив
			false,          //не локально
			false,          // не ждать
			nil,            //args

		)
		if err != nil {
			errorLoger(err, "Failed to register a consumer")
		}

	}

	//msgs, err := ch.Consume(
	//	queues.Name,    // очередь
	//	"OUT ConSumer", //консумер
	//	true,           //авто -акк
	//	false,          //эксклюзив
	//	false,          //не локально
	//	false,          // не ждать
	//	nil,            //args
	//
	//)
	//errorLoger(err, "Failed to register a consumer")
	forever := make(chan bool)
	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)
	}
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
