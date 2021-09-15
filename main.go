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
	"time"
)

// error handling function
func errorLoger(errLogreFile error, msgtoErrorLogerFile string) {
	fileWrite, err := os.OpenFile("error.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error Open or Read errorLog.log File", err)
	}
	log.SetOutput(fileWrite)
	log.Printf("%s:%s", msgtoErrorLogerFile, errLogreFile)

}

//Configuration structure
type ConfigYmal struct {
	Host          string `yaml:"host"`
	Port          string `yaml:"port"`
	Login         string `yaml:"login"`
	Password      string `yaml:"passwd"`
	QueueName     string `yaml:"queueName"`
	QueueMessages int    `yaml:"queueMessages"`
	QueueCount    int    `yaml:"queueCount"`
}

// YAML File parsing function
func inConfigParsingYmal(configFile string) (*ConfigYmal, error) {
	configFileOpen, err := ioutil.ReadFile(configFile)
	if err != nil {
		errorLoger(err, " NO CONFIG FILE")
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

	for i := 1; i <= configReader.QueueCount; i++ {

		ch, err := conn.Channel()
		if err != nil {
			errorLoger(err, "Failed to open a channel")
		}
		defer ch.Close()
		queueName := configReader.QueueName + strconv.Itoa(i)
		//fmt.Println("try: " + queueName)
		msgs, err := ch.Consume(
			queueName,
			"test",
			true,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			errorLoger(err, "Failed to open a channel")
			continue
		}
		//forever := make(chan bool)
		//i:=0
		go func() {
			for range msgs {

				time.Sleep(time.Second)
			}
			//fmt.Println("Считано " , i , " сообщений")
		}()
		//	fmt.Println("Считано " , i , " сообщений")
		fmt.Println("Successfully Connected to our RabbitMQ Instance")
		fmt.Println(" [*] - Waiting for messages")
		//time.Sleep(time.Duration(200).Minutes)
		//	close(forever)
		//	<-forever

	}
	time.Sleep(20 * time.Minute)
}
