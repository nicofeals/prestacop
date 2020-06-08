package main

import (
	"os"
	"strings"
	"time"

	"github.com/nicofeals/prestacop/config"
	"github.com/urfave/cli"
)

var version string

func main() {
	app := buildCLI()
	_ = app.Run(os.Args)
}

func getEnvironment(c *cli.Context) string {
	return strings.TrimSpace(c.GlobalString("env"))
}

func getLogLevel(c *cli.Context) string {
	return strings.TrimSpace(c.GlobalString("level"))
}

func getBroker(c *cli.Context) string {
	return c.String("broker")
}

func getRegularMessageTopic(c *cli.Context) string {
	return c.String("regular-message-topic")
}

func getAssistanceMessageTopic(c *cli.Context) string {
	return c.String("assistance-message-topic")
}

func getCSVpath(c *cli.Context) string {
	return c.String("csv-path")
}

func getMessageInterval(c *cli.Context) time.Duration {
	return c.Duration("message-interval")
}

func getDroneInstances(c *cli.Context) int {
	return c.Int("drone-instances")
}

func buildCLI() *cli.App {
	app := cli.NewApp()
	app.Name = "prestacop-drone"
	app.Usage = "Prestacop Drone simulator sending messages to a kafka stream"
	app.Version = version

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "env, e",
			Value:  "development",
			Usage:  "runtime environment",
			EnvVar: config.EnvKeyEnvironment,
		},
		cli.StringFlag{
			Name:   "level",
			Value:  "info",
			Usage:  "logging level",
			EnvVar: config.EnvKeyLogLevel,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:  "drone",
			Usage: "start drone simulator",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "broker, b",
					Value:  "localhost",
					Usage:  "broker to produce messages to",
					EnvVar: config.EnvKeyBroker,
				},
				cli.StringFlag{
					Name:   "regular-message-topic",
					Value:  "regular-msg",
					Usage:  "topic for regular messages",
					EnvVar: config.EnvKeyRegularMessageTopic,
				},
				cli.StringFlag{
					Name:   "assistance-message-topic",
					Value:  "assistance-msg",
					Usage:  "topic for assistance messages",
					EnvVar: config.EnvKeyAssistanceMessageTopic,
				},
				cli.DurationFlag{
					Name:   "message-interval",
					Value:  time.Minute,
					Usage:  "time between each sent message",
					EnvVar: config.EnvKeyMessageInterval,
				},
				cli.IntFlag{
					Name:   "drone-instances",
					Value:  1,
					Usage:  "number of drone instances to run simultaneously",
					EnvVar: config.EnvKeyDroneInstances,
				},
			},
			Action: func(c *cli.Context) {
				launchDrone(c)
			},
		},
		{
			Name:  "csv",
			Usage: "start csv lines sender",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "broker, b",
					Value:  "localhost",
					Usage:  "broker to produce messages to",
					EnvVar: config.EnvKeyBroker,
				},
				cli.StringFlag{
					Name:   "message-topic",
					Value:  "regular-msg",
					Usage:  "topic for messages",
					EnvVar: config.EnvKeyRegularMessageTopic,
				},
				cli.StringFlag{
					Name:   "csv-path",
					Value:  "",
					Usage:  "path of the csv file to use",
					EnvVar: config.EnvKeyCSVPath,
				},
			},
			Action: func(c *cli.Context) {
				launchCsvSender(c)
			},
		},
	}

	return app
}
