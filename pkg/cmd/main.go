package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sputnik-systems/backup-scheduler/pkg/sdk"
	"github.com/sputnik-systems/backup-scheduler/pkg/sdk/clickhouse"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
)

const (
	backupNameFormat    = "2006-01-02T15:04"
	statusCheckInterval = 5 * time.Second
)

type options struct {
	endpoint, schedule string
}

var (
	rootCmd = &cobra.Command{
		Use:   "scheduler",
		Short: "Scheduler for backup creation",
		Long:  "Simple daemon, than interact with several type apis for create and upload backups",
	}

	clickhouseCmd = &cobra.Command{
		Use:   "clickhouse",
		Short: "Scheduler clickhouse backuping",
		RunE:  run,
	}

	c      sdk.ApiClient
	logger = log.Default()

	opts = &options{
		endpoint: "http://localhost:7171",
		schedule: "0 0 * * *",
	}
)

func init() {
	rootCmd.AddCommand(clickhouseCmd)

	rootCmd.PersistentFlags().StringVar(&opts.endpoint, "backup.endpoint", opts.endpoint, "backup manage api endpoint url")
	rootCmd.PersistentFlags().StringVar(&opts.schedule, "backup.schedule", opts.schedule, "backup schedule in cron notation")
}

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func run(cmd *cobra.Command, args []string) error {
	var err error

	switch cmd.Use {
	case "clickhouse":
		c, err = clickhouse.New(opts.endpoint, logger)
	}
	if err != nil {
		return err
	}

	bf := func() {
		err := backup()
		if err != nil {
			logger.Printf("failed to create backup: %s\n", err)
		}
	}

	cd := cron.New(cron.WithLogger(cron.VerbosePrintfLogger(logger)))
	_, err = cd.AddFunc(opts.schedule, bf)
	if err != nil {
		return fmt.Errorf("failed to add function execution by cron daemon: %s", err)
	}

	cd.Run()

	return nil
}

func backup() error {
	name := time.Now().Format(backupNameFormat)
	err := c.Create(context.Background(), name)
	if err != nil {
		return err
	}

	for {
		status, err := c.Status(context.Background(), name)
		if err != nil {
			logger.Printf("failed to get backup status: %s\n", err)
		}

		logger.Printf("backup %s status: %s\n", name, status)

		if status == "success" {
			break
		}

		time.Sleep(statusCheckInterval)
	}

	err = c.Upload(context.Background(), name)

	return err
}
