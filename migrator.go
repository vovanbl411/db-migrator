// migrator.go
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// MigrationParams и Migrator остаются почти такими же

// Константы для таймаутов
const (
	PortForwardWaitTime      = 9 * time.Second
	ReplicationTimeout       = 150 * time.Second
	ReplicationCheckInterval = 10 * time.Second
	PodReadyTimeout          = 300 * time.Second
	ScaleDownWaitTime        = 20 * time.Second
)

type MigrationParams struct {
	K8sStatefulSetName string
	DBName             string
	DBNameWithSuffix   string // Имя БД с суффиксом для bare-metal
	K8sAppUser         string
	K8sAppPassword     string
	K8sNodePort        int
	ReplicatorUser     string
	ReplicatorPassword string
	SubscriptionName   string
	PublicationName    string
}

type Migrator struct {
	Config        AppConfig
	Params        MigrationParams
	K8sNodeIP     string
	DryRun        bool
	PerfLogger    *PerformanceLogger
	rollbackSteps []func() error
}

// performIfNotDryRun выполняет функцию, если не в режиме DryRun
func (m *Migrator) performIfNotDryRun(operation string, action func() error) error {
	if m.DryRun {
		Logger.WithFields(logrus.Fields{
			"db_name": m.Params.DBName,
			"action":  operation,
		}).Info("[DRY RUN] Skipping action")
		return nil
	}

	Logger.WithFields(logrus.Fields{
		"db_name": m.Params.DBName,
		"action":  operation,
	}).Info("Executing action")

	return action()
}

// NewMigrator теперь просто создает пустой мигратор
func NewMigrator(config AppConfig) *Migrator {
	return &Migrator{
		Config:        config,
		PerfLogger:    NewPerformanceLogger(),
		rollbackSteps: make([]func() error, 0),
	}
}

// SetDB подготавливает мигратор для работы с конкретной БД
func (m *Migrator) SetDB(stsName string, stsConfig StatefulSet) {
	m.Params.K8sStatefulSetName = stsName
	m.Params.DBName = stsConfig.ConfigMap.DB
	// Добавляем суффикс к имени БД для bare-metal, если он задан
	if m.Config.DBNameSuffix != "" {
		m.Params.DBNameWithSuffix = m.Params.DBName + m.Config.DBNameSuffix
	} else {
		m.Params.DBNameWithSuffix = m.Params.DBName
	}
	m.Params.K8sAppUser = stsConfig.ConfigMap.User
	m.Params.K8sAppPassword = stsConfig.ConfigMap.Password
	m.Params.K8sNodePort = m.Config.NodePortBase + stsConfig.NodePort

	// Логируем информацию о базе данных
	Logger.WithFields(logrus.Fields{
		"statefulset_name": stsName,
		"db_name":          m.Params.DBName,
		"node_port":        m.Params.K8sNodePort,
	}).Debug("Set database configuration for migration")

	dbNameSanitized := strings.ReplaceAll(m.Params.DBName, "-", "_")
	dbNameWithSuffixSanitized := strings.ReplaceAll(m.Params.DBNameWithSuffix, "-", "_")
	m.Params.ReplicatorUser = fmt.Sprintf("replicator_%s", dbNameSanitized)
	m.Params.ReplicatorPassword = fmt.Sprintf("ReplicatorPasswordFor%s", m.Params.DBName)
	m.Params.SubscriptionName = fmt.Sprintf("%s_subscription", dbNameWithSuffixSanitized)
	m.Params.PublicationName = fmt.Sprintf("%s_publication", dbNameSanitized)
}

// Все методы теперь просто возвращают ошибку и больше не выводят шаги в консоль
// (этим займется TUI)

// Вспомогательные функции для ConfigureK8sPublisher
func (m *Migrator) checkWalLevel() (string, error) {
	podName := fmt.Sprintf("%s-0", m.Params.K8sStatefulSetName)

	walCheckArgs := []string{"exec", "-it", podName, "-n", m.Config.K8sNamespace, "--", "psql", "-U", m.Params.K8sAppUser, "-d", m.Params.DBName, "-t", "-c", "SHOW wal_level;"}
	walLevel, err := RunCommand("kubectl", walCheckArgs, CommandOptions{CaptureOutput: true, Check: true})
	if err != nil {
		Logger.WithError(err).WithFields(logrus.Fields{
			"db_name":  m.Params.DBName,
			"pod_name": podName,
		}).Error("Failed to check wal_level")
		return "", fmt.Errorf("не удалось проверить wal_level: %w", err)
	}

	return walLevel, nil
}

func (m *Migrator) updateWalLevel() error {
	podName := fmt.Sprintf("%s-0", m.Params.K8sStatefulSetName)

	alterArgs := []string{"exec", "-it", podName, "-n", m.Config.K8sNamespace, "--", "psql", "-U", m.Params.K8sAppUser, "-d", m.Params.DBName, "-c", "ALTER SYSTEM SET wal_level = 'logical';"}
	if _, err := RunCommand("kubectl", alterArgs, CommandOptions{Check: true}); err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to alter wal_level")
		return err
	}

	return nil
}

func (m *Migrator) scaleDownStatefulSet() error {
	scaleDownArgs := []string{"scale", fmt.Sprintf("statefulset/%s", m.Params.K8sStatefulSetName), "--replicas=0", "-n", m.Config.K8sNamespace}
	if _, err := RunCommand("kubectl", scaleDownArgs, CommandOptions{Check: true}); err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to scale down StatefulSet")
		return err
	}
	Logger.WithField("db_name", m.Params.DBName).Info("StatefulSet scaled down, waiting 20 seconds before scaling up...")
	time.Sleep(ScaleDownWaitTime)

	return nil
}

func (m *Migrator) scaleUpStatefulSet() error {
	scaleUpArgs := []string{"scale", fmt.Sprintf("statefulset/%s", m.Params.K8sStatefulSetName), "--replicas=1", "-n", m.Config.K8sNamespace}
	if _, err := RunCommand("kubectl", scaleUpArgs, CommandOptions{Check: true}); err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to scale up StatefulSet")
		return err
	}

	podName := fmt.Sprintf("%s-0", m.Params.K8sStatefulSetName)
	waitArgs := []string{"wait", "--for=condition=ready", fmt.Sprintf("pod/%s", podName), "--timeout=300s", "-n", m.Config.K8sNamespace}
	if _, err := RunCommand("kubectl", waitArgs, CommandOptions{Check: true}); err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to wait for pod to be ready")
		return err
	}
	Logger.WithField("db_name", m.Params.DBName).Info("Pod is ready after scaling operations")

	return nil
}

func (m *Migrator) createReplicationRoleAndPublication() error {
	podName := fmt.Sprintf("%s-0", m.Params.K8sStatefulSetName)

	Logger.WithField("db_name", m.Params.DBName).Info("Creating replication role and publication")
	sqlCommands := fmt.Sprintf(`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '%s') THEN CREATE ROLE %s WITH REPLICATION LOGIN PASSWORD '%s'; END IF; END $$; GRANT CONNECT ON DATABASE "%s" TO %s; GRANT USAGE ON SCHEMA public TO %s; GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s; DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '%s') THEN CREATE PUBLICATION %s FOR ALL TABLES; END IF; END $$;`, m.Params.ReplicatorUser, m.Params.ReplicatorUser, m.Params.ReplicatorPassword, m.Params.DBName, m.Params.ReplicatorUser, m.Params.ReplicatorUser, m.Params.ReplicatorUser, m.Params.ReplicatorUser, m.Params.PublicationName, m.Params.PublicationName)
	psqlArgs := []string{"exec", "-i", podName, "-n", m.Config.K8sNamespace, "--", "psql", "-U", m.Params.K8sAppUser, "-d", m.Params.DBName}
	_, err := RunCommand("kubectl", psqlArgs, CommandOptions{Stdin: sqlCommands, Check: true})
	if err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to create replication role and publication")
		return err
	}
	Logger.WithField("db_name", m.Params.DBName).Info("Successfully configured K8s publisher")

	return nil
}

func (m *Migrator) PrepareServers() error {
	operation := "PrepareServers"
	m.PerfLogger.StartTimer(operation)

	Logger.WithFields(logrus.Fields{
		"db_name": m.Params.DBName,
		"dry_run": m.DryRun,
	}).Info("Starting server preparation")

	if m.DryRun {
		Logger.WithFields(logrus.Fields{
			"db_name": m.Params.DBName,
		}).Info("[DRY RUN] Skipping server preparation steps")
		return nil // В режиме dry-run всегда возвращаем успех
	}

	jsonpath := "{.items[0].status.addresses[?(@.type=='InternalIP')].address}"
	out, err := RunCommand("kubectl", []string{"get", "nodes", "-o", fmt.Sprintf("jsonpath=%s", jsonpath)}, CommandOptions{CaptureOutput: true, Check: true})
	if err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to get Kubernetes node IP")
		return err
	}
	m.K8sNodeIP = out
	Logger.WithField("k8s_node_ip", m.K8sNodeIP).Info("Successfully obtained Kubernetes node IP")

	// Добавляем шаг отката
	m.addRollbackStep("RestoreK8sStatefulSet", func() error {
		Logger.Info("Rolling back: Restoring K8s StatefulSet")
		scaleUpArgs := []string{"scale", fmt.Sprintf("statefulset/%s", m.Params.K8sStatefulSetName), "--replicas=1", "-n", m.Config.K8sNamespace}
		_, err := RunCommand("kubectl", scaleUpArgs, CommandOptions{Check: true})
		return err
	})

	m.PerfLogger.EndTimer(operation)
	return nil
}

func (m *Migrator) ConfigureK8sPublisher() error {
	operation := "ConfigureK8sPublisher"
	m.PerfLogger.StartTimer(operation)

	Logger.WithFields(logrus.Fields{
		"db_name":  m.Params.DBName,
		"pod_name": fmt.Sprintf("%s-0", m.Params.K8sStatefulSetName),
		"dry_run":  m.DryRun,
	}).Info("Starting configuration of K8s publisher")

	err := m.performIfNotDryRun("ConfigureK8sPublisher", func() error {
		// Проверяем текущий уровень WAL
		walLevel, err := m.checkWalLevel()
		if err != nil {
			return err
		}

		if !strings.Contains(walLevel, "logical") {
			Logger.WithField("db_name", m.Params.DBName).Info("wal_level is not set to logical, updating...")

			// Обновляем уровень WAL
			if err := m.updateWalLevel(); err != nil {
				return err
			}

			// Масштабируем StatefulSet до 0 и обратно до 1
			if err := m.scaleDownStatefulSet(); err != nil {
				return err
			}

			if err := m.scaleUpStatefulSet(); err != nil {
				return err
			}
		} else {
			Logger.WithField("db_name", m.Params.DBName).Info("wal_level is already set to logical")
		}

		// Создаем роль репликатора и публикацию
		if err := m.createReplicationRoleAndPublication(); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Добавляем шаг отката для удаления роли репликации и публикации
	m.addRollbackStep("RemoveReplicationRoleAndPublication", func() error {
		Logger.Info("Rolling back: Removing replication role and publication")
		podName := fmt.Sprintf("%s-0", m.Params.K8sStatefulSetName)
		sqlCommands := fmt.Sprintf(`DO $$ BEGIN IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '%s') THEN DROP ROLE IF EXISTS %s; END IF; END $$; DROP PUBLICATION IF EXISTS %s;`, m.Params.ReplicatorUser, m.Params.ReplicatorUser, m.Params.PublicationName)
		psqlArgs := []string{"exec", "-i", podName, "-n", m.Config.K8sNamespace, "--", "psql", "-U", m.Params.K8sAppUser, "-d", m.Params.DBName}
		_, err := RunCommand("kubectl", psqlArgs, CommandOptions{Stdin: sqlCommands, Check: true})
		return err
	})

	m.PerfLogger.EndTimer(operation)
	return nil
}

// Вспомогательные функции для MigrateSchema
func (m *Migrator) startPortForward() (*exec.Cmd, error) {
	Logger.WithField("db_name", m.Params.DBName).Info("Starting port-forward for schema dump")

	ctx, cancel := context.WithCancel(context.Background())
	pfArgs := []string{"port-forward", fmt.Sprintf("svc/%s", m.Params.K8sStatefulSetName), "5432:5432", "-n", m.Config.K8sNamespace}
	pfCmd := exec.CommandContext(ctx, "kubectl", pfArgs...)
	if err := pfCmd.Start(); err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to start port-forward")
		cancel()
		return nil, fmt.Errorf("не удалось запустить port-forward: %w", err)
	}

	// Ждем, пока port-forward установится
	time.Sleep(PortForwardWaitTime)

	return pfCmd, nil
}

func (m *Migrator) createSchemaDump() (string, error) {
	Logger.WithField("db_name", m.Params.DBName).Info("Creating schema dump")

	dumpArgs := []string{"-h", "localhost", "-p", "5432", "-U", m.Params.K8sAppUser, "-d", m.Params.DBName, "--schema-only", "--no-publications", "--no-subscriptions", "--no-owner", "--no-privileges"}
	dumpEnv := map[string]string{"PGPASSWORD": m.Params.K8sAppPassword}
	dumpOutput, err := RunCommand("pg_dump", dumpArgs, CommandOptions{CaptureOutput: true, Check: true, ExtraEnv: dumpEnv})
	if err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to create schema dump")
		// Теперь мы добавляем вывод самой команды pg_dump в нашу ошибку!
		errorDetails := fmt.Sprintf("Вывод команды pg_dump:\n%s", dumpOutput)
		return "", fmt.Errorf("не удалось создать дамп схемы: %w\n\n%s", err, errorDetails)
	}

	return dumpOutput, nil
}

func (m *Migrator) saveSchemaDumpToFile(dumpOutput string, filename string) error {
	cleanDump := strings.ReplaceAll(dumpOutput, "SET default_transaction_read_only = off;", "")
	if err := os.WriteFile(filename, []byte(cleanDump), 0644); err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to save schema dump file")
		return fmt.Errorf("не удалось сохранить файл дампа: %w", err)
	}

	return nil
}

func (m *Migrator) copySchemaDumpToBareMetal(filename string) error {
	Logger.WithFields(logrus.Fields{
		"db_name": m.Params.DBName,
		"bm_host": m.Config.BMHost,
	}).Info("Copying schema dump to bare-metal host")

	scpDest := fmt.Sprintf("%s@%s:/tmp/", m.Config.BMSSHUser, m.Config.BMHost)
	if _, err := RunCommand("scp", []string{filename, scpDest}, CommandOptions{Check: true}); err != nil {
		Logger.WithError(err).WithFields(logrus.Fields{
			"db_name":          m.Params.DBName,
			"schema_dump_file": filename,
		}).Error("Failed to copy schema dump to bare-metal host")
		return err
	}

	return nil
}

func (m *Migrator) createDatabaseOnBareMetal() error {
	Logger.WithField("db_name", m.Params.DBName).Info("Creating database on bare-metal host")

	sshCommands := fmt.Sprintf(`set -e; sudo -u %s psql -d "%s" -c "DROP SUBSCRIPTION IF EXISTS %s;" || true; sudo -u %s psql -c 'DROP DATABASE IF EXISTS "%s";'; sudo -u %s psql -c 'CREATE DATABASE "%s";'`, m.Config.BMPGUser, m.Params.DBNameWithSuffix, m.Params.SubscriptionName, m.Config.BMPGUser, m.Params.DBNameWithSuffix, m.Config.BMPGUser, m.Params.DBNameWithSuffix)
	sshArgs := []string{fmt.Sprintf("%s@%s", m.Config.BMSSHUser, m.Config.BMHost), sshCommands}
	if _, err := RunCommand("ssh", sshArgs, CommandOptions{Check: true}); err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to create database on bare-metal host")
		return err
	}

	return nil
}

func (m *Migrator) restoreSchemaOnBareMetal(filename string) error {
	Logger.WithField("db_name", m.Params.DBName).Info("Restoring schema on bare-metal host")

	restoreCmd := fmt.Sprintf("sudo -u %s psql -d \"%s\" < /tmp/%s", m.Config.BMPGUser, m.Params.DBNameWithSuffix, filename)
	restoreArgs := []string{fmt.Sprintf("%s@%s", m.Config.BMSSHUser, m.Config.BMHost), restoreCmd}
	_, err := RunCommand("ssh", restoreArgs, CommandOptions{Check: true})
	if err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to restore schema on bare-metal host")
		return err
	}

	return nil
}

func (m *Migrator) MigrateSchema() error {
	operation := "MigrateSchema"
	m.PerfLogger.StartTimer(operation)

	Logger.WithFields(logrus.Fields{
		"db_name": m.Params.DBName,
		"dry_run": m.DryRun,
	}).Info("Starting schema migration")

	schemaDumpFile := fmt.Sprintf("%s_schema_clean.sql", m.Params.DBName)

	err := m.performIfNotDryRun("MigrateSchema", func() error {
		// Запускаем port-forward
		pfCmd, err := m.startPortForward()
		if err != nil {
			return err
		}

		// Закрываем port-forward при выходе из функции
		defer func() {
			if pfCmd != nil && pfCmd.Process != nil {
				pfCmd.Process.Kill()
			}
		}()

		// Создаем дамп схемы
		dumpOutput, err := m.createSchemaDump()
		if err != nil {
			return err
		}

		// Сохраняем дамп в файл
		if err := m.saveSchemaDumpToFile(dumpOutput, schemaDumpFile); err != nil {
			return err
		}

		// Удаляем временный файл при выходе из функции
		defer func() {
			if err := os.Remove(schemaDumpFile); err != nil {
				Logger.WithError(err).WithField("schema_dump_file", schemaDumpFile).Warn("Failed to remove temporary schema dump file")
			} else {
				Logger.WithField("schema_dump_file", schemaDumpFile).Debug("Removed temporary schema dump file")
			}
		}()

		// Копируем дамп на bare-metal хост
		if err := m.copySchemaDumpToBareMetal(schemaDumpFile); err != nil {
			return err
		}

		// Создаем базу данных на bare-metal хосте
		if err := m.createDatabaseOnBareMetal(); err != nil {
			return err
		}

		// Восстанавливаем схему на bare-metal хосте
		if err := m.restoreSchemaOnBareMetal(schemaDumpFile); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	Logger.WithField("db_name", m.Params.DBName).Info("Successfully completed schema migration")

	// Добавляем шаг отката для удаления созданной базы данных на bare-metal
	m.addRollbackStep("DropBareMetalDatabase", func() error {
		Logger.Info("Rolling back: Dropping bare-metal database")
		sshCommands := fmt.Sprintf(`sudo -u %s psql -c 'DROP DATABASE IF EXISTS "%s";'`, m.Config.BMPGUser, m.Params.DBNameWithSuffix)
		sshArgs := []string{fmt.Sprintf("%s@%s", m.Config.BMSSHUser, m.Config.BMHost), sshCommands}
		_, err := RunCommand("ssh", sshArgs, CommandOptions{Check: true})
		return err
	})

	m.PerfLogger.EndTimer(operation)
	return nil
}

// Вспомогательные функции для StartAndVerifyReplication
func (m *Migrator) createSubscriptionOnBareMetal() error {
	Logger.WithFields(logrus.Fields{
		"db_name":       m.Params.DBName,
		"k8s_node_ip":   m.K8sNodeIP,
		"k8s_node_port": m.Params.K8sNodePort,
	}).Info("Creating subscription on bare-metal host")

	connString := fmt.Sprintf("'host=%s port=%d user=%s password=%s dbname=%s'", m.K8sNodeIP, m.Params.K8sNodePort, m.Params.ReplicatorUser, m.Params.ReplicatorPassword, m.Params.DBName)
	createSubCmd := fmt.Sprintf(`sudo -u %s psql -d "%s" -c "CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s;"`, m.Config.BMPGUser, m.Params.DBNameWithSuffix, m.Params.SubscriptionName, connString, m.Params.PublicationName)
	sshArgs := []string{fmt.Sprintf("%s@%s", m.Config.BMSSHUser, m.Config.BMHost), createSubCmd}
	if _, err := RunCommand("ssh", sshArgs, CommandOptions{Check: true}); err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to create subscription on bare-metal host")
		return err
	}

	return nil
}

func (m *Migrator) waitForReplicationSlotActive() error {
	Logger.WithField("db_name", m.Params.DBName).Info("Waiting for replication slot to become active")

	// Вычисляем количество попыток на основе таймаута интервала проверки
	maxAttempts := int(ReplicationTimeout / ReplicationCheckInterval)

	for i := 1; i <= maxAttempts; i++ {
		Logger.WithFields(logrus.Fields{
			"db_name": m.Params.DBName,
			"attempt": i,
		}).Debug("Checking replication slot status")

		checkSlotSQL := fmt.Sprintf("SELECT active FROM pg_replication_slots WHERE slot_name = '%s';", m.Params.SubscriptionName)
		checkArgs := []string{"exec", "-i", fmt.Sprintf("%s-0", m.Params.K8sStatefulSetName), "-n", m.Config.K8sNamespace, "--", "psql", "-U", m.Params.K8sAppUser, "-d", m.Params.DBName, "-t", "-c", checkSlotSQL}
		output, err := RunCommand("kubectl", checkArgs, CommandOptions{CaptureOutput: true, Check: true})
		if err != nil {
			Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to check replication slot status")
			return fmt.Errorf("не удалось проверить слот репликации: %w", err)
		}
		if strings.Contains(output, "t") {
			Logger.WithField("db_name", m.Params.DBName).Info("Replication slot is now active")
			return nil
		}
		Logger.WithFields(logrus.Fields{
			"db_name": m.Params.DBName,
			"attempt": i,
		}).Info("Replication slot is not yet active, waiting...")
		time.Sleep(ReplicationCheckInterval)
	}
	Logger.WithField("db_name", m.Params.DBName).Error("Replication slot did not become active within the timeout period")

	return fmt.Errorf("ошибка: слот репликации не стал активным за %s", ReplicationTimeout.String())
}

func (m *Migrator) StartAndVerifyReplication() error {
	operation := "StartAndVerifyReplication"
	m.PerfLogger.StartTimer(operation)

	Logger.WithFields(logrus.Fields{
		"db_name": m.Params.DBName,
		"dry_run": m.DryRun,
	}).Info("Starting replication setup and verification")

	err := m.performIfNotDryRun("StartAndVerifyReplication", func() error {
		// Создаем подписку на bare-metal хосте
		if err := m.createSubscriptionOnBareMetal(); err != nil {
			return err
		}

		// Ждем, пока слот репликации станет активным
		if err := m.waitForReplicationSlotActive(); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Добавляем шаг отката для удаления подписки на bare-metal
	m.addRollbackStep("DropBareMetalSubscription", func() error {
		Logger.Info("Rolling back: Dropping bare-metal subscription")
		dropSubCmd := fmt.Sprintf("sudo -u %s psql -d \"%s\" -c 'DROP SUBSCRIPTION IF EXISTS %s;'", m.Config.BMPGUser, m.Params.DBNameWithSuffix, m.Params.SubscriptionName)
		_, err := RunCommand("ssh", []string{fmt.Sprintf("%s@%s", m.Config.BMSSHUser, m.Config.BMHost), dropSubCmd}, CommandOptions{Check: true})
		return err
	})

	m.PerfLogger.EndTimer(operation)
	return nil
}

// Вспомогательные функции для GrantAppPermissions
func (m *Migrator) grantAppPermissionsOnBareMetal() error {
	Logger.WithField("db_name", m.Params.DBName).Info("Granting application permissions on bare-metal host")

	sqlGrants := fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %s; GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %s; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO %s;`, m.Params.K8sAppUser, m.Params.K8sAppUser, m.Params.K8sAppUser, m.Params.K8sAppUser, m.Params.K8sAppUser)
	grantCmd := fmt.Sprintf("sudo -u %s psql -d \"%s\" -c \"%s\"", m.Config.BMPGUser, m.Params.DBNameWithSuffix, sqlGrants)
	sshArgs := []string{fmt.Sprintf("%s@%s", m.Config.BMSSHUser, m.Config.BMHost), grantCmd}
	_, err := RunCommand("ssh", sshArgs, CommandOptions{Check: true})
	if err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to grant application permissions")
		return err
	}

	return nil
}

func (m *Migrator) GrantAppPermissions() error {
	operation := "GrantAppPermissions"
	m.PerfLogger.StartTimer(operation)

	Logger.WithFields(logrus.Fields{
		"db_name": m.Params.DBName,
		"dry_run": m.DryRun,
	}).Info("Starting granting application permissions")

	err := m.performIfNotDryRun("GrantAppPermissions", func() error {
		// Выдаем права приложения на bare-metal хосте
		if err := m.grantAppPermissionsOnBareMetal(); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	Logger.WithField("db_name", m.Params.DBName).Info("Successfully granted application permissions")

	m.PerfLogger.EndTimer(operation)
	return nil
}

// addRollbackStep добавляет шаг отката в стек
func (m *Migrator) addRollbackStep(name string, rollbackFunc func() error) {
	m.rollbackSteps = append(m.rollbackSteps, rollbackFunc)
	Logger.WithField("rollback_step", name).Info("Added rollback step")
}

// rollback выполняет все шаги отката в обратном порядке
func (m *Migrator) rollback() error {
	Logger.Info("Starting rollback process")

	// Выполняем шаги отката в обратном порядке
	for i := len(m.rollbackSteps) - 1; i >= 0; i-- {
		Logger.WithField("rollback_step_index", i).Info("Executing rollback step")
		if err := m.rollbackSteps[i](); err != nil {
			Logger.WithError(err).WithField("rollback_step_index", i).Error("Rollback step failed")
			// Продолжаем выполнение остальных шагов отката, даже если один из них не удался
		}
	}

	Logger.Info("Rollback process completed")
	return nil
}

// clearRollbackSteps очищает стек шагов отката
func (m *Migrator) clearRollbackSteps() {
	m.rollbackSteps = make([]func() error, 0)
}

// Вспомогательные функции для FinalCutover
func (m *Migrator) dropSubscriptionOnBareMetal() error {
	Logger.WithField("db_name", m.Params.DBName).Info("Dropping subscription on bare-metal host")
	dropSubCmd := fmt.Sprintf("sudo -u %s psql -d \"%s\" -c 'DROP SUBSCRIPTION %s;'", m.Config.BMPGUser, m.Params.DBNameWithSuffix, m.Params.SubscriptionName)
	if _, err := RunCommand("ssh", []string{fmt.Sprintf("%s@%s", m.Config.BMSSHUser, m.Config.BMHost), dropSubCmd}, CommandOptions{Check: true}); err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to drop subscription on bare-metal host")
		return err
	}

	return nil
}

func (m *Migrator) scaleDownK8sStatefulSet() error {
	Logger.WithField("db_name", m.Params.DBName).Info("Scaling down Kubernetes StatefulSet")
	scaleDownArgs := []string{"scale", fmt.Sprintf("statefulset/%s", m.Params.K8sStatefulSetName), "--replicas=0", "-n", m.Config.K8sNamespace}
	_, err := RunCommand("kubectl", scaleDownArgs, CommandOptions{Check: true})
	if err != nil {
		Logger.WithError(err).WithField("db_name", m.Params.DBName).Error("Failed to scale down Kubernetes StatefulSet")
		return err
	}

	return nil
}

// FinalCutover теперь не интерактивный, TUI покажет сообщение
func (m *Migrator) FinalCutover() error {
	operation := "FinalCutover"
	m.PerfLogger.StartTimer(operation)

	Logger.WithFields(logrus.Fields{
		"db_name": m.Params.DBName,
		"dry_run": m.DryRun,
	}).Info("Starting final cutover")

	err := m.performIfNotDryRun("FinalCutover", func() error {
		// Удаляем подписку на bare-metal хосте
		if err := m.dropSubscriptionOnBareMetal(); err != nil {
			return err
		}

		// Масштабируем StatefulSet в K8s до 0
		if err := m.scaleDownK8sStatefulSet(); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	Logger.WithField("db_name", m.Params.DBName).Info("Successfully completed final cutover")

	// Добавляем шаг отката для масштабирования StatefulSet обратно до 1
	m.addRollbackStep("RestoreK8sStatefulSetAfterCutover", func() error {
		Logger.Info("Rolling back: Restoring K8s StatefulSet after cutover")
		scaleUpArgs := []string{"scale", fmt.Sprintf("statefulset/%s", m.Params.K8sStatefulSetName), "--replicas=1", "-n", m.Config.K8sNamespace}
		_, err := RunCommand("kubectl", scaleUpArgs, CommandOptions{Check: true})
		return err
	})

	m.PerfLogger.EndTimer(operation)
	return nil
}

// MultiMigratorState хранит состояние миграции для нескольких БД
type MultiMigratorState struct {
	DBName   string
	Status   string // "pending", "running", "completed", "failed"
	Error    error
	Migrator *Migrator
}

// MultiMigrator управляет миграцией нескольких баз данных
type MultiMigrator struct {
	Config     AppConfig
	Values     *ValuesFile
	DBStates   map[string]*MultiMigratorState
	DryRun     bool
	PerfLogger *PerformanceLogger
}

// NewMultiMigrator создает новый MultiMigrator
func NewMultiMigrator(config AppConfig, values *ValuesFile, dryRun bool) *MultiMigrator {
	return &MultiMigrator{
		Config:     config,
		Values:     values,
		DBStates:   make(map[string]*MultiMigratorState),
		DryRun:     dryRun,
		PerfLogger: NewPerformanceLogger(),
	}
}

// AddDB добавляет базу данных для миграции
func (mm *MultiMigrator) AddDB(dbName string) {
	stsConfig := mm.Values.StatefulSets[dbName]
	migrator := NewMigrator(mm.Config)
	migrator.DryRun = mm.DryRun
	migrator.SetDB(dbName, stsConfig)

	mm.DBStates[dbName] = &MultiMigratorState{
		DBName:   dbName,
		Status:   "pending",
		Migrator: migrator,
	}
}

// MigrateAll запускает миграцию всех добавленных БД
func (mm *MultiMigrator) MigrateAll() error {
	operation := "MultiMigrateAll"
	mm.PerfLogger.StartTimer(operation)

	Logger.Info("Starting migration of multiple databases")

	// Запускаем миграцию для каждой базы данных
	for dbName, state := range mm.DBStates {
		Logger.WithField("db_name", dbName).Info("Starting migration for database")
		state.Status = "running"

		// Выполняем все шаги миграции для текущей БД
		err := mm.migrateSingleDB(state.Migrator)
		if err != nil {
			Logger.WithError(err).WithField("db_name", dbName).Error("Migration failed for database")
			state.Status = "failed"
			state.Error = err
		} else {
			Logger.WithField("db_name", dbName).Info("Migration completed for database")
			state.Status = "completed"
		}
	}

	mm.PerfLogger.EndTimer(operation)

	// Проверяем, были ли ошибки
	for _, state := range mm.DBStates {
		if state.Status == "failed" {
			return fmt.Errorf("migration failed for at least one database")
		}
	}

	return nil
}

// migrateSingleDB выполняет миграцию одной БД
func (mm *MultiMigrator) migrateSingleDB(migrator *Migrator) error {
	// Выполняем все шаги миграции
	steps := []func() error{
		migrator.PrepareServers,
		migrator.ConfigureK8sPublisher,
		migrator.MigrateSchema,
		migrator.StartAndVerifyReplication,
		migrator.GrantAppPermissions,
		migrator.FinalCutover,
	}

	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}

	return nil
}

// RollbackAll выполняет откат всех миграций
func (mm *MultiMigrator) RollbackAll() error {
	Logger.Info("Starting rollback of multiple databases")

	var rollbackErrors []error

	// Выполняем откат для каждой базы данных в обратном порядке
	for dbName, state := range mm.DBStates {
		if state.Migrator != nil {
			Logger.WithField("db_name", dbName).Info("Starting rollback for database")
			err := state.Migrator.rollback()
			if err != nil {
				Logger.WithError(err).WithField("db_name", dbName).Error("Rollback failed for database")
				rollbackErrors = append(rollbackErrors, err)
			} else {
				Logger.WithField("db_name", dbName).Info("Rollback completed for database")
			}
		}
	}

	if len(rollbackErrors) > 0 {
		return fmt.Errorf("rollback failed for %d databases", len(rollbackErrors))
	}

	return nil
}
