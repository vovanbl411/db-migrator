// main.go
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/sirupsen/logrus"
)

func main() {
	// Добавляем флаг командной строки
	dryRun := flag.Bool("dry-run", false, "Запустить в режиме 'сухого запуска' без выполнения реальных команд")
	configFile := flag.String("config", "config.yaml", "Путь к файлу конфигурации")
	usePlanner := flag.Bool("planner", false, "Использовать планировщик миграций")
	verbose := flag.Bool("verbose", false, "Включить подробный вывод логов (все уровни)")
	quiet := flag.Bool("quiet", false, "Уменьшить вывод логов (только ошибки)")
	flag.Parse()

	// Инициализируем логгер в зависимости от режима работы
	if *usePlanner {
		// В режиме планировщика выводим логи в stdout
		InitLogger(true)
	} else {
		// В TUI режиме выводим логи только в файл
		InitLogger(false)
	}

	// Настройка уровня логирования в зависимости от флагов
	if *verbose {
		Logger.SetLevel(logrus.DebugLevel)
	} else if *quiet {
		Logger.SetLevel(logrus.ErrorLevel)
	} else {
		// По умолчанию - info уровень
		Logger.SetLevel(logrus.InfoLevel)
	}

	// Инициализируем логгер в зависимости от режима работы
	if *usePlanner {
		// В режиме планировщика выводим логи в stdout
		InitLogger(true)
	} else {
		// В TUI режиме выводим логи только в файл
		InitLogger(false)
	}

	// Настройка уровня логирования в зависимости от флагов
	if *verbose {
		Logger.SetLevel(logrus.DebugLevel)
	} else if *quiet {
		Logger.SetLevel(logrus.ErrorLevel)
	} else {
		// По умолчанию - info уровень
		Logger.SetLevel(logrus.InfoLevel)
	}

	Logger.Info("Starting DB Migrator application")

	Logger.WithField("dry_run", *dryRun).WithField("use_planner", *usePlanner).Info("Configuration loaded")

	// Загружаем основную конфигурацию из файла
	appConfig, err := LoadAppConfig(*configFile)
	if err != nil {
		Logger.WithError(err).Error("Failed to load configuration")
		log.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	values, err := LoadValues(appConfig.ValuesFile)
	if err != nil {
		Logger.WithError(err).WithField("file", appConfig.ValuesFile).Error("Failed to load values file")
		log.Fatalf("Ошибка загрузки файла %s: %v", appConfig.ValuesFile, err)
	}

	Logger.Info("Configuration loaded successfully")

	// Если используется планировщик, создаем и запускаем план миграции
	if *usePlanner {
		planner := NewDefaultMigrationPlanner(*appConfig, values)

		// Получаем список всех доступных баз данных
		var allDBs []string
		for dbName := range values.StatefulSets {
			allDBs = append(allDBs, dbName)
		}

		// Создаем опции планирования
		options := MigrationPlanOptions{
			ParallelMigrations: false,
			MaxConcurrent:      1,
			RollbackOnFailure:  true,
			DryRun:             *dryRun,
		}

		// Создаем план миграции для всех баз данных
		plan, err := planner.CreatePlan(allDBs, options)
		if err != nil {
			Logger.WithError(err).Error("Failed to create migration plan")
			log.Fatalf("Ошибка создания плана миграции: %v", err)
		}

		// Валидируем план
		if err := planner.ValidatePlan(plan); err != nil {
			Logger.WithError(err).Error("Migration plan validation failed")
			log.Fatalf("Ошибка валидации плана миграции: %v", err)
		}

		// Выполняем план миграции
		if err := planner.ExecutePlan(plan); err != nil {
			Logger.WithError(err).Error("Migration plan execution failed")
			log.Fatalf("Ошибка выполнения плана миграции: %v", err)
		}

		// Выводим метрики производительности
		Logger.Info("Migration completed. Performance metrics:")
		metrics := planner.GetPlanProgress(plan)
		for _, stepStatus := range metrics.Steps {
			Logger.WithFields(logrus.Fields{
				"step_id":  stepStatus.ID,
				"status":   stepStatus.Status,
				"duration": stepStatus.Duration.String(),
			}).Info("Step completed")
		}

		Logger.Info("Application exited successfully after executing migration plan")
	} else {
		model := NewTUIModel(*appConfig, values, *dryRun)

		p := tea.NewProgram(model, tea.WithAltScreen(), tea.WithMouseAllMotion())
		if _, err := p.Run(); err != nil {
			Logger.WithError(err).Error("Application exited with error")
			fmt.Printf("Произошла ошибка: %v", err)
			os.Exit(1)
		}

		// Выводим метрики производительности при завершении
		Logger.Info("Migration completed. Performance metrics:")
		var metrics map[string]time.Duration

		// Используем PerfLogger от multiMigrator если мигрируем несколько БД, иначе от обычного мигратора
		if len(model.selectedDBs) > 1 {
			metrics = model.multiMigrator.PerfLogger.GetMetrics()
		} else {
			metrics = model.migrator.PerfLogger.GetMetrics()
		}

		for operation, duration := range metrics {
			Logger.WithFields(logrus.Fields{
				"operation": operation,
				"duration":  duration.String(),
			}).Info("Performance metric")
		}

		// Очищаем шаги отката при успешном завершении миграции
		if model.state == stateFinished {
			if len(model.selectedDBs) > 1 {
				// Для миграции нескольких БД очищаем шаги отката у всех миграторов
				for _, state := range model.multiMigrator.DBStates {
					if state.Migrator != nil {
						state.Migrator.clearRollbackSteps()
					}
				}
				Logger.Info("Rollback steps cleared after successful multi-database migration")
			} else {
				model.migrator.clearRollbackSteps()
				Logger.Info("Rollback steps cleared after successful migration")
			}
		}

		Logger.Info("Application exited successfully")
	}
}
