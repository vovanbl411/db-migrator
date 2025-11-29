// planner_usage.go
// Пример использования планировщика миграций

package main

import (
	"fmt"
	"log"
)

// RunPlannerExample запускает пример использования планировщика миграций
func RunPlannerExample() {
	// Загружаем конфигурацию
	appConfig, err := LoadAppConfig("config.yaml")
	if err != nil {
		log.Printf("Ошибка загрузки конфигурации: %v", err)
		return
	}

	values, err := LoadValues(appConfig.ValuesFile)
	if err != nil {
		log.Printf("Ошибка загрузки значений: %v", err)
		return
	}

	// Создаем планировщик
	planner := NewDefaultMigrationPlanner(*appConfig, values)

	// Получаем список всех доступных баз данных
	var allDBs []string
	for dbName := range values.StatefulSets {
		allDBs = append(allDBs, dbName)
	}

	fmt.Printf("Найдено %d баз данных для миграции: %v\n", len(allDBs), allDBs)

	// Создаем опции планирования
	options := MigrationPlanOptions{
		ParallelMigrations: true,  // Разрешаем параллельные миграции
		MaxConcurrent:      2,     // Максимум 2 параллельные миграции
		RollbackOnFailure:  true,  // Выполнять откат при ошибке
		DryRun:             false, // Реальная миграция (без dry-run)
	}

	// Создаем план миграции
	plan, err := planner.CreatePlan(allDBs, options)
	if err != nil {
		log.Printf("Ошибка создания плана миграции: %v", err)
		return
	}

	fmt.Printf("Создан план миграции с ID: %s\n", plan.ID)
	fmt.Printf("Стратегия: %s\n", plan.Strategy)
	fmt.Printf("Количество шагов: %d\n", len(plan.Steps))

	// Добавляем зависимости между базами данных (опционально)
	// Например, если миграция БД B зависит от завершения миграции БД A
	// planner.AddDependency(plan, "postgres-db-a", "postgres-db-b", "cutover")

	// Валидируем план
	if err := planner.ValidatePlan(plan); err != nil {
		log.Printf("Ошибка валидации плана миграции: %v", err)
		return
	}

	fmt.Println("План миграции успешно создан и валидирован")

	// Выводим статус плана
	status := planner.GetPlanProgress(plan)
	fmt.Printf("Прогресс выполнения: %.2f%%\n", status.Progress*100)

	// Выполняем план миграции
	fmt.Println("Начинаем выполнение плана миграции...")
	if err := planner.ExecutePlan(plan); err != nil {
		log.Printf("Ошибка выполнения плана миграции: %v", err)
		return
	}

	fmt.Println("План миграции успешно выполнен!")

	// Выводим финальный статус
	finalStatus := planner.GetPlanProgress(plan)
	fmt.Printf("Финальный прогресс: %.2f%%\n", finalStatus.Progress*100)
	fmt.Printf("Статус: %s\n", finalStatus.Status)
}
